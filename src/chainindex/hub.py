"""The in-memory fan-out: a ring of sealed blocks, unsealed tip logs, and one bounded queue per subscriber."""

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable

from pydantic import BaseModel

from chainindex.events import (
    ErrorEvent,
    Event,
    HeadEvent,
    LogEvent,
    ReorgEvent,
    SealedEvent,
    Subscription,
)
from chainindex.logger import get_logger
from chainindex.metrics import Metrics

logger = get_logger(__name__)


class SlowSubscriber(Exception):
    """A subscriber's queue filled up, so it was dropped instead of slowing everyone else."""


class SealedBlock(BaseModel):
    """A sealed block's logs, kept in the ring for subscribers that resume."""

    block_number: int
    block_hash: str
    logs: list[LogEvent]


class Subscriber:
    """One consumer of the stream with its own queue."""

    def __init__(self, subscription: Subscription, queue_size: int):
        self.subscription = subscription
        self.queue: asyncio.Queue[Event | None] = asyncio.Queue(maxsize=queue_size)
        self.dropped = False

    def offer(self, event: Event) -> bool:
        """Queues an event without waiting and returns False once the subscriber has fallen too far behind."""
        if self.dropped:
            return False
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped = True
            # Make room for the marker so the consumer wakes up and learns it was dropped.
            self.queue.get_nowait()
            self.queue.put_nowait(None)
            return False
        return True

    async def events(self) -> AsyncIterator[Event]:
        """Yields queued events until the subscriber is dropped."""
        while True:
            event = await self.queue.get()
            if event is None:
                raise SlowSubscriber("The subscriber fell behind and was dropped")
            yield event


class Hub:
    """Publishes live events to subscribers and remembers recent sealed blocks for resumes."""

    def __init__(
        self,
        *,
        ring_blocks: int,
        queue_size: int,
        metrics: Metrics,
        ring_max_logs: int = 200_000,
        pending_max_logs: int = 50_000,
    ):
        self.ring_blocks = ring_blocks
        self.queue_size = queue_size
        self.metrics = metrics
        self.ring_max_logs = ring_max_logs
        self.pending_max_logs = pending_max_logs
        self.ring: deque[SealedBlock] = deque()
        self.ring_logs = 0
        self.ring_start: int | None = None
        self.sealed: SealedEvent | None = None
        self.pending: dict[tuple[int, int, str], LogEvent] = {}
        self.subscribers: set[Subscriber] = set()

    def _fan_out(self, event: Event, wants: Callable[[Subscription], bool]) -> None:
        for subscriber in list(self.subscribers):
            if wants(subscriber.subscription) and not subscriber.offer(event):
                self.subscribers.discard(subscriber)
                self.metrics.live_subscriber_drops.inc()
                self.metrics.live_subscribers.set(len(self.subscribers))
                logger.warning("Dropped a stream subscriber that fell behind")

    def publish_head(self, event: HeadEvent) -> None:
        """Sends a new head to subscribers that asked for heads."""
        self._fan_out(event, lambda subscription: subscription.heads)

    def publish_log(self, event: LogEvent) -> None:
        """Sends a log as soon as it is seen; unsealed logs are remembered until their block is sealed."""
        if not event.sealed:
            self.pending[event.key] = event
            if len(self.pending) > self.pending_max_logs:
                # A dict keeps insertion order, so the first key is the oldest. Forgetting it only risks a repeat
                # when its block is sealed, and clients drop repeats by key.
                del self.pending[next(iter(self.pending))]
        self._fan_out(event, lambda subscription: subscription.wants(event))

    def seal(self, from_block: int, block_number: int, block_hash: str, logs: list[LogEvent]) -> None:
        """Records ``[from_block, block_number]`` as sealed with its complete logs and tells every subscriber."""
        if self.ring_start is None:
            self.ring_start = from_block
        by_block: dict[int, list[LogEvent]] = {}
        for log in sorted(logs, key=lambda log: (log.block_number, log.log_index)):
            by_block.setdefault(log.block_number, []).append(log)
        for number, block_logs in by_block.items():
            self.ring.append(SealedBlock(block_number=number, block_hash=block_logs[0].block_hash, logs=block_logs))
            self.ring_logs += len(block_logs)
        self.pending = {key: log for key, log in self.pending.items() if key[0] > block_number}
        self.sealed = SealedEvent(block_number=block_number, block_hash=block_hash)
        oldest = block_number - self.ring_blocks + 1
        while self.ring and (self.ring[0].block_number < oldest or self.ring_logs > self.ring_max_logs):
            evicted = self.ring.popleft()
            self.ring_logs -= len(evicted.logs)
            # A resume from an evicted block is refused, never served with a gap.
            oldest = max(oldest, evicted.block_number + 1)
        self.ring_start = max(self.ring_start, oldest)
        self._fan_out(self.sealed, lambda subscription: True)

    def reorg(self, block_number: int) -> None:
        """Forgets everything above the fork point and tells every subscriber."""
        while self.ring and self.ring[-1].block_number > block_number:
            self.ring_logs -= len(self.ring.pop().logs)
        self.pending = {key: log for key, log in self.pending.items() if key[0] <= block_number}
        if self.sealed is not None and self.sealed.block_number > block_number:
            # The hash of the fork point is not known here; the next seal brings the canonical one.
            self.sealed = SealedEvent(block_number=block_number, block_hash="")
        self._fan_out(ReorgEvent(block_number=block_number), lambda subscription: True)

    def subscribe(self, subscription: Subscription) -> Subscriber:
        """Registers a subscriber, first queueing the ring from ``from_block`` and the unsealed tip.

        Nothing awaits between the replay and the registration, so no event falls between them.
        """
        subscriber = Subscriber(subscription, self.queue_size)
        from_block = subscription.from_block
        if from_block is not None:
            if self.ring_start is None or from_block < self.ring_start:
                subscriber.offer(ErrorEvent(reason="resume point is older than the ring", oldest_block=self.ring_start))
            else:
                for block in self.ring:
                    if block.block_number >= from_block:
                        for log in block.logs:
                            if subscription.wants(log):
                                subscriber.offer(log)
                if self.sealed is not None:
                    subscriber.offer(self.sealed)
        for log in self.pending.values():
            if subscription.wants(log) and (from_block is None or log.block_number >= from_block):
                subscriber.offer(log)
        if subscriber.dropped:
            raise SlowSubscriber("The replay alone overflows the subscriber queue; resume from a later block")
        self.subscribers.add(subscriber)
        self.metrics.live_subscribers.set(len(self.subscribers))
        return subscriber

    def unsubscribe(self, subscriber: Subscriber) -> None:
        """Removes a subscriber."""
        self.subscribers.discard(subscriber)
        self.metrics.live_subscribers.set(len(self.subscribers))

    async def stream(self, subscription: Subscription) -> AsyncIterator[Event]:
        """Yields events for an in-process subscriber until it stops iterating."""
        subscriber = self.subscribe(subscription)
        try:
            async for event in subscriber.events():
                yield event
        finally:
            self.unsubscribe(subscriber)

    def sealed_keys(self, from_block: int, to_block: int, addresses: set[str]) -> set[tuple[int, int, str]] | None:
        """Returns the keys of sealed logs of some addresses in a range, or None when the ring does not cover it."""
        if self.ring_start is None or self.sealed is None:
            return None
        if from_block < self.ring_start or to_block > self.sealed.block_number:
            return None
        return {
            log.key
            for block in self.ring
            if from_block <= block.block_number <= to_block
            for log in block.logs
            if log.address in addresses
        }
