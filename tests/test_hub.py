"""Hub fan-out, sealing, resumes from the ring, reorgs and slow subscribers."""

import contextlib

import pytest

from chainindex.events import (
    ErrorEvent,
    Event,
    HeadEvent,
    LogEvent,
    ReorgEvent,
    SealedEvent,
    Subscription,
)
from chainindex.hub import Hub, SlowSubscriber, Subscriber
from chainindex.metrics import Metrics

POOL = "0x00000000000000000000000000000000000000aa"
GAUGE = "0x00000000000000000000000000000000000000bb"


def log_event(
    block_number: int, log_index: int = 0, *, address: str = POOL, contract: str = "pool", event: str = "Swap",
    sealed: bool = False,
) -> LogEvent:
    return LogEvent(
        block_number=block_number, block_hash=f"0x{block_number:x}", block_timestamp=1_700_000_000 + block_number,
        transaction_hash=f"0x{block_number:x}{log_index:x}", transaction_index=0, log_index=log_index,
        address=address, contract=contract, event=event, args={}, topics=[], data="0x", sealed=sealed,
    )


def sealed_copy(event: LogEvent) -> LogEvent:
    return event.model_copy(update={"sealed": True})


def make_hub(*, ring_blocks: int = 100, queue_size: int = 100, ring_max_logs: int = 1000, pending_max_logs: int = 1000) -> Hub:
    return Hub(
        ring_blocks=ring_blocks, queue_size=queue_size, metrics=Metrics("test", isolated=True),
        ring_max_logs=ring_max_logs, pending_max_logs=pending_max_logs,
    )


def queued(subscriber: Subscriber) -> list[Event | None]:
    """Takes every event waiting in a subscriber's queue."""
    events = []
    while not subscriber.queue.empty():
        events.append(subscriber.queue.get_nowait())
    return events


def seal_blocks(hub: Hub, first: int, last: int) -> list[LogEvent]:
    """Seals blocks one at a time, each with one pool log, and returns those logs."""
    logs = [log_event(block_number, sealed=True) for block_number in range(first, last + 1)]
    for log in logs:
        hub.seal(log.block_number, log.block_number, log.block_hash, [log])
    return logs


@pytest.mark.parametrize(
    ("subscription", "receives"),
    [
        (Subscription(), True),
        (Subscription(contracts=["pool"]), True),
        (Subscription(contracts=["gauge"]), False),
        (Subscription(addresses=["0x00000000000000000000000000000000000000AA"]), True),
        (Subscription(addresses=[GAUGE]), False),
        (Subscription(events=["Swap"]), True),
        (Subscription(events=["Mint"]), False),
    ],
)
def test_a_live_log_reaches_only_subscribers_that_want_it(subscription, receives):
    hub = make_hub()
    subscriber = hub.subscribe(subscription)
    log = log_event(10)
    hub.publish_log(log)
    assert queued(subscriber) == ([log] if receives else [])


@pytest.mark.parametrize("heads", [True, False])
def test_a_head_reaches_only_subscribers_that_asked_for_heads(heads):
    hub = make_hub()
    subscriber = hub.subscribe(Subscription(heads=heads))
    head = HeadEvent(block_number=10, block_hash="0xa", parent_hash="0x9", timestamp=1_700_000_010)
    hub.publish_head(head)
    assert queued(subscriber) == ([head] if heads else [])


def test_a_seal_moves_its_logs_from_pending_into_the_ring_and_tells_subscribers():
    hub = make_hub()
    subscriber = hub.subscribe(Subscription(contracts=["gauge"]))
    unsealed = [log_event(10), log_event(11), log_event(12)]
    for log in unsealed:
        hub.publish_log(log)
    hub.seal(10, 11, "0xb", [sealed_copy(log) for log in unsealed[:2]])
    assert list(hub.pending) == [unsealed[2].key]
    assert [block.block_number for block in hub.ring] == [10, 11]
    assert queued(subscriber) == [SealedEvent(block_number=11, block_hash="0xb")]


def test_a_resume_replays_the_ring_then_the_seal_then_pending_logs_then_live_ones():
    hub = make_hub()
    sealed_logs = seal_blocks(hub, 10, 12)
    pending_log = log_event(13)
    hub.publish_log(pending_log)
    subscriber = hub.subscribe(Subscription(from_block=11))
    live_log = log_event(14)
    hub.publish_log(live_log)
    assert queued(subscriber) == [
        sealed_logs[1], sealed_logs[2], SealedEvent(block_number=12, block_hash="0xc"), pending_log, live_log,
    ]


@pytest.mark.parametrize(("sealed_through", "oldest_block"), [(None, None), (20, 18)])
def test_a_resume_older_than_the_ring_gets_an_error(sealed_through, oldest_block):
    hub = make_hub(ring_blocks=3)
    if sealed_through is not None:
        seal_blocks(hub, 10, sealed_through)
    subscriber = hub.subscribe(Subscription(from_block=17))
    assert queued(subscriber) == [ErrorEvent(reason="resume point is older than the ring", oldest_block=oldest_block)]


def test_a_reorg_trims_the_ring_and_pending_logs_and_reaches_every_subscriber():
    hub = make_hub()
    seal_blocks(hub, 10, 12)
    hub.publish_log(log_event(13))
    subscribers = [hub.subscribe(Subscription(contracts=["pool"])), hub.subscribe(Subscription(contracts=["gauge"]))]
    for subscriber in subscribers:
        queued(subscriber)
    hub.reorg(10)
    assert [block.block_number for block in hub.ring] == [10]
    assert hub.pending == {}
    assert hub.sealed == SealedEvent(block_number=10, block_hash="")
    for subscriber in subscribers:
        assert queued(subscriber) == [ReorgEvent(block_number=10)]


async def test_a_subscriber_whose_queue_overflows_is_dropped_while_others_keep_receiving():
    hub = make_hub(queue_size=2)
    slow = hub.subscribe(Subscription())
    fast = hub.subscribe(Subscription())
    logs = [log_event(block_number) for block_number in (10, 11, 12)]
    received = []
    for log in logs:
        hub.publish_log(log)
        received += queued(fast)
    assert received == logs
    assert hub.subscribers == {fast}
    assert hub.metrics.registry.get_sample_value("test_live_subscriber_drops_total") == 1
    # The oldest event made room for the drop marker.
    seen = []
    with pytest.raises(SlowSubscriber):
        async for event in slow.events():
            seen.append(event)
    assert seen == [logs[1]]


@pytest.mark.parametrize(("sealed_through", "from_block", "to_block"), [(None, 10, 12), (12, 9, 11), (12, 11, 13)])
def test_sealed_keys_is_none_for_a_range_the_ring_does_not_cover(sealed_through, from_block, to_block):
    hub = make_hub()
    if sealed_through is not None:
        seal_blocks(hub, 10, sealed_through)
    assert hub.sealed_keys(from_block, to_block, {POOL}) is None


def test_sealed_keys_lists_the_sealed_logs_of_the_given_addresses():
    hub = make_hub()
    pool_log = log_event(10, 0, sealed=True)
    gauge_log = log_event(10, 1, address=GAUGE, contract="gauge", sealed=True)
    later_log = log_event(12, sealed=True)
    hub.seal(10, 12, "0xc", [pool_log, gauge_log, later_log])
    assert hub.sealed_keys(10, 11, {POOL}) == {pool_log.key}


async def test_a_stream_unsubscribes_when_the_consumer_stops():
    hub = make_hub()
    pending_log = log_event(10)
    hub.publish_log(pending_log)
    stream = hub.stream(Subscription())
    async with contextlib.aclosing(stream):
        assert await anext(stream) == pending_log
        assert len(hub.subscribers) == 1
    assert hub.subscribers == set()


def test_the_ring_evicts_its_oldest_blocks_past_its_log_cap_and_refuses_resumes_into_them():
    hub = make_hub(ring_max_logs=3)
    seal_blocks(hub, 1, 5)

    resumed = hub.subscribe(Subscription(from_block=2))

    assert [block.block_number for block in hub.ring] == [3, 4, 5]
    assert hub.ring_logs == 3 and hub.ring_start == 3
    assert isinstance(queued(resumed)[0], ErrorEvent)


def test_unsealed_logs_past_their_cap_are_forgotten_oldest_first():
    hub = make_hub(pending_max_logs=2)

    for block_number in (1, 2, 3):
        hub.publish_log(log_event(block_number))

    assert sorted(key[0] for key in hub.pending) == [2, 3]
