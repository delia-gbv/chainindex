"""The bot side of the live stream: subscribe once, survive reconnects, never see a log twice.

```python
async with StreamClient("ws://chainindex-base.chainindex:8547/stream") as client:
    async for event in client.subscribe(contracts=["aerodrome_cl_pool"], events=["Swap"]):
        ...
```

After a disconnect the client resumes from its last sealed block, so the hub replays exactly what was missed.
Logs already delivered unsealed are not repeated when their block is sealed.
"""

import asyncio
from collections.abc import AsyncIterator, Sequence
from types import TracebackType

import aiohttp

from chainindex.events import (
    EVENT_ADAPTER,
    ErrorEvent,
    Event,
    LogEvent,
    ReorgEvent,
    SealedEvent,
    Subscription,
)
from chainindex.logger import get_logger

logger = get_logger(__name__)

RECONNECT_SECONDS = (0.25, 0.5, 1.0, 2.0, 5.0, 10.0)


class StreamClient:
    """A reconnecting subscriber of one chain service's ``/stream``."""

    def __init__(self, url: str, *, reconnect_seconds: Sequence[float] = RECONNECT_SECONDS):
        self.url = url
        self.reconnect_seconds = tuple(reconnect_seconds)
        self.session: aiohttp.ClientSession | None = None
        self.connected = False

    async def __aenter__(self) -> "StreamClient":
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self, kind: type[BaseException] | None, error: BaseException | None, traceback: TracebackType | None
    ) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None

    async def subscribe(
        self,
        *,
        contracts: Sequence[str] = (),
        addresses: Sequence[str] = (),
        events: Sequence[str] = (),
        heads: bool = False,
        from_block: int | None = None,
    ) -> AsyncIterator[Event]:
        """Yields events forever, reconnecting and resuming as needed."""
        if self.session is None:
            raise RuntimeError("Use StreamClient as an async context manager")
        last_sealed = from_block - 1 if from_block is not None else None
        delivered: set[tuple[int, int, str]] = set()
        failures = 0
        while True:
            subscription = Subscription(
                contracts=list(contracts), addresses=list(addresses), events=list(events), heads=heads,
                from_block=last_sealed + 1 if last_sealed is not None else None,
            )
            try:
                async with self.session.ws_connect(self.url, heartbeat=20, max_msg_size=1 << 24) as socket:
                    await socket.send_str(subscription.model_dump_json())
                    self.connected = True
                    async for message in socket:
                        if message.type != aiohttp.WSMsgType.TEXT:
                            break
                        failures = 0
                        event = EVENT_ADAPTER.validate_json(message.data)
                        if isinstance(event, LogEvent):
                            if event.key in delivered or (last_sealed is not None and event.block_number <= last_sealed):
                                continue
                            delivered.add(event.key)
                        elif isinstance(event, SealedEvent):
                            last_sealed = event.block_number
                            delivered = {key for key in delivered if key[0] > last_sealed}
                        elif isinstance(event, ReorgEvent):
                            delivered = {key for key in delivered if key[0] <= event.block_number}
                            if last_sealed is not None and last_sealed > event.block_number:
                                last_sealed = event.block_number
                        elif isinstance(event, ErrorEvent):
                            logger.warning(f"Stream {self.url}: {event.reason} (oldest block {event.oldest_block})")
                        yield event
                    close_code = socket.close_code
                logger.warning(f"Stream {self.url} closed (code {close_code}); resuming after block {last_sealed}")
            except (aiohttp.ClientError, TimeoutError) as exc:
                logger.warning(f"Stream {self.url} unreachable ({type(exc).__name__}); retrying")
            self.connected = False
            wait = self.reconnect_seconds[min(failures, len(self.reconnect_seconds) - 1)]
            failures += 1
            await asyncio.sleep(wait)
