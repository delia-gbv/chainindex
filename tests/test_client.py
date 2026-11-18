"""StreamClient against the real server and Hub on a local port: live delivery, resumes, and the HTTP probes."""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable

import aiohttp
import pytest
from aiohttp import web
from pydantic import BaseModel, ConfigDict

from chainindex.client import StreamClient
from chainindex.events import Event, HeadEvent, LogEvent, SealedEvent, Subscription
from chainindex.hub import Hub
from chainindex.metrics import Metrics
from chainindex.server import make_app

POOL = "0x00000000000000000000000000000000000000aa"


class Service(BaseModel):
    """A running app: its base URL, its hub, and the server side of every websocket it accepted."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    url: str = ""
    hub: Hub
    sockets: list[web.WebSocketResponse] = []


def log_event(block_number: int, log_index: int = 0, *, event: str = "Swap", sealed: bool = False) -> LogEvent:
    return LogEvent(
        block_number=block_number, block_hash=f"0x{block_number:x}", block_timestamp=1_700_000_000 + block_number,
        transaction_hash=f"0x{block_number:x}{log_index:x}", transaction_index=0, log_index=log_index,
        address=POOL, contract="pool", event=event, args={}, topics=[], data="0x", sealed=sealed,
    )


def sealed_copy(event: LogEvent) -> LogEvent:
    return event.model_copy(update={"sealed": True})


@contextlib.asynccontextmanager
async def serve(ready: Callable[[], bool] = lambda: True) -> AsyncIterator[Service]:
    """Runs the app around a fresh hub on a free local port."""
    service = Service(hub=Hub(ring_blocks=100, queue_size=100, metrics=Metrics("test", isolated=True)))
    app = make_app(service.hub, status=lambda: {}, ready=ready)

    async def remember_socket(request: web.Request, response: web.StreamResponse) -> None:
        if isinstance(response, web.WebSocketResponse):
            service.sockets.append(response)

    app.on_response_prepare.append(remember_socket)
    # A stream handler notices a gone client only on its next event, so shutdown would wait the default 60 s.
    runner = web.AppRunner(app, shutdown_timeout=0.2)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    host, port = runner.addresses[0][:2]
    service.url = f"http://{host}:{port}"
    try:
        yield service
    finally:
        await runner.cleanup()


@contextlib.asynccontextmanager
async def collect(url: str, **subscription) -> AsyncIterator[asyncio.Queue[Event]]:
    """Runs a StreamClient subscription in the background and queues what it yields."""
    received: asyncio.Queue[Event] = asyncio.Queue()

    async def consume() -> None:
        async with StreamClient(f"{url}/stream", reconnect_seconds=(0.01,)) as client:
            async for event in client.subscribe(**subscription):
                await received.put(event)

    consumer = asyncio.create_task(consume())
    try:
        yield received
    finally:
        consumer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer


async def until(condition: Callable[[], bool], seconds: float = 2) -> None:
    """Waits until a condition holds, raising TimeoutError after ``seconds``."""
    async with asyncio.timeout(seconds):
        while not condition():
            await asyncio.sleep(0.005)


async def take(received: asyncio.Queue[Event], count: int) -> list[Event]:
    async with asyncio.timeout(2):
        return [await received.get() for _ in range(count)]


async def test_the_client_yields_what_the_hub_publishes_for_its_subscription():
    async with serve() as service, collect(service.url, events=["Swap"], heads=True) as received:
        hub = service.hub
        await until(lambda: len(hub.subscribers) == 1)
        head = HeadEvent(block_number=11, block_hash="0xb", parent_hash="0xa", timestamp=1_700_000_011)
        swap = log_event(11, 0)
        hub.publish_head(head)
        hub.publish_log(log_event(11, 1, event="Mint"))
        hub.publish_log(swap)
        hub.seal(11, 11, "0xb", [sealed_copy(swap)])
        assert await take(received, 3) == [head, swap, SealedEvent(block_number=11, block_hash="0xb")]


async def test_a_reconnecting_client_resumes_after_its_last_seal_without_repeating_logs():
    async with serve() as service, collect(service.url) as received:
        hub = service.hub
        await until(lambda: len(hub.subscribers) == 1)
        first = log_event(11)
        hub.publish_log(first)
        hub.seal(11, 11, "0xb", [sealed_copy(first)])
        seen_unsealed = log_event(12, 0)
        hub.publish_log(seen_unsealed)
        assert await take(received, 3) == [first, SealedEvent(block_number=11, block_hash="0xb"), seen_unsealed]

        await service.sockets[0].close()
        # Sealed while the client is away, so only the replay can bring the log it never saw.
        never_seen = log_event(12, 1, sealed=True)
        hub.seal(12, 12, "0xc", [sealed_copy(seen_unsealed), never_seen])
        assert await take(received, 2) == [never_seen, SealedEvent(block_number=12, block_hash="0xc")]
        assert [subscriber.subscription.from_block for subscriber in hub.subscribers] == [12]
        assert received.empty()


async def test_healthz_says_ok():
    async with serve() as service, aiohttp.ClientSession() as session:
        async with session.get(f"{service.url}/healthz") as response:
            assert (response.status, await response.text()) == (200, "ok")


@pytest.mark.parametrize(("ready", "status", "text"), [(True, 200, "ready"), (False, 503, "warming up")])
async def test_readyz_reflects_whether_the_service_is_ready(ready, status, text):
    async with serve(ready=lambda: ready) as service, aiohttp.ClientSession() as session:
        async with session.get(f"{service.url}/readyz") as response:
            assert (response.status, await response.text()) == (status, text)


@pytest.mark.parametrize("first_message", ["hello", '{"heads": "maybe"}', b"\x00"])
async def test_a_stream_that_does_not_start_with_a_subscription_is_closed_as_a_policy_violation(first_message):
    async with serve() as service, aiohttp.ClientSession() as session:
        async with session.ws_connect(f"{service.url}/stream") as socket:
            if isinstance(first_message, bytes):
                await socket.send_bytes(first_message)
            else:
                await socket.send_str(first_message)
            message = await socket.receive(timeout=2)
            assert message.type == aiohttp.WSMsgType.CLOSE
            assert socket.close_code == 1008


async def test_a_client_that_disconnects_is_unsubscribed_before_the_next_event():
    async with serve() as service, aiohttp.ClientSession() as session:
        async with session.ws_connect(f"{service.url}/stream") as socket:
            await socket.send_str(Subscription().model_dump_json())
            await until(lambda: len(service.hub.subscribers) == 1)
        await until(lambda: not service.hub.subscribers, seconds=0.5)
