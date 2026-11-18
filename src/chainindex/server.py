"""The service's HTTP side: ``/stream`` for bots, ``/healthz`` and ``/readyz`` for Kubernetes, ``/status`` for people."""

import asyncio
from collections.abc import Callable
from typing import Any

from aiohttp import WSMsgType, web
from pydantic import ValidationError

from chainindex.events import Subscription
from chainindex.hub import Hub, SlowSubscriber, Subscriber
from chainindex.logger import get_logger

logger = get_logger(__name__)

SUBSCRIBE_TIMEOUT_SECONDS = 10.0
SLOW_CONSUMER_CODE = 1013


async def _drain(socket: web.WebSocketResponse) -> None:
    """Reads and ignores client messages so pings and the close handshake are processed."""
    async for message in socket:
        if message.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
            return


async def _send(socket: web.WebSocketResponse, subscriber: Subscriber) -> None:
    """Forwards a subscriber's events until it is dropped or the socket fails."""
    try:
        async for event in subscriber.events():
            await socket.send_str(event.model_dump_json())
    except SlowSubscriber:
        await socket.close(code=SLOW_CONSUMER_CODE, message=b"slow consumer: resume from your last sealed block")
    except (ConnectionResetError, RuntimeError):
        return


def make_app(hub: Hub, *, status: Callable[[], dict[str, Any]], ready: Callable[[], bool]) -> web.Application:
    """Builds the aiohttp application."""
    sockets: set[web.WebSocketResponse] = set()

    async def close_streams(app: web.Application) -> None:
        # Going away (1001) tells clients to reconnect, and shutdown need not wait out each stream.
        for socket in list(sockets):
            await socket.close(code=1001, message=b"server shutting down")

    async def stream(request: web.Request) -> web.WebSocketResponse:
        socket = web.WebSocketResponse(heartbeat=20, max_msg_size=1 << 20)
        await socket.prepare(request)
        try:
            first = await socket.receive_str(timeout=SUBSCRIBE_TIMEOUT_SECONDS)
            subscription = Subscription.model_validate_json(first)
        except (TimeoutError, TypeError, ValidationError) as exc:
            await socket.close(code=1008, message=f"Send a subscription first: {type(exc).__name__}".encode())
            return socket
        try:
            subscriber = hub.subscribe(subscription)
        except SlowSubscriber as exc:
            await socket.close(code=SLOW_CONSUMER_CODE, message=str(exc).encode())
            return socket
        sockets.add(socket)
        reader = asyncio.create_task(_drain(socket))
        sender = asyncio.create_task(_send(socket, subscriber))
        try:
            # Whichever ends first ends both: a client that leaves is unsubscribed at once.
            await asyncio.wait({reader, sender}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            reader.cancel()
            sender.cancel()
            hub.unsubscribe(subscriber)
            sockets.discard(socket)
        return socket

    async def healthz(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def readyz(request: web.Request) -> web.Response:
        return web.Response(text="ready") if ready() else web.Response(status=503, text="warming up")

    async def status_page(request: web.Request) -> web.Response:
        return web.json_response(status())

    app = web.Application()
    app.on_shutdown.append(close_streams)
    app.add_routes(
        [
            web.get("/stream", stream),
            web.get("/healthz", healthz),
            web.get("/readyz", readyz),
            web.get("/status", status_page),
        ]
    )
    return app
