"""RpcPool against local JSON-RPC nodes whose replies are scripted per request."""

import asyncio
import socket
import struct
import time

import pytest
from aiohttp import web

from chainindex.config import LaneSpec
from chainindex.metrics import Metrics
from chainindex.rpc import RpcError, RpcPool, RpcUnavailable, Throttle


def answer(call: dict) -> dict:
    """Echoes a call's method and params, or answers the method ``fail`` with a JSON-RPC error."""
    if call["method"] == "fail":
        return {"jsonrpc": "2.0", "id": call["id"], "error": {"code": -32000, "message": "execution reverted"}}
    return {"jsonrpc": "2.0", "id": call["id"], "result": {"method": call["method"], "params": call["params"]}}


def answers(body: dict | list) -> web.Response:
    """Answers every call, a batch in reverse order so results must be matched by id."""
    if isinstance(body, list):
        return web.json_response([answer(call) for call in reversed(body)])
    return web.json_response(answer(body))


def throttled_by_status(body: dict | list) -> web.Response:
    return web.Response(status=429, headers={"Retry-After": "1"})


def throttled_in_json(body: dict | list) -> web.Response:
    error = {"code": -32016, "message": "Rate limit reached for this key"}
    return web.json_response({"jsonrpc": "2.0", "id": body["id"], "error": error})


def throttled_without_id(body: dict | list) -> web.Response:
    error = {"code": -32005, "message": "Too many requests"}
    return web.json_response({"jsonrpc": "2.0", "id": None, "error": error})


def server_error(body: dict | list) -> web.Response:
    return web.Response(status=500, text="internal error")


def cloudflare_challenge(body: dict | list) -> web.Response:
    page = "<!DOCTYPE html><html lang=\"en-US\"><head><title>Just a moment...</title></head></html>"
    return web.Response(status=403, text=page, content_type="text/html")


def request_blocked(body: dict | list) -> web.Response:
    return web.Response(status=403, text="Request blocked")


def batch_too_large(body: dict | list) -> web.Response:
    return web.json_response(
        {"jsonrpc": "2.0", "id": None, "error": {"code": -32010, "message": "The batch request was too large", "data": "Exceeded max limit of 20"}}
    )


class MockNode:
    """A local JSON-RPC node that answers with its scripted replies first, then normally."""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.requests: list[dict | list] = []
        self.runner: web.AppRunner | None = None
        self.url = ""

    async def handle(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.requests.append(body)
        reply = self.replies.pop(0) if self.replies else answers
        return reply(body)

    async def start(self) -> None:
        app = web.Application()
        app.router.add_post("/", self.handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        host, port = self.runner.addresses[0][:2]
        self.url = f"http://{host}:{port}/"

    async def stop(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()


def closed_port_url() -> str:
    """Returns a local URL nothing listens on, so connecting to it is refused."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}/"


@pytest.fixture
async def start_node():
    """Starts MockNodes and stops them after the test."""
    nodes: list[MockNode] = []

    async def start(*replies) -> MockNode:
        node = MockNode(*replies)
        await node.start()
        nodes.append(node)
        return node

    yield start
    for node in nodes:
        await node.stop()


@pytest.fixture
async def make_pool():
    """Builds ledger-lane RpcPools with a generous budget and closes them after the test."""
    pools: list[RpcPool] = []

    def make(url: str, *, throttle: Throttle | None = None, max_attempts: int = 8) -> RpcPool:
        pool = RpcPool(
            "ledger",
            url,
            LaneSpec(requests_per_second=1000, max_concurrency=4),
            throttle=throttle or Throttle(),
            metrics=Metrics("test", isolated=True),
            timeout_seconds=5,
            max_attempts=max_attempts,
        )
        pools.append(pool)
        return pool

    yield make
    for pool in pools:
        await pool.close()


async def test_a_single_call_is_sent_as_one_object_and_returns_its_result(start_node, make_pool):
    node = await start_node()
    pool = make_pool(node.url)
    assert await pool.request("eth_chainId", []) == {"method": "eth_chainId", "params": []}
    assert isinstance(node.requests[0], dict)


async def test_a_batch_returns_results_in_call_order_with_failed_calls_as_errors(start_node, make_pool):
    node = await start_node()
    pool = make_pool(node.url)
    results = await pool.batch([("eth_getBalance", ["0xa"]), ("fail", []), ("eth_chainId", [])])
    assert results[0] == {"method": "eth_getBalance", "params": ["0xa"]}
    assert isinstance(results[1], RpcError) and results[1].code == -32000
    assert results[2] == {"method": "eth_chainId", "params": []}


async def test_a_json_rpc_error_raises_without_a_retry(start_node, make_pool):
    node = await start_node()
    pool = make_pool(node.url)
    with pytest.raises(RpcError, match="execution reverted"):
        await pool.request("fail", [])
    assert len(node.requests) == 1


@pytest.mark.parametrize("throttled", [throttled_by_status, throttled_in_json, throttled_without_id, cloudflare_challenge])
async def test_a_throttled_request_waits_and_retries_without_resting_the_endpoint(throttled, start_node, make_pool, monkeypatch):
    monkeypatch.setattr("chainindex.rpc.THROTTLE_WAIT_SECONDS", (0.01,))
    monkeypatch.setattr("chainindex.rpc.CHALLENGE_WAIT_SECONDS", 0.01)
    node = await start_node(throttled)
    throttle = Throttle()
    pool = make_pool(node.url, throttle=throttle)
    assert await pool.request("eth_chainId", []) == {"method": "eth_chainId", "params": []}
    assert len(node.requests) == 2
    assert throttle.paused_until > 0
    assert pool.failures == 0


@pytest.mark.parametrize("failing", ["http_500", "connection_refused"])
async def test_a_failed_endpoint_rests_before_it_is_tried_again(failing, start_node, make_pool):
    url = (await start_node(server_error)).url if failing == "http_500" else closed_port_url()
    pool = make_pool(url, max_attempts=1)
    with pytest.raises(RpcUnavailable):
        await pool.request("eth_chainId", [])
    assert pool.failures == 1
    assert pool.resting_until > time.monotonic()


class KeepAliveNode(MockNode):
    """A node that resets a kept-alive connection when another request arrives on it, as one that timed it out would."""

    def __init__(self) -> None:
        super().__init__()
        self.served: set[tuple[str, int]] = set()
        self.dropped = 0

    async def handle(self, request: web.Request) -> web.Response:
        transport = request.transport
        assert transport is not None
        peer = transport.get_extra_info("peername")
        if peer in self.served:
            self.dropped += 1
            transport.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            transport.abort()
            return web.Response()
        self.served.add(peer)
        return await super().handle(request)


async def test_a_connection_the_node_dropped_is_redialed_at_once_without_resting_the_lane(make_pool):
    node = KeepAliveNode()
    await node.start()
    try:
        pool = make_pool(node.url)
        await pool.request("eth_chainId", [])
        started = time.monotonic()
        assert await pool.request("eth_chainId", []) == {"method": "eth_chainId", "params": []}
        assert time.monotonic() - started < 1
        assert node.dropped == 1
        assert pool.failures == 0
        assert pool.resting_until == 0
    finally:
        await node.stop()


class SlowFailingNode(MockNode):
    """A node that answers its first requests with HTTP 500 after a moment, so they are all in flight together."""

    def __init__(self, failures: int) -> None:
        super().__init__(*([server_error] * failures))

    async def handle(self, request: web.Request) -> web.Response:
        await asyncio.sleep(0.05)
        return await super().handle(request)


async def test_requests_failing_together_rest_the_lane_once_not_once_each(make_pool):
    node = SlowFailingNode(failures=4)
    await node.start()
    try:
        pool = make_pool(node.url, max_attempts=1)
        results = await asyncio.gather(*(pool.request("eth_chainId", []) for _ in range(4)), return_exceptions=True)
        assert all(isinstance(result, RpcUnavailable) for result in results)
        assert len(node.requests) == 4
        assert pool.failures == 1
        assert pool.resting_until - time.monotonic() <= 2.0
    finally:
        await node.stop()


async def test_a_lane_still_failing_after_its_rest_rests_twice_as_long(start_node, make_pool, monkeypatch):
    monkeypatch.setattr("chainindex.rpc.REST_SECONDS", 0.05)
    node = await start_node(server_error, server_error)
    pool = make_pool(node.url, max_attempts=1)
    for _outage in range(2):
        with pytest.raises(RpcUnavailable):
            await pool.request("eth_chainId", [])
        await asyncio.sleep(max(0.0, pool.resting_until - time.monotonic()))
    assert pool.failures == 2


async def test_a_lane_gives_up_after_max_attempts(start_node, make_pool, monkeypatch):
    monkeypatch.setattr("chainindex.rpc.REST_SECONDS", 0.01)
    node = await start_node(server_error, server_error, server_error)
    pool = make_pool(node.url, max_attempts=3)
    with pytest.raises(RpcUnavailable, match="after 3 attempts: HTTP 500"):
        await pool.request("eth_chainId", [])
    assert len(node.requests) == 3


@pytest.mark.parametrize(("lane", "paused"), [("stream", False), ("ledger", True), ("backfill", True)])
async def test_a_throttle_pauses_every_lane_but_the_stream(lane, paused):
    throttle = Throttle()
    throttle.hit(0.2)
    started = time.monotonic()
    await throttle.wait(lane)
    assert (time.monotonic() - started > 0.15) == paused


async def test_a_plain_refusal_is_an_answer_the_caller_can_shrink_but_a_challenge_page_pauses_every_background_lane(
    start_node, make_pool, monkeypatch
):
    monkeypatch.setattr("chainindex.rpc.CHALLENGE_WAIT_SECONDS", 30.0)
    refusing = await start_node(request_blocked)
    challenging = await start_node(cloudflare_challenge)
    throttle = Throttle()

    with pytest.raises(RpcError, match="Request blocked"):
        await make_pool(refusing.url, throttle=throttle).request("eth_getLogs", [{}])
    assert throttle.paused_until == 0

    pool = make_pool(challenging.url, throttle=throttle)
    request = asyncio.create_task(pool.request("eth_getLogs", [{}]))
    while throttle.paused_until == 0:
        await asyncio.sleep(0.01)
    request.cancel()
    await asyncio.gather(request, return_exceptions=True)
    assert throttle.paused_until - time.monotonic() > 20


async def test_a_batch_refused_for_its_size_is_an_answer_and_the_lane_does_not_rest(start_node, make_pool):
    node = await start_node(batch_too_large)
    pool = make_pool(node.url)

    answers = await pool.batch([("eth_getBlockByNumber", [hex(number), False]) for number in range(25)])

    assert all(isinstance(answer, RpcError) and "too large" in answer.message for answer in answers)
    assert len(answers) == 25
    assert pool.failures == 0
    assert pool.resting_until == 0
