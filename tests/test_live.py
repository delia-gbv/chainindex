"""The live chain publishes websocket logs at once, seals every range over HTTP, and resolves forks against HTTP."""

import asyncio
import contextlib
import json

from chainindex.abi import topic
from chainindex.events import HeadEvent, LogEvent, ReorgEvent, SealedEvent, Subscription
from chainindex.fetch import LogFetcher
from chainindex.hub import Hub
from chainindex.live import BlockRecorder, LiveChain
from chainindex.metrics import Metrics
from chainindex.models import Header
from chainindex.registry import Registry

from .doubles import MockChain, MockStore, address, chain_config, word

POOL = address(0xA)
STRANGER = address(0x5)
SYNC = topic("Sync(uint256,uint256)")
KIND = [{"name": "pool", "abi": "AerodromePool", "start_block": 1, "addresses": [{"address": POOL}]}]


async def build(chain: MockChain) -> tuple[LiveChain, Hub]:
    config = chain_config(KIND)
    registry = Registry(config)
    registry.reconcile([])
    metrics = Metrics("test", isolated=True)
    hub = Hub(ring_blocks=100, queue_size=1000, metrics=metrics)
    live = LiveChain(
        config, registry, hub, fetcher=LogFetcher(chain, addresses_per_request=8, max_blocks=100), pool=chain,
        metrics=metrics,
    )
    return live, hub


def sync_log(chain: MockChain, block: int, contract_address: str = POOL):
    return chain.add_log(contract_address, block, [SYNC], "0x" + word(5)[2:] + word(7)[2:])


def drain(subscriber) -> list:
    events = []
    while not subscriber.queue.empty():
        events.append(subscriber.queue.get_nowait())
    return events


async def warm(live: LiveChain, chain: MockChain) -> None:
    """Puts the live chain at the node's head with everything up to it sealed."""
    live.on_head(chain.header(chain.head), "ws-0")
    while await live.seal_step():
        pass


async def test_a_websocket_log_reaches_subscribers_before_its_block_is_sealed():
    chain = MockChain(head=100, lane="stream")
    live, hub = await build(chain)
    await warm(live, chain)
    subscriber = hub.subscribe(Subscription(contracts=["pool"]))
    chain.head = 101
    log = sync_log(chain, 101)

    live.on_head(chain.header(101), "ws-0")
    live.on_log(log, "ws-0")
    fresh = drain(subscriber)
    await live.seal_step()
    after_seal = drain(subscriber)

    assert [(event.event, event.sealed, event.args) for event in fresh] == [("Sync", False, {"reserve0": 5, "reserve1": 7})]
    assert after_seal == [SealedEvent(block_number=101, block_hash=chain.block_hash(101))]


async def test_the_seal_delivers_logs_no_websocket_delivered():
    chain = MockChain(head=100, lane="stream")
    live, hub = await build(chain)
    await warm(live, chain)
    subscriber = hub.subscribe(Subscription())
    chain.head = 102
    sync_log(chain, 102)
    live.on_head(chain.header(101), "ws-0")
    live.on_head(chain.header(102), "ws-0")

    await live.seal_step()
    events = drain(subscriber)

    assert [type(event) for event in events] == [LogEvent, SealedEvent]
    assert events[0].sealed and events[0].block_number == 102


async def test_a_log_both_websockets_deliver_is_published_once():
    chain = MockChain(head=100, lane="stream")
    live, hub = await build(chain)
    await warm(live, chain)
    subscriber = hub.subscribe(Subscription())
    log = sync_log(chain, 101)

    live.on_log(log, "ws-0")
    live.on_log(log, "ws-1")

    assert len(drain(subscriber)) == 1


async def test_logs_of_addresses_outside_the_registry_are_not_published():
    chain = MockChain(head=100, lane="stream")
    live, hub = await build(chain)
    await warm(live, chain)
    subscriber = hub.subscribe(Subscription())

    live.on_log(sync_log(chain, 101, STRANGER), "ws-0")

    assert drain(subscriber) == []


async def test_a_head_the_http_node_disowns_is_ignored():
    chain = MockChain(head=101, lane="stream")
    live, hub = await build(chain)
    await warm(live, chain)
    subscriber = hub.subscribe(Subscription(heads=True))
    stale = Header(number=101, hash="0x" + "ee" * 32, parent_hash=chain.block_hash(100), timestamp=1)

    live.on_head(stale, "ws-1")
    await asyncio.gather(*live.background)

    assert drain(subscriber) == []
    assert live.head == chain.header(101)


async def test_a_block_a_jump_skipped_arriving_late_fills_the_gap_without_a_reorg():
    chain = MockChain(head=103, lane="stream")
    live, hub = await build(chain)
    for number in (100, 101, 103):
        live.on_head(chain.header(number), "ws-1")
    subscriber = hub.subscribe(Subscription(heads=True))

    live.on_head(chain.header(102), "ws-0")
    await asyncio.gather(*live.background)

    assert drain(subscriber) == []
    assert live.head == chain.header(103)
    assert live.canonical_hash(102) == chain.block_hash(102)


async def test_a_fork_check_the_http_node_cannot_answer_ignores_the_head():
    chain = MockChain(head=101, lane="stream")
    live, hub = await build(chain)
    await warm(live, chain)
    subscriber = hub.subscribe(Subscription(heads=True))
    chain.head = 100  # the HTTP node has not seen block 101 yet
    rival = Header(number=101, hash="0x" + "ee" * 32, parent_hash=chain.block_hash(100), timestamp=1)

    live.on_head(rival, "ws-1")
    await asyncio.gather(*live.background)

    assert drain(subscriber) == []
    assert live.head == chain.header(101)


async def test_a_canonical_fork_rolls_subscribers_back_to_the_fork_point():
    chain = MockChain(head=102, lane="stream")
    live, hub = await build(chain)
    for number in (100, 101, 102):
        live.on_head(chain.header(number), "ws-0")
    subscriber = hub.subscribe(Subscription(heads=True))
    chain.fork(101, "0x" + "a1" * 32)
    chain.fork(102, "0x" + "a2" * 32)

    live.on_head(chain.header(102), "ws-0")
    await asyncio.gather(*live.background)
    events = drain(subscriber)

    assert events[0] == ReorgEvent(block_number=100)
    assert [event.block_hash for event in events[1:] if isinstance(event, HeadEvent)] == ["0x" + "a1" * 32, "0x" + "a2" * 32]
    assert live.canonical_hash(102) == "0x" + "a2" * 32


async def test_without_websocket_heads_the_seal_follows_the_http_head():
    chain = MockChain(head=100, lane="stream")
    live, hub = await build(chain)

    while await live.seal_step():
        pass

    assert live.sealed == 100
    assert hub.sealed == SealedEvent(block_number=100, block_hash=chain.block_hash(100))


async def test_each_block_records_when_its_head_first_reached_us_and_a_fetched_one_records_nothing():
    chain = MockChain(head=100, lane="stream")
    live, _hub = await build(chain)
    store = MockStore()
    recorder = BlockRecorder(live, store, chain, confirmations=0)
    live.on_head(chain.header(97), "ws-0", received_at=999.8)
    await recorder.step()
    live.on_head(chain.header(98), "ws-0", received_at=1_000.4)
    live.on_head(chain.header(98), "ws-1", received_at=1_000.9)
    live.on_head(chain.header(100), "ws-0", received_at=1_001.2)

    await recorder.step()

    assert [header.number for header in store.headers] == [97, 98, 99, 100]
    assert store.arrivals == {
        (97, chain.header(97).hash): 999.8, (98, chain.header(98).hash): 1_000.4, (100, chain.header(100).hash): 1_001.2,
    }


async def test_block_latency_places_each_block_in_its_shared_second_and_waits_for_the_second_to_close():
    chain = MockChain(head=100, lane="stream")
    live, _hub = await build(chain)
    recorder = BlockRecorder(live, MockStore(), chain, confirmations=0)
    # A 0.1 s chain: blocks 1-3 share second 10, 4-5 second 11, 6 opens second 12. Each arrives 0.1 s after its place.
    stamps = {1: 10, 2: 10, 3: 10, 4: 11, 5: 11, 6: 12}
    headers = [Header(number=n, hash=f"0x{n:064x}", parent_hash=f"0x{n - 1:064x}", timestamp=s) for n, s in stamps.items()]
    places = {1: 10.0, 2: 10 + 1 / 3, 3: 10 + 2 / 3, 4: 11.0, 5: 11.5, 6: 12.0}
    live.arrivals = {(h.number, h.hash): places[h.number] + 0.1 for h in headers}

    # The first step skips its first second (it may have started earlier) and holds its last open.
    recorder.observe_latency(headers[:5])
    recorder.observe_latency(headers[5:])

    registry = live.metrics.registry
    assert registry.get_sample_value("test_live_head_latency_seconds_count") == 2  # second 11 only
    # Both 0.1 s late: in the 0.1 s bucket, none in the 0.05 s one (signed buckets keep no sum).
    assert registry.get_sample_value("test_live_head_latency_seconds_bucket", {"le": "0.1"}) == 2
    assert registry.get_sample_value("test_live_head_latency_seconds_bucket", {"le": "0.05"}) == 0


async def test_a_restart_fills_a_gap_up_to_its_limit_and_leaves_a_longer_one():
    chain = MockChain(head=100, lane="stream")
    live, _hub = await build(chain)
    live.on_head(chain.header(100), "ws-0")
    filling = MockStore()
    await filling.insert_blocks([chain.header(50)])
    leaving = MockStore()
    await leaving.insert_blocks([chain.header(50)])

    await BlockRecorder(live, filling, chain, confirmations=0, max_gap=60).step()
    await BlockRecorder(live, leaving, chain, confirmations=0, max_gap=40).step()

    assert [header.number for header in filling.headers] == list(range(50, 101))
    assert [header.number for header in leaving.headers] == [50, 100]


async def reconnects_within(monkeypatch, *, answer: bool, seconds: float = 1.0) -> int:
    """Counts the connections one socket feed opens to a node that drops each one, answering its subscription or not."""
    from websockets.asyncio.server import serve

    from chainindex import live as live_module
    from chainindex.live import SocketFeed

    # Two quick retries, then a wait the test outlasts: a feed whose backoff never starts over stops at three connections.
    monkeypatch.setattr(live_module, "RECONNECT_SECONDS", (0.0, 0.0, 60.0))
    connections = 0

    async def drop(socket) -> None:
        nonlocal connections
        connections += 1
        # Answers every subscription for a moment, then hangs up.
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(0.05):
                while answer:
                    request = json.loads(await socket.recv())
                    await socket.send(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": f"0x{request['id']:x}"}))

    live, _hub = await build(MockChain(head=100, lane="stream"))
    async with serve(drop, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        feed = SocketFeed("ws-0", f"ws://127.0.0.1:{port}", live, metrics=Metrics("test", isolated=True))
        task = asyncio.create_task(feed.run())
        await asyncio.sleep(seconds)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return connections


async def test_a_websocket_that_subscribed_before_dropping_reconnects_at_once(monkeypatch):
    assert await reconnects_within(monkeypatch, answer=True) > 3


async def test_a_websocket_that_never_subscribes_backs_off(monkeypatch):
    assert await reconnects_within(monkeypatch, answer=False) == 3
