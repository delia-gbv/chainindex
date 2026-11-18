"""The ledger writes every stream's confirmed logs once: backfill, tip, runtime additions, factory children."""

import asyncio

import pytest
from clickhouse_connect.driver.exceptions import OperationalError
from eth_abi import encode

from chainindex import ledger as ledger_module
from chainindex.abi import topic
from chainindex.collect import collect
from chainindex.fetch import LogFetcher
from chainindex.ledger import Ledger
from chainindex.metrics import Metrics
from chainindex.registry import Registry
from chainindex.rpc import RpcUnavailable
from chainindex.store import ContractRow, Cursor

from .doubles import MockChain, MockStore, address, chain_config, word

FACTORY = address(0xF)
POOL_A = address(0xA)
POOL_B = address(0xB)
STRANGER = address(0x5)
USDC = address(0xC)
SWAP = topic("Swap(address,address,int256,int256,uint160,uint128,int24)")
POOL_CREATED = topic("PoolCreated(address,address,uint24,int24,address)")


def swap(chain: MockChain, pool: str, block: int) -> None:
    data = "0x" + encode(["int256", "int256", "uint160", "uint128", "int24"], [1, -1, 2**96, 10**18, -5]).hex()
    chain.add_log(pool, block, [SWAP, word(address(1)), word(address(2))], data)


def pool_created(chain: MockChain, block: int, pool: str, token0: str = address(1)) -> None:
    data = "0x" + encode(["int24", "address"], [60, pool]).hex()
    chain.add_log(FACTORY, block, [POOL_CREATED, word(token0), word(address(2)), word(3000)], data)


def factory_kind(**pool_overrides):
    pool = {"name": "pool", "abi": "UniswapV3Pool", "start_block": 100, **pool_overrides}
    return [
        {
            "name": "factory", "abi": "UniswapV3Factory",
            "addresses": [{"address": FACTORY, "start_block": 100}],
            "creates": {"event": "PoolCreated", "param": "pool", "contract": "pool"},
        },
        pool,
    ]


async def build(contracts, chain: MockChain, store: MockStore, *, config_overrides=None, **ledger_options) -> Ledger:
    config = chain_config(contracts, **(config_overrides or {}))
    registry = Registry(config)
    await store.record_contracts(registry.reconcile(await store.contracts()))
    ledger = Ledger(
        config, registry, store,
        tip_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        backfill_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        head_pool=chain, metrics=Metrics("test", isolated=True), **ledger_options,
    )
    await ledger.load()
    return ledger


async def settle(ledger: Ledger) -> None:
    """Runs both lanes until neither has work left."""
    for _round in range(200):
        tip_behind = await ledger.tip_step()
        backfilled = await ledger.backfill_step()
        if not tip_behind and not backfilled:
            return
    raise AssertionError("The ledger never settled")


LISTED = [{"name": "pool", "abi": "UniswapV3Pool", "start_block": 100, "addresses": [{"address": POOL_A}, {"address": POOL_B}]}]


async def test_a_fresh_ledger_backfills_history_then_follows_the_tip():
    chain, store = MockChain(head=1000), MockStore()
    for pool, block in ((POOL_A, 150), (POOL_B, 500), (POOL_A, 995)):
        swap(chain, pool, block)
    ledger = await build(LISTED, chain, store)

    await settle(ledger)
    assert store.blocks_with_logs() == [150, 500]
    assert {cursor.to_block for cursor in store.cursor_rows.values()} == {990}

    chain.head = 1010
    await settle(ledger)
    assert store.blocks_with_logs() == [150, 500, 995]
    assert {store.lanes[key] for key in store.logs} == {"backfill", "tip"}


async def test_a_kind_without_an_abi_stores_every_log_its_addresses_emit():
    chain, store = MockChain(head=1000), MockStore()
    chain.add_log(USDC, 150, [topic("Anything(uint256)")], "0x" + "00" * 32)
    chain.add_log(USDC, 160, [])
    swap(chain, POOL_A, 170)
    ledger = await build([{"name": "token", "start_block": 100, "addresses": [{"address": USDC}]}], chain, store)

    await settle(ledger)

    assert store.blocks_with_logs() == [150, 160]
    assert all("topics" not in log_filter for log_filter in chain.get_logs_requests())


async def test_a_restarted_ledger_resumes_from_its_cursors_without_refetching():
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 150)
    await settle(await build(LISTED, chain, store))
    fetched_before = len(chain.get_logs_requests())

    await settle(await build(LISTED, chain, store))

    assert len(chain.get_logs_requests()) == fetched_before


async def test_a_contract_added_at_runtime_is_backfilled_and_joins_the_tip_lane():
    chain, store = MockChain(head=1000), MockStore()
    only_a = [{"name": "pool", "abi": "UniswapV3Pool", "start_block": 100, "addresses": [{"address": POOL_A}]}]
    ledger = await build(only_a, chain, store)
    await settle(ledger)
    swap(chain, POOL_B, 300)
    swap(chain, POOL_B, 1005)
    await store.record_contracts([ContractRow(address=POOL_B, contract="pool", start_block=100, source="cli")])
    ledger.registry.load(await store.contracts())

    chain.head = 1020
    await settle(ledger)

    assert store.blocks_with_logs() == [300, 1005]
    assert ledger.cursors[f"address:{POOL_B}"].to_block == ledger.tip_block == 1010


async def test_a_stream_a_few_blocks_behind_the_tip_joins_the_tip_lane_without_a_backfill_pass():
    chain, store = MockChain(head=1000), MockStore()
    only_a = [{"name": "pool", "abi": "UniswapV3Pool", "start_block": 100, "addresses": [{"address": POOL_A}]}]
    windows = []
    ledger = await build(only_a, chain, store, on_tip_window=lambda streams, start, end, logs: windows.append((start, end, logs)))
    await settle(ledger)
    swap(chain, POOL_A, 1006)
    swap(chain, POOL_B, 987)
    swap(chain, POOL_B, 1005)
    await store.record_contracts([ContractRow(address=POOL_B, contract="pool", start_block=985, source="cli")])
    ledger.registry.load(await store.contracts())
    chain.head = 1020

    await ledger.tip_step()

    assert store.blocks_with_logs() == [987, 1005, 1006]
    assert ledger.cursors[f"address:{POOL_B}"].to_block == ledger.tip_block == 1010
    assert [(start, end, [log.block_number for log in logs]) for start, end, logs in windows[-1:]] == [(991, 1010, [1005, 1006])]


async def test_a_stream_catching_up_after_a_restart_is_backfilled_before_a_new_contracts_history():
    chain, store = MockChain(head=1000), MockStore()
    only_a = [{"name": "pool", "abi": "UniswapV3Pool", "start_block": 100, "addresses": [{"address": POOL_A}]}]
    await settle(await build(only_a, chain, store))
    chain.head = 1030
    restarted = await build(LISTED, chain, store)

    await restarted.tip_step()
    await restarted.backfill_step()

    assert restarted.cursors[f"address:{POOL_A}"].to_block >= restarted.tip_block
    assert restarted.cursors[f"address:{POOL_B}"].to_block < restarted.tip_block


async def test_children_of_a_factory_are_kept_from_their_creation_when_the_kind_tracks_all():
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 150)
    pool_created(chain, 200, POOL_A)
    swap(chain, POOL_A, 250)
    swap(chain, STRANGER, 260)
    ledger = await build(factory_kind(track="all", fetch="topics"), chain, store)

    await settle(ledger)

    assert store.addresses_with_logs() == {FACTORY, POOL_A}
    assert store.blocks_with_logs() == [200, 250]
    assert [(row.address, row.source) for row in store.contract_rows if row.contract == "pool"] == [(POOL_A, "factory")]


@pytest.mark.parametrize("pool_fetch", ["addresses", "topics"])
async def test_children_of_a_factory_scanned_by_topic_are_found(pool_fetch):
    chain, store = MockChain(head=1000), MockStore()
    pool_created(chain, 200, POOL_A)
    swap(chain, POOL_A, 250)
    swap(chain, STRANGER, 260)
    factory, pool = factory_kind(track="all", fetch=pool_fetch)
    # The child kind first, so its scan would run before the factory's if the file's order were kept.
    ledger = await build([pool, {**factory, "fetch": "topics"}], chain, store)

    await settle(ledger)

    assert [(row.address, row.source) for row in store.contract_rows if row.contract == "pool"] == [(POOL_A, "factory")]
    assert store.addresses_with_logs() == {FACTORY, POOL_A}
    assert store.blocks_with_logs() == [200, 250]


async def test_a_scanned_factory_registers_its_children_before_their_scan_is_filtered():
    # The live seal has no factory gate: one window holds the creation and the child's first log.
    chain = MockChain(head=1000)
    pool_created(chain, 200, POOL_A)
    swap(chain, POOL_A, 250)
    factory, pool = factory_kind(track="all", fetch="topics")
    registry = Registry(chain_config([pool, {**factory, "fetch": "topics"}]))
    registry.reconcile([])

    async def register(logs):
        registry.apply(registry.discover(logs), recorded=False)

    fetcher = LogFetcher(chain, addresses_per_request=2, max_blocks=100)
    logs = await collect(registry, registry.streams(), 100, 300, fetcher, on_children=register)

    assert [(log.address, log.block_number) for log in logs] == [(FACTORY, 200), (POOL_A, 250)]


async def test_children_of_a_kind_that_tracks_all_by_address_are_fetched_from_their_creation_by_address():
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 150)
    pool_created(chain, 200, POOL_A)
    swap(chain, POOL_A, 250)
    pool_created(chain, 600, POOL_B)
    swap(chain, POOL_B, 650)
    swap(chain, POOL_B, 995)
    swap(chain, STRANGER, 260)
    ledger = await build(factory_kind(track="all"), chain, store)

    await settle(ledger)
    chain.head = 1010
    await settle(ledger)

    assert store.addresses_with_logs() == {FACTORY, POOL_A, POOL_B}
    assert store.blocks_with_logs() == [200, 250, 600, 650, 995]
    assert all("address" in log_filter for log_filter in chain.get_logs_requests())
    assert ledger.cursors[f"address:{POOL_B}"].from_block == 600


async def test_children_a_factory_added_later_created_in_the_past_get_their_history():
    chain, store = MockChain(head=1000), MockStore()
    ledger = await build(factory_kind(track="all", fetch="topics"), chain, store)
    await settle(ledger)
    late_factory = address(0xE)
    data = "0x" + encode(["int24", "address"], [60, POOL_B]).hex()
    chain.add_log(late_factory, 300, [POOL_CREATED, word(address(1)), word(address(2)), word(3000)], data)
    swap(chain, POOL_B, 400)
    await store.record_contracts([ContractRow(address=late_factory, contract="factory", start_block=100, source="cli")])
    ledger.registry.load(await store.contracts())

    await settle(ledger)

    assert store.addresses_with_logs() == {late_factory, POOL_B}
    assert ledger.cursors[f"address:{POOL_B}"].done


async def test_only_children_whose_creation_matches_are_indexed():
    chain, store = MockChain(head=1000), MockStore()
    pool_created(chain, 200, POOL_A, token0=USDC)
    pool_created(chain, 210, POOL_B)
    swap(chain, POOL_A, 250)
    swap(chain, POOL_B, 260)
    ledger = await build(factory_kind(track="matching", match={"token0": [USDC]}), chain, store)

    await settle(ledger)

    assert store.addresses_with_logs() == {FACTORY, POOL_A}


async def test_a_member_added_to_a_topic_scanned_kind_has_its_past_fetched_on_its_own():
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 150)
    swap(chain, POOL_B, 300)
    kind = [{"name": "pool", "abi": "UniswapV3Pool", "fetch": "topics", "start_block": 100, "addresses": [{"address": POOL_A}]}]
    ledger = await build(kind, chain, store)
    await settle(ledger)
    assert store.addresses_with_logs() == {POOL_A}

    await store.record_contracts([ContractRow(address=POOL_B, contract="pool", start_block=100, source="cli")])
    ledger.registry.load(await store.contracts())
    await settle(ledger)

    assert store.blocks_with_logs() == [150, 300]
    catch_up = ledger.cursors[f"address:{POOL_B}"]
    assert catch_up.done and catch_up.until_block == 990


async def test_a_window_whose_hashes_the_live_chain_disputes_is_fetched_again(monkeypatch):
    monkeypatch.setattr(ledger_module, "HASH_RETRY_SECONDS", 0)
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 995)
    disputes = iter(["0xstale"])

    def canonical_hash(number: int) -> str | None:
        return next(disputes, chain.block_hash(number)) if number == 995 else None

    ledger = await build(LISTED, chain, store, canonical_hash=canonical_hash)
    await settle(ledger)
    chain.head = 1010
    fetched_before = len(chain.get_logs_requests())
    await settle(ledger)

    assert store.blocks_with_logs() == [995]
    assert len(chain.get_logs_requests()) - fetched_before == 2


@pytest.mark.parametrize("start_block, expected", [(100, [150, 500]), (400, [500])])
async def test_an_address_only_has_logs_from_its_start_block(start_block, expected):
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 150)
    swap(chain, POOL_A, 500)
    kind = [{"name": "pool", "abi": "UniswapV3Pool", "addresses": [{"address": POOL_A, "start_block": start_block}]}]

    await settle(await build(kind, chain, store))

    assert store.blocks_with_logs() == expected


class MockChainRefusingOneAddress(MockChain):
    """A chain whose provider cannot serve one address's history."""

    def __init__(self, head: int, refused: str):
        super().__init__(head)
        self.refused = refused

    async def batch(self, calls):
        for method, params in calls:
            if method == "eth_getLogs" and self.refused in (params[0].get("address") or []):
                raise RpcUnavailable(f"{self.refused} history is pruned")
        return await super().batch(calls)


async def test_a_stream_the_provider_cannot_serve_does_not_hold_up_the_others():
    chain, store = MockChainRefusingOneAddress(head=1000, refused=POOL_A), MockStore()
    swap(chain, POOL_A, 150)
    swap(chain, POOL_B, 500)
    kind = [
        {"name": "pool", "abi": "UniswapV3Pool", "addresses": [{"address": POOL_A, "start_block": 100}]},
        {"name": "other", "abi": "UniswapV3Pool", "addresses": [{"address": POOL_B, "start_block": 120}]},
    ]
    ledger = await build(kind, chain, store)

    await settle(ledger)

    assert store.blocks_with_logs() == [500]
    assert f"address:{POOL_A}" in ledger.resting


async def test_the_backfill_window_shrinks_on_dense_ranges_and_grows_back_on_sparse_ones():
    chain, store = MockChain(head=1000), MockStore()
    for block in range(100, 140):
        for _repeat in range(5):
            swap(chain, POOL_A, block)
    contracts = [{"name": "pool", "abi": "UniswapV3Pool", "start_block": 100, "addresses": [{"address": POOL_A}]}]
    config_overrides = {"ledger": {"confirmations": 10, "poll_seconds": 0, "backfill_window_blocks": 200, "backfill_window_logs": 50}}
    ledger = await build(contracts, chain, store, config_overrides=config_overrides)
    await ledger.tip_step()

    await ledger.backfill_step()
    after_dense = ledger.backfill_windows["addresses"]
    await settle(ledger)

    assert after_dense < 200
    assert ledger.backfill_windows["addresses"] == 200
    assert len(store.logs) == 200


async def test_streams_that_start_at_different_blocks_end_up_sharing_requests():
    chain, store = MockChain(head=5000), MockStore()
    starts = {address(0x10 + offset): 100 + 37 * offset for offset in range(6)}
    for pool, start in starts.items():
        swap(chain, pool, start)
        swap(chain, pool, start - 1)
    kind = [{"name": "pool", "abi": "UniswapV3Pool", "addresses": [{"address": pool, "start_block": start} for pool, start in starts.items()]}]
    ledger = await build(kind, chain, store)
    await ledger.tip_step()

    windows = 0
    while await ledger.backfill_step():
        windows += 1

    # Six staggered streams over ~4,800 blocks in 200-block windows: about one walk, not six.
    assert windows <= 30
    assert store.blocks_with_logs() == sorted(starts.values())
    assert {ledger.cursors[f"address:{pool}"].from_block for pool in starts} == set(starts.values())


async def test_a_restart_puts_the_tip_lane_at_the_head_even_when_every_stream_is_far_behind():
    chain, store = MockChain(head=100_000), MockStore()
    ledger = await build(LISTED, chain, store)
    await ledger.tip_step()
    await ledger.backfill_step()

    restarted = await build(LISTED, chain, store)
    await restarted.tip_step()

    assert restarted.tip_block == 100_000 - 10


async def test_a_window_that_ends_before_a_streams_start_never_moves_its_cursor_back():
    chain, store = MockChain(head=100_000), MockStore()
    kind = [
        {"name": "pool", "abi": "UniswapV3Pool", "addresses": [{"address": POOL_A, "start_block": 100}]},
        {"name": "late", "abi": "UniswapV3Pool", "addresses": [{"address": POOL_B, "start_block": 90_000}]},
    ]
    ledger = await build(kind, chain, store)
    stream_b = next(stream for stream in ledger.registry.streams() if stream.addresses == [POOL_B])

    await ledger.process([stream_b], 100, 299, lane="tip", fetcher=ledger.tip_fetcher)

    assert ledger.cursors[f"address:{POOL_B}"].to_block == 89_999


async def test_an_empty_cursor_an_earlier_version_left_below_its_start_is_put_back():
    chain, store = MockChain(head=100_000), MockStore()
    await store.save_cursors([Cursor(stream=f"address:{POOL_B}", from_block=90_000, to_block=10_500)])
    kind = [{"name": "late", "abi": "UniswapV3Pool", "addresses": [{"address": POOL_B, "start_block": 90_000}]}]
    ledger = await build(kind, chain, store)

    cursor = ledger.cursor(next(iter(ledger.registry.streams())))

    assert (cursor.from_block, cursor.to_block) == (90_000, 89_999)


class GatedChain(MockChain):
    """A chain that holds back every eth_getLogs naming one address until released."""

    def __init__(self, head: int, held: str):
        super().__init__(head)
        self.held = held
        self.release = asyncio.Event()

    async def batch(self, calls):
        if any(method == "eth_getLogs" and self.held in (params[0].get("address") or []) for method, params in calls):
            await self.release.wait()
        return await super().batch(calls)


async def test_two_backfill_workers_never_fetch_the_same_stream_at_once():
    chain, store = GatedChain(head=1000, held=POOL_A), MockStore()
    swap(chain, POOL_A, 500)
    chain.add_log(POOL_B, 500, [topic("Swap(address,uint256,uint256,uint256,uint256,address)")])
    kinds = [
        {"name": "pool", "abi": "UniswapV3Pool", "events": ["Swap"], "start_block": 100, "addresses": [{"address": POOL_A}]},
        {"name": "pair", "abi": "AerodromePool", "events": ["Swap"], "start_block": 200, "addresses": [{"address": POOL_B}]},
    ]
    ledger = await build(kinds, chain, store)
    await ledger.tip_step()

    held = asyncio.create_task(ledger.backfill_step())
    while not ledger.in_flight:
        await asyncio.sleep(0)
    assert await ledger.backfill_step()

    assert ledger.in_flight == {f"address:{POOL_A}"}
    assert ledger.cursors[f"address:{POOL_B}"].to_block > 200
    chain.release.set()
    await held
    assert ledger.in_flight == set()
    assert ledger.cursors[f"address:{POOL_A}"].to_block > 100


async def test_address_streams_backfill_from_the_address_endpoint_and_topic_scans_from_the_backfill_one():
    nodes = {lane: MockChain(head=1000) for lane in ("tip", "backfill", "addresses")}
    for chain in nodes.values():
        pool_created(chain, 150, POOL_A)
        swap(chain, POOL_A, 200)
    config = chain_config(factory_kind(track="created", fetch="topics"))
    registry = Registry(config)
    store = MockStore()
    await store.record_contracts(registry.reconcile(await store.contracts()))
    ledger = Ledger(
        config, registry, store,
        tip_fetcher=LogFetcher(nodes["tip"], addresses_per_request=2, max_blocks=100),
        backfill_fetcher=LogFetcher(nodes["backfill"], addresses_per_request=2, max_blocks=100),
        address_fetcher=LogFetcher(nodes["addresses"], addresses_per_request=2, max_blocks=100),
        head_pool=nodes["tip"], metrics=Metrics("test", isolated=True),
    )
    await ledger.load()

    await settle(ledger)

    assert store.blocks_with_logs() == [150, 200]
    assert nodes["addresses"].get_logs_requests()
    assert all("address" in log_filter for log_filter in nodes["addresses"].get_logs_requests())
    assert all("address" not in log_filter for log_filter in nodes["backfill"].get_logs_requests())


class HeldWindowChain(MockChain):
    """A chain that holds back the eth_getLogs starting at one block until released, then answers or fails it once."""

    def __init__(self, head: int, held_from: int, *, fail: bool = False):
        super().__init__(head)
        self.held_from = held_from
        self.fail = fail
        self.release = asyncio.Event()

    async def batch(self, calls):
        if any(method == "eth_getLogs" and int(params[0]["fromBlock"], 16) == self.held_from for method, params in calls):
            await self.release.wait()
            self.held_from = -1
            if self.fail:
                raise RpcUnavailable("the node dropped the request")
        return await super().batch(calls)


AERODROME_SWAP = topic("Swap(address,address,uint256,uint256,uint256,uint256)")
SCANNED = [{"name": "scanned", "abi": "AerodromePool", "fetch": "topics", "start_block": 100, "events": ["Swap"]}]
PIPELINED = {"ledger": {"confirmations": 10, "poll_seconds": 0, "backfill_window_blocks": 200, "scan_windows_in_flight": 2}}


async def scanning_ledger(chain: MockChain) -> tuple[Ledger, MockStore, str]:
    store = MockStore()
    await store.record_contracts([ContractRow(address=POOL_A, contract="scanned", start_block=100, source="cli")])
    for block in (150, 450):
        chain.add_log(POOL_A, block, [AERODROME_SWAP, word(address(1)), word(address(2))])
    ledger = await build(SCANNED, chain, store, config_overrides=PIPELINED)
    await ledger.tip_step()
    return ledger, store, next(stream.key for stream in ledger.registry.streams() if stream.by_topic)


async def test_a_topic_scan_fetches_windows_together_and_its_cursor_joins_them_only_without_a_gap():
    chain = HeldWindowChain(head=1000, held_from=100)
    ledger, store, key = await scanning_ledger(chain)

    held = asyncio.create_task(ledger.backfill_step())
    while not ledger.claimed.get(key):
        await asyncio.sleep(0)
    await ledger.backfill_step()

    assert store.blocks_with_logs() == [450]
    assert ledger.cursors[key].to_block == 99
    chain.release.set()
    await held
    assert store.blocks_with_logs() == [150, 450]
    assert ledger.cursors[key].to_block == 499


async def test_a_failed_scan_window_is_fetched_again_before_the_cursor_passes_it(monkeypatch):
    monkeypatch.setattr(ledger_module, "STREAM_REST_SECONDS", 0)
    chain = HeldWindowChain(head=1000, held_from=100, fail=True)
    ledger, store, key = await scanning_ledger(chain)

    held = asyncio.create_task(ledger.backfill_step())
    while not ledger.claimed.get(key):
        await asyncio.sleep(0)
    await ledger.backfill_step()
    chain.release.set()
    await held
    assert ledger.cursors[key].to_block == 99

    await ledger.backfill_step()

    assert store.blocks_with_logs() == [150, 450]
    assert ledger.cursors[key].to_block == 499


async def test_an_address_listed_after_a_topic_scanned_past_follows_the_tip_despite_its_old_catch_up_limit():
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 150)
    swap(chain, POOL_A, 600)
    await store.save_cursors([Cursor(stream=f"address:{POOL_A}", from_block=100, to_block=99, until_block=99)])
    kind = [{"name": "pool", "abi": "UniswapV3Pool", "start_block": 100, "addresses": [{"address": POOL_A}]}]
    ledger = await build(kind, chain, store)

    await settle(ledger)

    assert store.blocks_with_logs() == [150, 600]
    assert ledger.cursors[f"address:{POOL_A}"].until_block is None


async def test_a_window_whose_hashes_stay_disputed_is_written_and_sent_to_the_audit(monkeypatch):
    monkeypatch.setattr(ledger_module, "HASH_RETRY_SECONDS", 0)
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 995)
    flagged: list[tuple[int, int]] = []
    ledger = await build(
        LISTED, chain, store,
        canonical_hash=lambda number: "0xstale" if number == 995 else None,
        on_unresolved_fork=lambda start, end: flagged.append((start, end)),
    )
    await settle(ledger)
    chain.head = 1010

    await settle(ledger)

    assert store.blocks_with_logs() == [995]
    assert [(start, end) for start, end in flagged if start <= 995 <= end]


async def test_a_stream_failing_at_the_tip_falls_to_the_backfill_lane_while_the_others_keep_up():
    chain, store = MockChainRefusingOneAddress(head=1000, refused="nobody"), MockStore()
    kind = [
        {"name": "pool", "abi": "UniswapV3Pool", "addresses": [{"address": POOL_A, "start_block": 100}]},
        {"name": "other", "abi": "UniswapV3Pool", "addresses": [{"address": POOL_B, "start_block": 100}]},
    ]
    ledger = await build(kind, chain, store)
    await settle(ledger)
    chain.refused = POOL_A
    swap(chain, POOL_A, 1005)
    swap(chain, POOL_B, 1005)
    chain.head = 1030

    await ledger.tip_step()
    await ledger.backfill_step()

    assert store.blocks_with_logs() == [1005]
    assert ledger.tip_block == 1020
    assert ledger.cursors[f"address:{POOL_A}"].to_block < ledger.tip_block
    assert f"address:{POOL_A}" in ledger.resting


class MockChainDown(MockChain):
    """A chain whose provider stops serving logs once ``down`` is set."""

    down = False

    async def batch(self, calls):
        if self.down and any(method == "eth_getLogs" for method, _params in calls):
            raise RpcUnavailable("the provider is down")
        return await super().batch(calls)


async def test_a_tip_window_no_stream_can_be_served_keeps_the_tip_where_it_was():
    chain, store = MockChainDown(head=1000), MockStore()
    ledger = await build(LISTED, chain, store)
    await settle(ledger)
    tip = ledger.tip_block
    chain.down = True
    chain.head = 1030

    with pytest.raises(RpcUnavailable):
        await ledger.tip_step()

    assert ledger.tip_block == tip


class MockStoreDown(MockStore):
    """A store whose ClickHouse stops taking inserts once ``down`` is set."""

    down = False

    async def insert_logs(self, logs, *, lane):
        if self.down:
            raise OperationalError("ClickHouse is down")
        return await super().insert_logs(logs, lane=lane)


async def test_a_tip_window_clickhouse_refuses_is_not_fetched_again_stream_by_stream():
    chain, store = MockChain(head=1000), MockStoreDown()
    ledger = await build(LISTED, chain, store)
    await settle(ledger)
    tip = ledger.tip_block
    swap(chain, POOL_A, 1005)
    swap(chain, POOL_B, 1005)
    chain.head = 1030
    store.down = True
    chain.requests.clear()

    with pytest.raises(OperationalError):
        await ledger.tip_step()

    # The window's one request had every stream's logs: going stream by stream would only ask for them again.
    assert [method for method, _params in chain.requests].count("eth_getLogs") == 1
    assert ledger.tip_block == tip
    assert not ledger.resting
    store.down = False
    await ledger.tip_step()
    assert store.blocks_with_logs() == [1005, 1005]
    assert all(ledger.cursors[f"address:{pool}"].to_block == ledger.tip_block for pool in (POOL_A, POOL_B))


async def test_a_backfill_window_clickhouse_refuses_is_retried_whole_not_split_and_rested():
    chain, store = MockChain(head=5000), MockStoreDown()
    pools = [address(0x10 + offset) for offset in range(6)]
    for pool in pools:
        swap(chain, pool, 150)
    ledger = await build([{"name": "pool", "abi": "UniswapV3Pool", "start_block": 100, "addresses": [{"address": pool} for pool in pools]}], chain, store)
    await ledger.tip_step()
    store.down = True
    chain.requests.clear()

    with pytest.raises(OperationalError):
        await ledger.backfill_step()

    # ClickHouse fails every stream alike: splitting the window would fetch each stream's logs again, alone, and resting
    # them would keep them off the lane for up to ten minutes after ClickHouse is back.
    group_requests = len(chain.get_logs_requests())
    assert not ledger.solo
    assert not ledger.resting
    assert not ledger.in_flight
    store.down = False
    chain.requests.clear()
    await ledger.backfill_step()
    assert len(chain.get_logs_requests()) == group_requests
    assert store.blocks_with_logs() == [150] * 6
    assert len({ledger.cursors[f"address:{pool}"].to_block for pool in pools}) == 1
