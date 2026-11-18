"""The verifier checks every stored block's hash once, tip first then history, and repairs the forks it finds."""

import pytest

from chainindex.audit import Auditor
from chainindex.fetch import LogFetcher
from chainindex.metrics import Metrics
from chainindex.rpc import RpcUnavailable
from chainindex.verify import VERIFY_STREAM, Verifier, block_ranges

from .doubles import MockChain, MockStore
from .test_ledger import LISTED, POOL_A, POOL_B, build, settle, swap

VERIFY = {"verify": {"depth_seconds": 20, "window_blocks": 100, "history_pause_seconds": 0}}


async def build_verifier(chain: MockChain, store: MockStore) -> Verifier:
    ledger = await build(LISTED, chain, store, config_overrides=VERIFY)
    await settle(ledger)
    metrics = Metrics("test", isolated=True)
    auditor = Auditor(
        ledger.config, ledger.registry, store, ledger,
        fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100), pool=chain, metrics=metrics,
    )
    verifier = Verifier(ledger.config, store, ledger, auditor, pool=chain, metrics=metrics)
    await verifier.load()
    return verifier


async def verify_all(verifier: Verifier) -> None:
    for _round in range(100):
        if not await verifier.step():
            return
    raise AssertionError("The verifier never finished")


def headers_asked(chain: MockChain) -> list[int]:
    return [int(params[0], 16) for method, params in chain.requests if method == "eth_getBlockByNumber" and params[0] != "latest"]


async def test_a_fork_after_the_confirmations_is_recorded_and_its_canonical_logs_are_stored():
    chain, store = MockChain(head=1000), MockStore()
    for block in (150, 400, 700):
        swap(chain, POOL_A, block)
    verifier = await build_verifier(chain, store)
    stale = chain.block_hash(400)
    chain.fork(400, "0x" + "ab" * 32)

    await verify_all(verifier)

    assert store.orphan_rows == [(400, stale, "0x" + "ab" * 32)]
    assert {log.block_hash for log in store.logs.values() if log.block_number == 400} == {stale, "0x" + "ab" * 32}
    assert [row.kind for row in store.audit_rows] == ["verify"]


async def test_every_stored_block_is_checked_once_and_history_is_walked_down_to_the_oldest():
    chain, store = MockChain(head=1000), MockStore()
    blocks = [120, 150, 400, 650, 960]
    for block in blocks:
        swap(chain, POOL_B, block)
    verifier = await build_verifier(chain, store)

    await verify_all(verifier)
    swap(chain, POOL_B, 1050)
    chain.head = 1200
    await settle(verifier.ledger)
    await verify_all(verifier)

    assert sorted(headers_asked(chain)) == [*blocks, 1050]
    cursor = store.cursor_rows[VERIFY_STREAM]
    assert cursor.from_block == 100   # the streams' start
    assert cursor.to_block == verifier.ledger.tip_block - verifier.depth_blocks


async def test_blocks_younger_than_the_depth_are_not_checked_yet():
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 985)
    verifier = await build_verifier(chain, store)

    await verify_all(verifier)

    assert 985 not in headers_asked(chain)


class MockChainPruned(MockChain):
    """A chain whose node has dropped every header below a block."""

    def __init__(self, head: int, pruned_below: int):
        super().__init__(head)
        self.pruned_below = pruned_below

    async def batch(self, calls):
        answers = await super().batch(calls)
        return [
            None if method == "eth_getBlockByNumber" and params[0] != "latest" and int(params[0], 16) < self.pruned_below
            else answer
            for (method, params), answer in zip(calls, answers)
        ]


async def test_history_stops_where_the_node_has_no_headers_left():
    chain, store = MockChainPruned(head=1000, pruned_below=500), MockStore()
    for block in (150, 600, 900):
        swap(chain, POOL_A, block)
    verifier = await build_verifier(chain, store)

    await verify_all(verifier)

    assert verifier.history_floor is not None
    assert store.cursor_rows[VERIFY_STREAM].from_block > 150
    assert not store.orphan_rows


def test_block_numbers_group_into_runs_of_consecutive_blocks():
    assert block_ranges([7, 3, 4, 5, 9, 4]) == [(3, 5), (7, 7), (9, 9)]


async def test_a_stream_near_the_head_holds_the_verifier_until_it_writes_but_one_far_behind_does_not():
    chain, store = MockChain(head=1000), MockStore()
    verifier = await build_verifier(chain, store)
    tip = verifier.ledger.tip_block
    resting = verifier.ledger.cursors[f"address:{POOL_B}"]

    resting.to_block = tip - verifier.depth_blocks - 5
    held = verifier.target(tip)
    resting.to_block = tip - 3 * verifier.depth_blocks
    free = verifier.target(tip)

    assert held == tip - verifier.depth_blocks - 5
    assert free == tip - verifier.depth_blocks


async def test_a_repair_that_fails_is_tried_again_before_its_block_is_recorded():
    chain, store = MockChain(head=1000), MockStore()
    swap(chain, POOL_A, 400)
    verifier = await build_verifier(chain, store)
    chain.fork(400, "0x" + "cd" * 32)
    repair = verifier.auditor.audit

    async def failing(kind, first, last):
        raise RpcUnavailable("the provider is down")

    verifier.auditor.audit = failing
    with pytest.raises(RpcUnavailable):
        await verify_all(verifier)
    assert not store.orphan_rows
    verifier.auditor.audit = repair
    await verify_all(verifier)

    assert [number for number, _stored, _canonical in store.orphan_rows] == [400]
    assert "0x" + "cd" * 32 in {log.block_hash for log in store.logs.values()}


class MockChainFlaky(MockChain):
    """A chain whose header requests fail once, as a throttled provider's would."""

    failures = 1

    async def batch(self, calls):
        if self.failures and any(method == "eth_getBlockByNumber" and params[0] != "latest" for method, params in calls):
            self.failures -= 1
            raise RpcUnavailable("throttled")
        return await super().batch(calls)


async def test_a_passing_failure_does_not_stop_the_history_walk():
    chain, store = MockChainFlaky(head=1000), MockStore()
    swap(chain, POOL_A, 150)
    verifier = await build_verifier(chain, store)
    chain.failures = 0
    await verify_all(verifier)
    verifier.cursor.from_block = verifier.cursor.to_block + 1
    chain.failures = 1

    with pytest.raises(RpcUnavailable):
        await verify_all(verifier)
    await verify_all(verifier)

    assert verifier.history_floor is None
    assert 150 in headers_asked(chain)
