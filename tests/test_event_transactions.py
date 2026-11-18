"""The event transactions lane stores the transaction behind every wanted event of a kind, once per hash, behind its
streams."""

import pytest
import yaml
from pydantic import ValidationError

from chainindex import dbtgen
from chainindex.abi import topic
from chainindex.event_transactions import EventTransactions, cursor_key
from chainindex.fetch import LogFetcher
from chainindex.ledger import Ledger
from chainindex.metrics import Metrics
from chainindex.registry import Registry
from chainindex.rpc import RpcError

from .doubles import MockChain, MockStore, address, chain_config, word

POOL = address(0xA)
OTHER_POOL = address(0xB)
ROUTER = address(0x99)
TRADER = address(0x55)
SWAP = topic("Swap(address,address,int256,int256,uint160,uint128,int24)")
MINT = topic("Mint(address,address,int24,int24,uint128,uint256,uint256)")
POOLS = {
    "name": "pool", "abi": "UniswapV3Pool", "start_block": 100, "transactions": {"events": ["Swap"]},
    "addresses": [{"address": POOL}, {"address": OTHER_POOL}],
}
# Two kinds of pool found by the shared chain-wide scan, logging the same Swap: Uniswap v3 and a fork of it.
CREATED_POOLS = (
    {
        "name": "factory", "abi": "UniswapV3Factory", "addresses": [{"address": address(0xF), "start_block": 100}],
        "creates": {"event": "PoolCreated", "param": "pool", "contract": "pool"},
    },
    {
        "name": "fork_factory", "abi": "AerodromeCLFactory", "addresses": [{"address": address(0xE), "start_block": 100}],
        "creates": {"event": "PoolCreated", "param": "pool", "contract": "fork_pool"},
    },
    {"name": "pool", "abi": "UniswapV3Pool", "track": "created", "fetch": "topics", "transactions": {"events": ["Swap"]}},
    {"name": "fork_pool", "abi": "AerodromeCLPool", "track": "created", "fetch": "topics", "transactions": {"events": ["Swap"]}},
)


def swap(chain: MockChain, pool: str, block: int, transaction_hash: str) -> None:
    chain.add_log(pool, block, [SWAP, word(ROUTER), word(TRADER)], word(1), transaction_hash=transaction_hash)


async def build(chain: MockChain, store: MockStore, contracts: tuple[dict, ...] = (POOLS,)) -> EventTransactions:
    config = chain_config(list(contracts))
    registry = Registry(config)
    await store.record_contracts(registry.reconcile(await store.contracts()))
    metrics = Metrics("test", isolated=True)
    ledger = Ledger(
        config, registry, store,
        tip_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        backfill_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        head_pool=chain, metrics=metrics,
    )
    await ledger.load()
    for _round in range(100):
        tip_behind = await ledger.tip_step()
        backfilled = await ledger.backfill_step()
        if not tip_behind and not backfilled:
            break
    lane = EventTransactions(config, registry, store, ledger, pool=chain, metrics=metrics)
    await lane.load()
    return lane


async def run(lane: EventTransactions) -> None:
    while await lane.step():
        pass


async def test_every_swaps_transaction_is_stored_once_with_its_fees_and_a_mint_alone_is_not():
    chain = MockChain(head=400)
    routed = chain.add_transaction(TRADER, ROUTER, 150, priority=7)
    swap(chain, POOL, 150, routed)
    swap(chain, OTHER_POOL, 150, routed)
    legacy = chain.add_transaction(TRADER, POOL, 160, priority=None)
    swap(chain, POOL, 160, legacy)
    minted = chain.add_transaction(TRADER, ROUTER, 170)
    chain.add_log(POOL, 170, [MINT, word(ROUTER), word(TRADER)], word(1), transaction_hash=minted)
    store = MockStore()
    lane = await build(chain, store)

    await run(lane)

    rows = {row.transaction_hash: row for row in store.event_transaction_rows}
    assert set(rows) == {routed, legacy}
    assert (rows[routed].sender, rows[routed].recipient, rows[routed].max_priority_fee_per_gas, rows[routed].type) == (TRADER, ROUTER, 7, 2)
    assert (rows[legacy].max_priority_fee_per_gas, rows[legacy].max_fee_per_gas, rows[legacy].type, rows[legacy].gas_price) == (None, None, 0, 10)
    assert rows[routed].selector == "0x04e45aaf"
    assert rows[routed].block_time == 1_700_000_150
    # The transaction with two swap logs was asked for once.
    asked = [params[0] for method, params in chain.requests if method == "eth_getTransactionByHash"]
    assert sorted(asked) == sorted([routed, legacy])


async def test_the_lane_trails_the_kinds_streams_and_audits_every_window():
    chain = MockChain(head=400)
    store = MockStore()
    lane = await build(chain, store)

    await run(lane)

    assert lane.cursors[cursor_key(lane.kinds[0])].to_block == 390
    audits = [row for row in store.audit_rows if row.kind == "event_transactions"]
    assert audits and all("unknown=0" in row.detail for row in audits)


async def test_a_transaction_the_node_does_not_know_is_counted_and_the_lane_moves_on():
    chain = MockChain(head=400)
    swap(chain, POOL, 150, "0x" + "ee" * 32)
    store = MockStore()
    lane = await build(chain, store)

    await run(lane)

    assert store.event_transaction_rows == []
    audits = [row for row in store.audit_rows if row.kind == "event_transactions"]
    assert any(row.missing_logs == 1 and "unknown=1" in row.detail for row in audits)
    assert lane.cursors[cursor_key(lane.kinds[0])].to_block == 390


async def test_a_later_start_block_skips_the_history_before_it():
    chain = MockChain(head=400)
    early = chain.add_transaction(TRADER, ROUTER, 150)
    swap(chain, POOL, 150, early)
    late = chain.add_transaction(TRADER, ROUTER, 250)
    swap(chain, POOL, 250, late)
    store = MockStore()
    lane = await build(chain, store, ({**POOLS, "transactions": {"events": ["Swap"], "start_block": 200}},))

    await run(lane)

    assert [row.transaction_hash for row in store.event_transaction_rows] == [late]


async def test_created_kinds_sharing_an_event_store_every_scanned_swaps_transaction_once():
    chain = MockChain(head=400)
    swapped = [chain.add_transaction(TRADER, ROUTER, block) for block in (150, 160, 170)]
    swap(chain, POOL, 150, swapped[0])
    swap(chain, OTHER_POOL, 160, swapped[1])
    # A pool no factory here made: the scan stores its log, and dbt's factory check leaves it out of the views.
    swap(chain, address(0x5), 170, swapped[2])
    store = MockStore()
    lane = await build(chain, store, CREATED_POOLS)

    await run(lane)

    assert {row.transaction_hash for row in store.event_transaction_rows} == set(swapped)
    asked = [params[0] for method, params in chain.requests if method == "eth_getTransactionByHash"]
    assert sorted(asked) == sorted(swapped)
    assert {lane.cursors[cursor_key(kind)].to_block for kind in lane.kinds} == {390}


async def test_kinds_stepping_together_write_their_audits_and_cursors_in_one_insert_each():
    chain = MockChain(head=400)
    swap(chain, POOL, 150, chain.add_transaction(TRADER, ROUTER, 150))
    store = MockStore()
    lane = await build(chain, store, CREATED_POOLS)
    inserts = {"audits": 0, "cursors": 0}
    record_audits, save_cursors = store.record_audits, store.save_cursors

    async def counted_audits(audits):
        inserts["audits"] += 1
        await record_audits(audits)

    async def counted_cursors(cursors):
        inserts["cursors"] += 1
        await save_cursors(cursors)

    store.record_audits, store.save_cursors = counted_audits, counted_cursors

    assert not await lane.step()

    assert inserts == {"audits": 1, "cursors": 1}
    assert len([row for row in store.audit_rows if row.kind == "event_transactions"]) == len(lane.kinds) == 2
    assert {store.cursor_rows[cursor_key(kind)].to_block for kind in lane.kinds} == {390}


async def test_a_kind_that_fails_still_leaves_the_audits_and_cursors_of_the_kinds_before_it():
    chain = MockChain(head=400)
    swap(chain, POOL, 150, chain.add_transaction(TRADER, ROUTER, 150))
    store = MockStore()
    lane = await build(chain, store, CREATED_POOLS)
    insert = store.insert_event_transactions
    asked = []

    async def second_fails(rows):
        asked.append(rows)
        if len(asked) == 2:
            raise ConnectionError("ClickHouse went away")
        return await insert(rows)

    store.insert_event_transactions = second_fails

    with pytest.raises(ConnectionError):
        await lane.step()

    first, second = lane.kinds
    assert [row.detail.split()[0] for row in store.audit_rows if row.kind == "event_transactions"] == [f"contract={first.name}"]
    assert store.cursor_rows[cursor_key(first)].to_block == 390
    assert cursor_key(second) not in store.cursor_rows


async def test_kinds_sharing_a_window_and_an_event_read_its_logs_and_stored_transactions_once():
    chain = MockChain(head=400)
    shared = chain.add_transaction(TRADER, ROUTER, 150)
    swap(chain, POOL, 150, shared)
    store = MockStore()
    lane = await build(chain, store, CREATED_POOLS)
    reads = {"logs": 0, "stored": 0}
    event_log_transactions, stored_event_transactions = store.event_log_transactions, store.stored_event_transactions

    async def counted_logs(*args):
        reads["logs"] += 1
        return await event_log_transactions(*args)

    async def counted_stored(*args):
        reads["stored"] += 1
        return await stored_event_transactions(*args)

    store.event_log_transactions, store.stored_event_transactions = counted_logs, counted_stored

    assert not await lane.step()

    assert reads == {"logs": 1, "stored": 1}
    assert [row.transaction_hash for row in store.event_transaction_rows] == [shared]
    assert [params[0] for method, params in chain.requests if method == "eth_getTransactionByHash"] == [shared]
    first, second = (row for row in store.audit_rows if row.kind == "event_transactions")
    assert (first.stored_logs, first.repaired_logs) == (0, 1)
    assert (second.stored_logs, second.repaired_logs) == (1, 0)


async def test_a_window_of_several_batches_keeps_every_answer():
    chain = MockChain(head=400)
    swapped = [chain.add_transaction(TRADER, ROUTER, 150) for _ in range(250)]
    for transaction_hash in swapped:
        swap(chain, POOL, 150, transaction_hash)
    store = MockStore()
    lane = await build(chain, store)

    await run(lane)

    assert sorted(row.transaction_hash for row in store.event_transaction_rows) == sorted(swapped)


async def test_a_restart_fetches_only_what_is_not_stored_yet():
    chain = MockChain(head=400)
    first = chain.add_transaction(TRADER, ROUTER, 150)
    swap(chain, POOL, 150, first)
    store = MockStore()
    lane = await build(chain, store)
    await run(lane)
    second = chain.add_transaction(TRADER, ROUTER, 160)
    # A log the auditor repaired after the lane had passed its block, say.
    await store.insert_logs([chain.add_log(POOL, 160, [SWAP, word(ROUTER), word(TRADER)], word(1), transaction_hash=second)], lane="audit")
    chain.requests.clear()
    lane.cursors.clear()

    await run(lane)

    asked = [params[0] for method, params in chain.requests if method == "eth_getTransactionByHash"]
    assert asked == [second]
    assert {row.transaction_hash for row in store.event_transaction_rows} == {first, second}


async def test_a_node_error_is_raised_so_the_window_is_retried():
    chain = MockChain(head=400)
    swap(chain, POOL, 150, chain.add_transaction(TRADER, ROUTER, 150))
    store = MockStore()
    lane = await build(chain, store)
    chain.batch = lambda calls: _errors(calls)

    with pytest.raises(RpcError):
        await lane.step()


async def _errors(calls):
    return [RpcError(-32000, "busy", method) for method, _params in calls]


@pytest.mark.parametrize(
    "contract, complaint",
    [
        ({**POOLS, "transactions": {"events": ["Sweep"]}}, "must be among the kind's indexed events"),
        ({**POOLS, "transactions": {"events": []}}, "must be among the kind's indexed events"),
        (
            {"name": "pool", "abi": "UniswapV3Pool", "track": "any", "fetch": "topics", "start_block": 1, "transactions": {"events": ["Swap"]}},
            "need a registry or factories",
        ),
    ],
)
def test_transactions_of_an_unindexed_event_or_an_unregistered_kind_are_refused(contract, complaint):
    with pytest.raises(ValidationError, match=complaint):
        chain_config([contract])


def test_the_cursor_is_named_after_the_events_so_a_changed_list_starts_over():
    swaps = chain_config([POOLS]).contract("pool")
    swaps_and_mints = chain_config([{**POOLS, "transactions": {"events": ["Swap", "Mint"]}}]).contract("pool")

    assert cursor_key(swaps).startswith("event_transactions:pool:")
    assert cursor_key(swaps) != cursor_key(swaps_and_mints)


def test_dbt_gets_the_raw_transactions_and_each_wanted_event_joined_to_them():
    files = dbtgen.render(chain_config([POOLS]))

    assert "testnet__event_transactions.sql" in files
    joined = files["testnet__pool__swap_transactions.sql"]
    assert "left join {{ ref('testnet__event_transactions') }}" in joined
    assert "tx.max_priority_fee_per_gas as tx_max_priority_fee_per_gas" in joined
    assert "testnet__pool__mint_transactions.sql" not in files


def test_only_the_transactions_carry_the_grain_test_so_dbt_never_tests_a_joined_view():
    models_yaml = dbtgen.render(chain_config([POOLS]))["_testnet__models.yml"]
    models = {model["name"]: model for model in yaml.safe_load(models_yaml)["models"]}

    assert models["testnet__event_transactions"]["data_tests"] == [{"unique_grain": {"arguments": {"columns": ["transaction_hash"]}}}]
    assert "data_tests" not in models["testnet__pool__swap_transactions"]


def test_a_deployment_can_turn_the_lane_off():
    from chainindex.config import Settings

    config = Settings(CHAIN="base", EVENT_TRANSACTIONS_ENABLED=False, _env_file=None).chain_config()

    assert all(contract.transactions is None for contract in config.contracts)
