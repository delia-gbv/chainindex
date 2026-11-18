"""Against a real, local ClickHouse: the store writes once, and dbt decodes real logs exactly as Python does."""

import json
import os
import subprocess
from datetime import UTC
from pathlib import Path

import pytest
from eth_abi import encode

from chainindex.clickhouse import get_client
from chainindex.config import ReadSpec, Settings, load_chain
from chainindex.dbtgen import read_model
from chainindex.models import Header, Log
from chainindex.registry import Registry
from chainindex.store import CallRow, ChainStore, ContractRow, Cursor, TransactionRow

pytestmark = pytest.mark.clickhouse

ROOT = Path(__file__).parents[1]
FIXTURE = Path(__file__).parent / "fixtures" / "base_logs.json"
FACTORIES = {"0xf8f2eb4940cfe7d13603dddd87f123820fc061ef": "aerodrome_cl_factory", "0x420dd381b31aef6683db6b902084cb0ffece40da": "aerodrome_pool_factory"}
BASIC_POOLS = {"0xcdac0d6c6c59727a65f871236188350531885c43"}


@pytest.fixture(scope="module")
def settings() -> Settings:
    host = os.environ.get("CLICKHOUSE_HOST")
    if not host:
        pytest.skip("CLICKHOUSE_HOST not set")
    if host not in ("localhost", "127.0.0.1"):
        pytest.skip("These tests drop chain_base and events_base, so they only run against a local ClickHouse")
    return Settings(_env_file=None)


@pytest.fixture(scope="module")
def store(settings: Settings) -> ChainStore:
    client = get_client(settings, database="default", autogenerate_session_id=False)
    for database in ("chain_base", "events_base"):
        client.command(f"DROP DATABASE IF EXISTS {database}")
    store = ChainStore(client, "chain_base")
    store.create()
    store.create()
    return store


def fixture_logs() -> list[Log]:
    return [Log.from_rpc(item) for item in json.loads(FIXTURE.read_text())]


def kind_of(address: str) -> str:
    if address in FACTORIES:
        return FACTORIES[address]
    return "aerodrome_pool" if address in BASIC_POOLS else "aerodrome_cl_pool"


async def test_logs_are_written_once_however_often_they_are_inserted(store):
    logs = fixture_logs()
    contracts = [ContractRow(address=address, contract=kind_of(address), start_block=1) for address in {log.address for log in logs}]
    await store.record_contracts(contracts)

    first = await store.insert_logs(logs[:20], lane="tip")
    second = await store.insert_logs(logs, lane="backfill")

    assert first == (20, 0)
    assert second == (len(logs) - 20, 20)
    assert len(await store.stored_logs(0, 1 << 62)) == len(logs)


async def test_the_latest_cursor_and_registry_row_win(store):
    await store.save_cursors([Cursor(stream="address:0x1", from_block=1, to_block=10)])
    await store.save_cursors([Cursor(stream="address:0x1", from_block=1, to_block=20, until_block=30)])
    first = ContractRow(address="0x" + "11" * 20, contract="aerodrome_pool", start_block=5)
    await store.record_contracts([first, first.model_copy(update={"active": False})])

    cursors = await store.cursors()
    rows = {row.address: row for row in await store.contracts()}

    assert cursors["address:0x1"] == Cursor(stream="address:0x1", from_block=1, to_block=20, until_block=30)
    assert rows[first.address].active is False


def dbt_build(settings: Settings) -> None:
    environment = {
        **os.environ, "CLICKHOUSE_HOST": settings.CLICKHOUSE_HOST,
        "CLICKHOUSE_PORT": str(settings.CLICKHOUSE_PORT), "CLICKHOUSE_PASSWORD": settings.CLICKHOUSE_PASSWORD,
    }
    # The test's own ClickHouse builds four of Base's hundred-odd views at once, where one at a time took half a minute.
    result = subprocess.run(
        ["uv", "run", "dbt", "build", "--profiles-dir", "profiles", "--threads", "4", "--select", "tag:chain_base"],
        cwd=ROOT / "dbt", env=environment, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout[-3000:]


def decoded_rows(store: ChainStore, table: str) -> dict[tuple[int, int], dict]:
    result = store.client.query(f"SELECT * FROM events_base.{table}")
    return {
        (row["block_number"], row["log_index"]): row
        for row in (dict(zip(result.column_names, values)) for values in result.result_rows)
    }


def comparable(value):
    if isinstance(value, str):
        return value.lower()
    return value


async def test_dbt_decodes_every_real_log_exactly_like_the_python_decoder(settings, store):
    dbt_build(settings)
    chain = load_chain("base")
    decoder = Registry(chain).decoder
    logs = fixture_logs()

    checked = 0
    for log in logs:
        decoded = decoder.decode(kind_of(log.address), log)
        assert decoded is not None
        table = next(
            f"{decoded.contract}__{event.table}" for event in chain.contract(decoded.contract).event_specs()
            if event.name == decoded.event
        )
        row = decoded_rows(store, table)[(log.block_number, log.log_index)]
        assert {name: comparable(row[name]) for name in decoded.args} == {
            name: comparable(value) for name, value in decoded.args.items()
        }, table
        # The driver hands back DateTime('UTC') values as naive UTC datetimes.
        assert row["block_time"].replace(tzinfo=UTC).timestamp() == log.block_timestamp
        checked += 1

    assert checked == len(logs)


async def test_a_log_stored_after_dbt_ran_is_decoded_without_another_run(settings, store):
    dbt_build(settings)
    before = decoded_rows(store, "aerodrome_cl_pool__swap")
    swap = next(log for log in fixture_logs() if kind_of(log.address) == "aerodrome_cl_pool" and log.topic0.startswith("0xc42079"))
    late = swap.model_copy(update={"block_number": swap.block_number + 1_000_000, "block_hash": "0x" + "ab" * 32})
    await store.insert_logs([late], lane="backfill")

    after = decoded_rows(store, "aerodrome_cl_pool__swap")

    assert set(after) - set(before) == {(late.block_number, late.log_index)}


async def test_orphaned_blocks_are_left_out_of_decoded_models(settings, store):
    swap = next(log for log in fixture_logs() if log.topic0.startswith("0xc42079"))
    forked = swap.model_copy(update={"block_number": swap.block_number + 2_000_000, "block_hash": "0x" + "cd" * 32})
    await store.insert_logs([forked], lane="tip")
    await store.record_orphans([(forked.block_number, forked.block_hash, "0x" + "ef" * 32)], detected_by="test")

    dbt_build(settings)

    assert (forked.block_number, forked.log_index) not in decoded_rows(store, "aerodrome_cl_pool__swap")


# No chain file reads pool state any more (the pool replay has it), so the reads' decoding is tested on these.
READS = [
    ReadSpec(
        name="cl_pool_state", contract="aerodrome_cl_pool", every_blocks=1,
        calls=["slot0() returns (uint160 sqrt_price_x96, int24 tick)", "liquidity() returns (uint128 liquidity)",
               "stakedLiquidity() returns (uint128 staked_liquidity)", "fee() returns (uint24 fee)"],
    ),
    ReadSpec(name="cl_pool_ticks", contract="aerodrome_cl_pool", every_blocks=15, tick_map={"spacings": 10}),
]


def create_read_views(store: ChainStore) -> None:
    """Creates the decoded read views dbt would build for READS, reading chain_base's tables directly."""
    chain = load_chain("base").model_copy(update={"reads": READS})
    store.client.command("CREATE DATABASE IF NOT EXISTS events_base")
    for read in READS:
        sql = read_model(chain, read)
        body = sql[sql.index(") }}") + 4 :]
        for table in ("calls", "orphaned_blocks"):
            body = body.replace(f"{{{{ source('chain_base', '{table}') }}}}", f"chain_base.{table}")
        store.client.command(f"CREATE OR REPLACE VIEW events_base.read__{read.name} AS {body}")


def call_row(read: str, block: int, address: str, function: str, data: str, output: bytes | None) -> CallRow:
    return CallRow(
        read=read, block_number=block, block_hash="0x" + format(block, "064x"), block_time=1_790_000_000 + block,
        address=address, function=function, input=data, success=output is not None,
        output="0x" + (output or b"").hex(),
    )


async def test_a_decoded_read_has_signed_values_and_nulls_for_failed_calls(store):
    pool = "0x" + "a1" * 20
    state = [
        call_row("cl_pool_state", 900, pool, "slot0", "0x3850c7bd", encode(["uint160", "int24", "uint16"], [2**96 + 5, -887, 1])),
        call_row("cl_pool_state", 900, pool, "liquidity", "0x1a686502", encode(["uint128"], [12345])),
        call_row("cl_pool_state", 900, pool, "stakedLiquidity", "0x3ab04b20", None),
        call_row("cl_pool_state", 900, pool, "fee", "0xddca3f43", encode(["uint24"], [500])),
    ]
    tick_input = "0xf30dba93" + format(-900 % (1 << 256), "064x")
    empty_input = "0xf30dba93" + format(100, "064x")
    ticks = [
        call_row("cl_pool_ticks", 900, pool, "ticks", tick_input, encode(["uint128", "int128", "int128"], [77, -55, -11])),
        call_row("cl_pool_ticks", 900, pool, "ticks", empty_input, encode(["uint128", "int128", "int128"], [0, 0, 0])),
    ]
    await store.insert_calls(state + ticks)
    assert await store.insert_calls(state) == 0

    create_read_views(store)
    state_rows = store.client.query(
        "SELECT sqrt_price_x96, tick, liquidity, staked_liquidity, fee FROM events_base.read__cl_pool_state WHERE block_number = 900"
    ).result_rows
    tick_rows = store.client.query(
        "SELECT tick, liquidity_gross, liquidity_net, staked_liquidity_net FROM events_base.read__cl_pool_ticks "
        "WHERE block_number = 900 ORDER BY tick"
    ).result_rows

    assert state_rows == [(2**96 + 5, -887, 12345, None, 500)]
    # A cleared tick is a change too: its zero ends the value it had.
    assert tick_rows == [(-900, 77, -55, -11), (100, 0, 0, 0)]


async def test_a_read_has_a_row_per_block_where_a_call_changed_and_nulls_for_the_calls_that_did_not(store):
    pool = "0x" + "a2" * 20
    await store.insert_calls(
        [
            call_row("cl_pool_state", 950, pool, "slot0", "0x3850c7bd", encode(["uint160", "int24"], [2**96, 7])),
            call_row("cl_pool_state", 950, pool, "liquidity", "0x1a686502", encode(["uint128"], [100])),
            call_row("cl_pool_state", 960, pool, "liquidity", "0x1a686502", encode(["uint128"], [200])),
            call_row("cl_pool_state", 970, pool, "slot0", "0x3850c7bd", None),
        ]
    )

    create_read_views(store)
    rows = store.client.query(
        "SELECT block_number, tick, liquidity FROM events_base.read__cl_pool_state WHERE address = {pool:String} ORDER BY block_number",
        parameters={"pool": pool},
    ).result_rows

    assert rows == [(950, 7, 100), (960, None, 200), (970, None, None)]


def synthetic_log(number: int) -> Log:
    """A log from its own address, in a block far past the fixture's."""
    return Log(
        block_number=10**9 + number // 10, block_hash=f"0x{number // 10:064x}", block_timestamp=1,
        transaction_hash=f"0x{number:064x}", transaction_index=0, log_index=number % 10,
        address=f"0x{number + 1:040x}", topics=(f"0x{7:064x}",), data="0x",
    )


async def test_a_window_with_more_addresses_than_one_query_parameter_holds_is_still_written_once(store):
    logs = [synthetic_log(number) for number in range(5000)]

    first = await store.insert_logs(logs, lane="backfill")
    second = await store.insert_logs(logs, lane="backfill")

    assert first == (5000, 0)
    assert second == (0, 5000)


async def test_block_times_come_from_stored_headers_then_stored_logs_for_more_blocks_than_one_parameter_holds(store):
    headers = [Header(number=number, hash=f"0x{number:064x}", parent_hash="0x", timestamp=number + 1) for number in range(2 * 10**8, 2 * 10**8 + 6000)]
    await store.insert_blocks(headers)
    stamped = synthetic_log(7).model_copy(update={"block_number": 2 * 10**8 + 7000, "block_timestamp": 1_800_000_000})
    unstamped = synthetic_log(8).model_copy(update={"block_number": 2 * 10**8 + 7001, "block_timestamp": 0})
    await store.insert_logs([stamped, unstamped], lane="backfill")

    found = await store.block_times([header.number for header in headers] + [stamped.block_number, unstamped.block_number])

    assert len(found) == 6001
    assert found[headers[-1].number] == headers[-1].timestamp
    assert found[stamped.block_number] == 1_800_000_000


async def test_an_accounts_transactions_are_written_once_and_its_logs_and_unwraps_found(store):
    wallet = "0x" + "77" * 20
    word = "0x" + "0" * 24 + wallet[2:]
    unwrap = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
    logs = [
        Log(
            block_number=900, block_hash="0x" + "09" * 32, block_timestamp=1_800_000_000, transaction_hash="0x" + "aa" * 32,
            transaction_index=0, log_index=0, address="0x" + "0e" * 20, topics=(unwrap, word), data="0x" + format(25, "064x"),
        ),
        Log(
            block_number=901, block_hash="0x" + "0a" * 32, block_timestamp=1_800_000_002, transaction_hash="0x" + "bb" * 32,
            transaction_index=0, log_index=0, address="0x" + "0c" * 20, topics=("0x" + "dd" * 32, "0x" + "00" * 32, word), data="0x",
        ),
    ]
    await store.insert_logs(logs, lane="test")
    row = TransactionRow(
        account=wallet, block_number=900, block_hash="0x" + "09" * 32, block_time=1_800_000_000, transaction_hash="0x" + "aa" * 32,
        transaction_index=0, sender=wallet, recipient="0x" + "0e" * 20, nonce=3, value=0, status=1, gas_used=50_000,
        effective_gas_price=10**7, l1_fee=2 * 10**9, fee=50_000 * 10**7 + 2 * 10**9, found_by="logs",
        raw_transaction="{}", raw_receipt="{}",
    )

    first = await store.insert_transactions([row])
    again = await store.insert_transactions([row])

    assert (first, again) == (1, 0)
    assert await store.account_log_transactions(word, 900, 901) == ["0x" + "aa" * 32, "0x" + "bb" * 32]
    assert await store.account_unwraps(word, unwrap, 900, 901) == [(900, 25)]
    assert await store.stored_transactions(wallet, 0, 10**9) == {"0x" + "aa" * 32}


async def test_the_verifier_sees_the_block_hashes_of_logs_and_calls_together(store):
    first = 3 * 10**8
    log = synthetic_log(9).model_copy(update={"block_number": first, "block_hash": "0x" + "c1" * 32})
    await store.insert_logs([log], lane="backfill")
    await store.insert_calls([call_row("verify_probe", first + 1, "0x" + "c2" * 20, "fee", "0xddca3f43", encode(["uint24"], [5]))])

    hashes = await store.stored_block_hashes(first, first + 1)

    assert hashes == {(first, "0x" + "c1" * 32), (first + 1, "0x" + format(first + 1, "064x"))}


async def test_a_header_recorded_twice_is_stored_once(store):
    header = Header(number=4 * 10**8, hash="0x" + "d1" * 32, parent_hash="0x", timestamp=1)

    assert await store.insert_blocks([header]) == 1
    assert await store.insert_blocks([header]) == 0
    assert store.client.query(f"SELECT count() FROM chain_base.blocks WHERE block_number = {header.number}").result_rows == [(1,)]


def test_each_migration_is_applied_once_and_an_edited_one_stops_the_start(store, monkeypatch):
    from chainindex import store as store_module

    assert store.migrate() == []
    names = store.client.query("SELECT name FROM chain_base.schema_migrations").result_rows
    assert names == [(name,) for name, _sql in store_module.MIGRATIONS]

    name, sql = store_module.MIGRATIONS[0]
    monkeypatch.setattr(store_module, "MIGRATIONS", [(name, sql + " ")])
    with pytest.raises(RuntimeError, match="other SQL"):
        store.migrate()


async def test_the_api_serves_decoded_events_from_the_dbt_views(settings, store):
    from aiohttp.test_utils import TestClient, TestServer

    from chainindex.api import ApiSettings, ChainIndexApi

    logs = fixture_logs()
    await store.record_contracts([ContractRow(address=address, contract=kind_of(address), start_block=1) for address in {log.address for log in logs}])
    await store.insert_logs(logs, lane="backfill")
    dbt_build(settings)
    swap = next(log for log in logs if kind_of(log.address) == "aerodrome_cl_pool" and len(log.topics) == 3)
    api = ChainIndexApi(ApiSettings(_env_file=None, API_CHAINS="base"), client=store.client)
    client = TestClient(TestServer(api.app()))
    await client.start_server()
    try:
        response = await client.get(
            f"/v1/base/events/aerodrome_cl_pool/Swap?address={swap.address}&to_block={swap.block_number}&limit=1000"
        )
        page = await response.json()
    finally:
        await client.close()

    assert response.status == 200
    rows = [dict(zip(page["columns"], row)) for row in page["rows"]]
    assert (swap.block_number, swap.log_index) in {(row["block_number"], row["log_index"]) for row in rows}
    assert all(row["address"] == swap.address for row in rows)
