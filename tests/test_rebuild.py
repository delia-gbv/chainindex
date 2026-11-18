"""Against a real, local ClickHouse: ``chainindex rebuild`` moves a table to the current schema without losing a row."""

import os

import pytest

from chainindex.clickhouse import get_client
from chainindex.config import Settings
from chainindex.models import Log
from chainindex.rebuild import Rebuilder, StillWriting
from chainindex.store import LOG_COLUMNS, ChainStore

pytestmark = pytest.mark.clickhouse

DATABASE = "chain_rebuild_test"
# The logs table as chainindex created it before logs were sorted by topic and address.
BLOCK_SORTED_LOGS = """
    CREATE TABLE chain_rebuild_test.logs (
        block_number UInt64, block_hash String, block_time DateTime('UTC'), transaction_hash String,
        transaction_index UInt32, log_index UInt32, address LowCardinality(String), topic0 String, topic1 String,
        topic2 String, topic3 String, data String, inserted_at DateTime64(6, 'UTC'), lane LowCardinality(String),
        INDEX address_index address TYPE bloom_filter GRANULARITY 4,
        INDEX topic0_index topic0 TYPE bloom_filter GRANULARITY 4,
        INDEX inserted_index inserted_at TYPE minmax GRANULARITY 4
    ) ENGINE = MergeTree
    PARTITION BY toYYYYMM(block_time)
    ORDER BY (block_number, transaction_index, log_index)
"""
AUGUST = 1_786_000_000
SEPTEMBER = 1_789_000_000


class MockCountingRebuilder(Rebuilder):
    """A rebuilder that remembers which partitions it copied."""

    def __init__(self, store: ChainStore):
        super().__init__(store)
        self.copied: list[str] = []

    def copy(self, table: str, partition: str, columns: list[str]) -> int:
        self.copied.append(partition)
        return super().copy(table, partition, columns)


@pytest.fixture
def store() -> ChainStore:
    host = os.environ.get("CLICKHOUSE_HOST")
    if not host:
        pytest.skip("CLICKHOUSE_HOST not set")
    if host not in ("localhost", "127.0.0.1"):
        pytest.skip(f"These tests drop {DATABASE}, so they only run against a local ClickHouse")
    client = get_client(Settings(_env_file=None), database="default", autogenerate_session_id=False)
    client.command(f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
    client.command(f"CREATE DATABASE {DATABASE}")
    client.command(BLOCK_SORTED_LOGS)
    store = ChainStore(client, DATABASE)
    store.create()
    return store


def log(number: int, timestamp: int) -> Log:
    return Log(
        block_number=number, block_hash=f"0x{number:064x}", block_timestamp=timestamp, transaction_hash=f"0x{number:064x}",
        transaction_index=0, log_index=0, address=f"0x{number % 7:040x}", topics=(f"0x{number % 3:064x}",), data="0x",
    )


async def store_quietly(store: ChainStore, logs: list[Log]) -> None:
    """Stores logs as if the service had written them an hour ago and stopped."""
    await store.insert_logs(logs, lane="backfill")
    store.client.command(
        f"ALTER TABLE {DATABASE}.logs UPDATE inserted_at = inserted_at - INTERVAL 1 HOUR WHERE 1 SETTINGS mutations_sync = 2"
    )


def sorting_key(store: ChainStore, table: str) -> str:
    query = "SELECT sorting_key FROM system.tables WHERE database = {database:String} AND name = {table:String}"
    return store.client.query(query, parameters={"database": DATABASE, "table": table}).result_rows[0][0]


def tables(store: ChainStore) -> set[str]:
    query = "SELECT name FROM system.tables WHERE database = {database:String}"
    return {name for (name,) in store.client.query(query, parameters={"database": DATABASE}).result_rows}


async def test_a_table_with_an_old_sorting_key_is_rebuilt_with_every_row(store):
    logs = [log(number, AUGUST + number) for number in range(1000)] + [log(number, SEPTEMBER + number) for number in range(1000, 1500)]
    await store_quietly(store, logs)

    rebuilt = Rebuilder(store).run()

    assert rebuilt == ["logs"]
    assert sorting_key(store, "logs") == "topic0, address, block_number, transaction_index, log_index"
    assert len(await store.stored_logs(0, 10**6)) == 1500
    assert "logs__rebuild" not in tables(store)


async def test_a_rebuild_refuses_while_the_service_still_writes(store):
    await store.insert_logs([log(1, AUGUST)], lane="tip")

    with pytest.raises(StillWriting):
        Rebuilder(store).run()

    assert sorting_key(store, "logs") == "block_number, transaction_index, log_index"


async def test_a_rerun_copies_only_the_partitions_a_stopped_run_had_not_finished(store):
    await store_quietly(store, [log(number, AUGUST + number) for number in range(100)] + [log(number, SEPTEMBER + number) for number in range(100, 200)])
    first = Rebuilder(store)
    store.create_table("logs", f"{DATABASE}.logs__rebuild")
    first.copy("logs", "202608", LOG_COLUMNS)
    # Half of September landed before the run stopped.
    store.client.command(
        f"INSERT INTO {DATABASE}.logs__rebuild SELECT * FROM {DATABASE}.logs WHERE _partition_id = '202609' AND block_number < 150"
    )

    rerun = MockCountingRebuilder(store)
    rerun.run()

    assert rerun.copied == ["202609"]
    assert len(await store.stored_logs(0, 10**6)) == 200


async def test_a_rerun_after_the_exchange_only_drops_the_old_table(store):
    await store_quietly(store, [log(number, AUGUST + number) for number in range(100)])
    Rebuilder(store).run()
    # What an exchange leaves behind when the run stops before the drop: the old table under the staging name.
    store.client.command(BLOCK_SORTED_LOGS.replace(".logs (", ".logs__rebuild ("))

    rerun = MockCountingRebuilder(store)

    assert rerun.run() == []
    assert rerun.copied == []
    assert "logs__rebuild" not in tables(store)
    assert len(await store.stored_logs(0, 10**6)) == 100


async def test_tables_with_the_current_schema_are_left_alone(store):
    await store_quietly(store, [log(1, AUGUST)])
    Rebuilder(store).run()

    assert Rebuilder(store).run() == []
    assert not any(name.endswith(("__rebuild", "__schema")) for name in tables(store))
