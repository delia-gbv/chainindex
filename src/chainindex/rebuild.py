"""``chainindex rebuild``: brings a chain database's raw tables to the code's current schema, partition by partition.

A changed sorting key cannot be altered in place. A table whose schema differs from the code's is copied into
``<table>__rebuild``, created with the current DDL, one partition per INSERT ... SELECT with its row count checked;
then the two are exchanged and the old one dropped. It runs while the chain's service is stopped. A rerun skips the
partitions already copied, and finishes a rebuild that stopped after the exchange.
"""

import time
from typing import Any

from chainindex.logger import get_logger
from chainindex.store import TABLES, ChainStore

logger = get_logger(__name__)

STAGING = "__rebuild"
PROBE = "__schema"
# The service writes cursors every few seconds and logs at every confirmed window, so two quiet minutes mean it is off.
QUIET_SECONDS = 120
# A month of a busy chain's logs is ~200M rows (~135 GiB uncompressed). The copy streams it in insert blocks of at most
# 256 MiB, each sorted to the new key and compressed by one of two insert threads, so memory stays near a gigabyte.
COPY_SETTINGS: dict[str, Any] = {
    "max_threads": 4,
    "max_insert_threads": 2,
    "min_insert_block_size_rows": 262144,
    "min_insert_block_size_bytes": 256 << 20,
    "max_execution_time": 0,
    # Two identical insert blocks are both real rows here, never a retried insert.
    "insert_deduplicate": 0,
}


class StillWriting(Exception):
    """The chain's service wrote to the database recently, so it may still be running."""


class Rebuilder:
    """Rebuilds the raw tables of one chain database whose schema the code has changed."""

    def __init__(self, store: ChainStore):
        self.store = store
        self.client = store.client
        self.database = store.database

    def _rows(self, sql: str, parameters: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        return list(self.client.query(sql, parameters=parameters or {}).result_rows)

    def exists(self, table: str) -> bool:
        """Tells whether a table exists in the chain database."""
        rows = self._rows(
            "SELECT count() FROM system.tables WHERE database = {database:String} AND name = {table:String}",
            {"database": self.database, "table": table},
        )
        return rows[0][0] > 0

    def schema(self, table: str) -> str:
        """Returns a table's DDL as ClickHouse prints it, with its name taken out so two tables compare."""
        rows = self._rows(
            "SELECT create_table_query FROM system.tables WHERE database = {database:String} AND name = {table:String}",
            {"database": self.database, "table": table},
        )
        return str(rows[0][0]).replace(f"{self.database}.{table} ", "{table} ", 1)

    def current_schema(self, table: str) -> str:
        """Returns the DDL the code creates a table with, read off an empty probe table."""
        probe = f"{table}{PROBE}"
        self.client.command(f"DROP TABLE IF EXISTS {self.database}.{probe} SYNC")
        self.store.create_table(table, f"{self.database}.{probe}")
        try:
            return self.schema(probe)
        finally:
            self.client.command(f"DROP TABLE IF EXISTS {self.database}.{probe} SYNC")

    def partition_rows(self, table: str) -> dict[str, int]:
        """Returns the rows of every partition of a table."""
        rows = self._rows(
            "SELECT partition_id, sum(rows) FROM system.parts WHERE active AND database = {database:String} "
            "AND table = {table:String} GROUP BY partition_id",
            {"database": self.database, "table": table},
        )
        return {str(partition): int(count) for partition, count in rows}

    def check_quiet(self, table: str) -> None:
        """Refuses to go on while the service writes: a recent insert into the table or a recent cursor update."""
        for checked, column in ((table, "updated_at" if table == "cursors" else "inserted_at"), ("cursors", "updated_at")):
            # The inserted_at minmax index keeps this to the newest granules.
            rows = self._rows(
                f"SELECT count() FROM {self.database}.{checked} "
                f"WHERE {column} > now() - toIntervalSecond({QUIET_SECONDS})"
            )
            if rows[0][0]:
                raise StillWriting(
                    f"{self.database}.{checked} was written in the last {QUIET_SECONDS} s: scale the chain's service to 0 "
                    "and wait before rebuilding"
                )

    def drop(self, table: str) -> None:
        """Drops a table one partition at a time, since ClickHouse refuses to drop more than 50 GB at once."""
        for partition in self.partition_rows(table):
            self.client.command(f"ALTER TABLE {self.database}.{table} DROP PARTITION ID '{partition}'")
        self.client.command(f"DROP TABLE IF EXISTS {self.database}.{table} SYNC")

    def copy(self, table: str, partition: str, columns: list[str]) -> int:
        """Copies one partition into the staging table and returns how many rows it holds there."""
        listed = ", ".join(columns)
        started = time.monotonic()
        self.client.command(
            f"INSERT INTO {self.database}.{table}{STAGING} ({listed}) SELECT {listed} FROM {self.database}.{table} "
            "WHERE _partition_id = {partition:String}",
            parameters={"partition": partition},
            settings=COPY_SETTINGS,
        )
        copied = self.partition_rows(f"{table}{STAGING}").get(partition, 0)
        logger.info(f"{table}: partition {partition} copied, {copied} rows in {time.monotonic() - started:.0f} s")
        return copied

    def rebuild(self, table: str) -> bool:
        """Brings one table to the code's schema and returns whether it had to be rebuilt."""
        staging = f"{table}{STAGING}"
        if not self.exists(table):
            logger.info(f"{table}: missing, `chainindex init` creates it")
            return False
        wanted = self.current_schema(table)
        if self.schema(table) == wanted:
            if self.exists(staging):
                # The old table of a rebuild that stopped after the exchange, or a copy nobody needs any more.
                logger.info(f"{table}: current; dropping the leftover {staging}")
                self.drop(staging)
            return False
        self.check_quiet(table)
        if self.exists(staging) and self.schema(staging) != wanted:
            logger.info(f"{table}: {staging} has an older schema; starting the copy over")
            self.drop(staging)
        self.store.create_table(table, f"{self.database}.{staging}")
        columns = [
            str(name) for (name,) in self._rows(
                "SELECT name FROM system.columns WHERE database = {database:String} AND table = {table:String} "
                "AND name IN (SELECT name FROM system.columns WHERE database = {database:String} AND table = {old:String}) "
                "ORDER BY position",
                {"database": self.database, "table": staging, "old": table},
            )
        ]
        source = self.partition_rows(table)
        copied = self.partition_rows(staging)
        for partition, rows in sorted(source.items()):
            if copied.get(partition) == rows:
                logger.info(f"{table}: partition {partition} already copied ({rows} rows)")
                continue
            if partition in copied:
                self.client.command(f"ALTER TABLE {self.database}.{staging} DROP PARTITION ID '{partition}'")
            landed = self.copy(table, partition, columns)
            if landed != rows:
                raise RuntimeError(f"{table}: partition {partition} has {rows} rows but {landed} were copied")
        extra = sorted(set(self.partition_rows(staging)) - set(source))
        if extra:
            raise RuntimeError(f"{table}: {staging} holds partitions {extra} the table does not have")
        self.client.command(f"EXCHANGE TABLES {self.database}.{table} AND {self.database}.{staging}")
        logger.info(f"{table}: exchanged with its rebuilt copy; dropping the old table")
        self.drop(staging)
        return True

    def run(self) -> list[str]:
        """Rebuilds every raw table whose schema differs from the code's and returns their names."""
        return [table for table in TABLES if self.rebuild(table)]
