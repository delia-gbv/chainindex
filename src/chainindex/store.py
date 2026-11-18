"""The chain's ClickHouse database: insert-only raw tables, the registry, and per-stream cursors."""

import asyncio
import hashlib
import json
import time
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel

from chainindex.clickhouse import safe_database
from chainindex.logger import get_logger
from chainindex.models import Header, Log

logger = get_logger(__name__)

# Raw tables are MergeTree and only ever inserted into. Only ``cursors`` replaces rows: it is operational state.
# ``logs`` is sorted by topic and address, since dbt decodes it on every read, one event of one kind at a time. Block
# windows go through the block_number minmax index, and address-only filters (kinds without an ABI) through the bloom
# filter. Hex hashes and data take ZSTD, about half of LZ4's size once a block's logs no longer sit together (measured
# on a busy chain's logs). ``chainindex rebuild`` moves an older table over.
TABLES: dict[str, str] = {
    "logs": """
        CREATE TABLE IF NOT EXISTS {table} (
            block_number UInt64,
            block_hash String CODEC(ZSTD(1)),
            block_time DateTime('UTC'),
            transaction_hash String CODEC(ZSTD(1)),
            transaction_index UInt32,
            log_index UInt32,
            address LowCardinality(String),
            topic0 String,
            topic1 String CODEC(ZSTD(1)),
            topic2 String CODEC(ZSTD(1)),
            topic3 String CODEC(ZSTD(1)),
            data String CODEC(ZSTD(1)),
            inserted_at DateTime64(6, 'UTC'),
            lane LowCardinality(String),
            INDEX block_index block_number TYPE minmax GRANULARITY 1,
            INDEX address_index address TYPE bloom_filter GRANULARITY 4,
            INDEX inserted_index inserted_at TYPE minmax GRANULARITY 4
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(block_time)
        ORDER BY (topic0, address, block_number, transaction_index, log_index)
    """,
    "blocks": """
        CREATE TABLE IF NOT EXISTS {table} (
            block_number UInt64,
            block_hash String,
            parent_hash String,
            block_time DateTime('UTC'),
            gas_used UInt64,
            gas_limit UInt64,
            base_fee_per_gas Nullable(UInt64),
            miner LowCardinality(String),
            inserted_at DateTime64(6, 'UTC'),
            received_at Nullable(DateTime64(3, 'UTC')),
            INDEX time_index block_time TYPE minmax GRANULARITY 4
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(block_time)
        ORDER BY block_number
    """,
    "contracts": """
        CREATE TABLE IF NOT EXISTS {table} (
            address String,
            contract LowCardinality(String),
            start_block UInt64,
            active Bool,
            source LowCardinality(String),
            label String,
            created_block Nullable(UInt64),
            creation_args String,
            inserted_at DateTime64(6, 'UTC')
        ) ENGINE = MergeTree
        ORDER BY (address, inserted_at)
    """,
    "orphaned_blocks": """
        CREATE TABLE IF NOT EXISTS {table} (
            block_number UInt64,
            block_hash String,
            canonical_hash String,
            detected_by LowCardinality(String),
            inserted_at DateTime64(6, 'UTC')
        ) ENGINE = MergeTree
        ORDER BY (block_number, block_hash)
    """,
    "audits": """
        CREATE TABLE IF NOT EXISTS {table} (
            kind LowCardinality(String),
            from_block UInt64,
            to_block UInt64,
            streams UInt32,
            rpc_logs UInt64,
            stored_logs UInt64,
            missing_logs UInt64,
            extra_logs UInt64,
            repaired_logs UInt64,
            detail String,
            inserted_at DateTime64(6, 'UTC')
        ) ENGINE = MergeTree
        ORDER BY inserted_at
    """,
    "calls": """
        CREATE TABLE IF NOT EXISTS {table} (
            read LowCardinality(String),
            block_number UInt64,
            block_hash String,
            block_time DateTime('UTC'),
            address LowCardinality(String),
            function LowCardinality(String),
            input String,
            success Bool,
            output String,
            inserted_at DateTime64(6, 'UTC'),
            INDEX inserted_index inserted_at TYPE minmax GRANULARITY 4
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(block_time)
        ORDER BY (read, block_number, address, input)
    """,
    # The accounts' transactions with their receipts, raw, plus the fields the books need typed. ``fee`` is what the
    # sender paid in native wei: gas used at the effective price, plus the L1 data fee on OP-stack chains.
    "transactions": """
        CREATE TABLE IF NOT EXISTS {table} (
            account LowCardinality(String),
            block_number UInt64,
            block_hash String,
            block_time DateTime('UTC'),
            transaction_hash String,
            transaction_index UInt32,
            sender String,
            recipient String,
            nonce UInt64,
            value UInt256,
            status UInt8,
            gas_used UInt64,
            effective_gas_price UInt256,
            l1_fee UInt256,
            fee UInt256,
            found_by LowCardinality(String),
            raw_transaction String,
            raw_receipt String,
            inserted_at DateTime64(6, 'UTC'),
            INDEX inserted_index inserted_at TYPE minmax GRANULARITY 4
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(block_time)
        ORDER BY (account, block_number, transaction_index)
    """,
    # The transactions behind the stored events of the kinds that ask for them (a pool's swaps), one row per hash,
    # raw as the node returned them plus the fields the markout studies read typed. ``gas_price`` is what the sender
    # paid per gas: a mined transaction's gasPrice is its effective price (checked against receipts).
    # Every one of them succeeded, since a reverted transaction emits no log, so no receipt is fetched.
    "event_transactions": """
        CREATE TABLE IF NOT EXISTS {table} (
            block_number UInt64,
            block_hash String CODEC(ZSTD(1)),
            block_time DateTime('UTC'),
            transaction_hash String CODEC(ZSTD(1)),
            transaction_index UInt32,
            sender String,
            recipient String,
            nonce UInt64,
            value UInt256,
            type UInt8,
            gas_limit UInt64,
            gas_price UInt256,
            max_fee_per_gas Nullable(UInt256),
            max_priority_fee_per_gas Nullable(UInt256),
            selector String,
            raw_transaction String CODEC(ZSTD(1)),
            inserted_at DateTime64(6, 'UTC'),
            INDEX hash_index transaction_hash TYPE bloom_filter GRANULARITY 4,
            INDEX inserted_index inserted_at TYPE minmax GRANULARITY 4
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(block_time)
        ORDER BY (block_number, transaction_index)
    """,
    "cursors": """
        CREATE TABLE IF NOT EXISTS {table} (
            stream String,
            from_block UInt64,
            to_block UInt64,
            until_block UInt64,
            updated_at DateTime64(6, 'UTC')
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY stream
    """,
    "schema_migrations": """
        CREATE TABLE IF NOT EXISTS {table} (
            name String,
            checksum String,
            applied_at DateTime64(6, 'UTC')
        ) ENGINE = MergeTree
        ORDER BY name
    """,
}
# Changes to tables an older release created, applied once each in this order and recorded in ``schema_migrations``
# with the checksum of their SQL. A migration whose SQL changed after it was applied stops the start, since the database
# would no longer be what the code says. Each must be safe to run twice (``IF NOT EXISTS``): a crash between running it
# and recording it runs it again. A new table needs none, since ``TABLES`` creates what is missing.
MIGRATIONS: list[tuple[str, str]] = [
    (
        "2026-10-06 blocks.received_at",
        "ALTER TABLE {database}.blocks ADD COLUMN IF NOT EXISTS received_at Nullable(DateTime64(3, 'UTC'))",
    ),
]
# Repeating an identical insert (a retry after a timeout) is a no-op within this many recent inserts.
DEDUPLICATION_WINDOW = 1000

LOG_COLUMNS = [
    "block_number", "block_hash", "block_time", "transaction_hash", "transaction_index", "log_index", "address",
    "topic0", "topic1", "topic2", "topic3", "data", "inserted_at", "lane",
]
BLOCK_COLUMNS = [
    "block_number", "block_hash", "parent_hash", "block_time", "gas_used", "gas_limit", "base_fee_per_gas", "miner",
    "inserted_at", "received_at",
]
CALL_COLUMNS = [
    "read", "block_number", "block_hash", "block_time", "address", "function", "input", "success", "output", "inserted_at",
]
TRANSACTION_COLUMNS = [
    "account", "block_number", "block_hash", "block_time", "transaction_hash", "transaction_index", "sender", "recipient",
    "nonce", "value", "status", "gas_used", "effective_gas_price", "l1_fee", "fee", "found_by", "raw_transaction",
    "raw_receipt", "inserted_at",
]
CONTRACT_COLUMNS = [
    "address", "contract", "start_block", "active", "source", "label", "created_block", "creation_args", "inserted_at",
]
EVENT_TRANSACTION_COLUMNS = [
    "block_number", "block_hash", "block_time", "transaction_hash", "transaction_index", "sender", "recipient", "nonce",
    "value", "type", "gas_limit", "gas_price", "max_fee_per_gas", "max_priority_fee_per_gas", "selector",
    "raw_transaction", "inserted_at",
]


# ClickHouse refuses an HTTP parameter over 128 KiB ("Field value too long"): an address list is ~45 bytes an item, a
# block number ~10.
MAX_PARAMETER_ADDRESSES = 1000
MAX_PARAMETER_NUMBERS = 5000

class ContractRow(BaseModel):
    """One registry change: an address of a contract kind starts or stops being indexed."""

    address: str
    contract: str
    start_block: int
    active: bool = True
    source: Literal["config", "cli", "factory"] = "config"
    label: str = ""
    created_block: int | None = None
    creation_args: dict[str, Any] = {}


class CallRow(BaseModel):
    """One view call's raw result at one block."""

    read: str
    block_number: int
    block_hash: str
    block_time: int
    address: str
    function: str
    input: str
    success: bool
    output: str


# How a transaction of an account was found: by a stored log naming it, by its nonce, or by its balance.
FoundBy = Literal["logs", "nonce", "balance"]


class TransactionRow(BaseModel):
    """One transaction of an account, with its receipt; ``found_by`` says how: logs, nonce or balance."""

    account: str
    block_number: int
    block_hash: str
    block_time: int
    transaction_hash: str
    transaction_index: int
    sender: str
    recipient: str
    nonce: int
    value: int
    status: int
    gas_used: int
    effective_gas_price: int
    l1_fee: int
    fee: int
    found_by: FoundBy
    raw_transaction: str
    raw_receipt: str


class EventTransactionRow(BaseModel):
    """One transaction behind a stored event, as the node returned it and with its fee fields typed."""

    block_number: int
    block_hash: str
    block_time: int
    transaction_hash: str
    transaction_index: int
    sender: str
    recipient: str
    nonce: int
    value: int
    type: int
    gas_limit: int
    gas_price: int
    max_fee_per_gas: int | None
    max_priority_fee_per_gas: int | None
    selector: str
    raw_transaction: str

    @classmethod
    def from_rpc(cls, transaction: dict[str, Any], block_time: int) -> "EventTransactionRow":
        """Builds a row from an ``eth_getTransactionByHash`` result; a legacy transaction has no fee cap fields."""

        def optional(field: str) -> int | None:
            return int(transaction[field], 16) if transaction.get(field) is not None else None

        return cls(
            block_number=int(transaction["blockNumber"], 16), block_hash=transaction["blockHash"].lower(), block_time=block_time,
            transaction_hash=transaction["hash"].lower(), transaction_index=int(transaction["transactionIndex"], 16),
            sender=transaction["from"].lower(), recipient=(transaction.get("to") or "").lower(),
            nonce=int(transaction["nonce"], 16), value=int(transaction["value"], 16), type=int(transaction.get("type") or "0x0", 16),
            gas_limit=int(transaction["gas"], 16), gas_price=int(transaction["gasPrice"], 16),
            max_fee_per_gas=optional("maxFeePerGas"), max_priority_fee_per_gas=optional("maxPriorityFeePerGas"),
            selector=(transaction.get("input") or "0x")[:10].lower(), raw_transaction=json.dumps(transaction, sort_keys=True),
        )


class Cursor(BaseModel):
    """The block range a stream has written: ``from_block > to_block`` means nothing yet.

    ``until_block`` ends a catch-up stream: an address added to a topic-scanned kind is fetched on its own up to
    the block the topic scan had reached, and the scan covers it from there.
    """

    stream: str
    from_block: int
    to_block: int
    until_block: int | None = None

    @property
    def done(self) -> bool:
        """Tells whether a catch-up stream has reached its end."""
        return self.until_block is not None and self.to_block >= self.until_block


class AuditRow(BaseModel):
    """The outcome of one comparison of RPC logs with stored logs."""

    kind: str
    from_block: int
    to_block: int
    streams: int
    rpc_logs: int
    stored_logs: int
    missing_logs: int
    extra_logs: int
    repaired_logs: int
    detail: str = ""


class InsertClock:
    """Hands out strictly increasing insert times, so dbt can use ``inserted_at`` as its watermark."""

    def __init__(self) -> None:
        self.last = 0

    def next(self) -> datetime:
        """Returns a time after every one handed out before."""
        now = time.time_ns() // 1000
        self.last = max(now, self.last + 1)
        return datetime.fromtimestamp(self.last / 1_000_000, tz=UTC)


def _topic(log: Log, index: int) -> str:
    return log.topics[index] if len(log.topics) > index else ""


def _log_row(log: Log, inserted_at: datetime, lane: str) -> list[Any]:
    block_time = datetime.fromtimestamp(log.block_timestamp or 0, tz=UTC)
    return [
        log.block_number, log.block_hash, block_time, log.transaction_hash, log.transaction_index, log.log_index,
        log.address, _topic(log, 0), _topic(log, 1), _topic(log, 2), _topic(log, 3), log.data, inserted_at, lane,
    ]


class ChainStore:
    """Reads and writes one chain database; every write goes through one lock so insert times stay ordered."""

    def __init__(self, client: Any, database: str):
        self.client = client
        self.database = database
        self.clock = InsertClock()
        self.write_lock = asyncio.Lock()

    def create(self) -> None:
        """Creates the database and every table that is missing."""
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {safe_database(self.database)}")
        for table in TABLES:
            self.create_table(table, f"{self.database}.{table}")
        self.migrate()

    def migrate(self) -> list[str]:
        """Applies the migrations this database has not had yet and returns their names."""
        rows = self.client.query(f"SELECT name, checksum FROM {self.database}.schema_migrations").result_rows
        applied = {str(name): str(checksum) for name, checksum in rows}
        done = []
        for name, sql in MIGRATIONS:
            checksum = hashlib.sha256(sql.encode()).hexdigest()
            if name in applied:
                if applied[name] != checksum:
                    raise RuntimeError(
                        f"Migration {name!r} was applied with other SQL than the code now has; add a new migration instead"
                    )
                continue
            self.client.command(sql.format(database=self.database))
            self.client.insert(
                f"{self.database}.schema_migrations", [[name, checksum, datetime.now(tz=UTC)]],
                column_names=["name", "checksum", "applied_at"],
            )
            logger.info(f"Applied migration {name} to {self.database}")
            done.append(name)
        return done

    def create_table(self, table: str, target: str) -> None:
        """Creates one raw table under the name ``target`` if it is missing, with its insert deduplication."""
        self.client.command(TABLES[table].format(table=target))
        if table in ("logs", "blocks", "contracts", "calls"):
            self.client.command(
                f"ALTER TABLE {target} MODIFY SETTING non_replicated_deduplication_window = {DEDUPLICATION_WINDOW}"
            )

    async def _insert(
        self, table: str, rows: list[list[Any]], columns: list[str], settings: dict[str, Any] | None = None
    ) -> None:
        """Inserts rows into one of this database's tables in a worker thread, written as soon as ClickHouse has them.

        Every write is one batch behind the write lock, so the server's async-insert buffer has nothing to merge it with
        and would only hold it (about 55 ms a write on a busy chain) while the lock waits.
        """
        await asyncio.to_thread(
            self.client.insert, f"{self.database}.{table}", rows, column_names=columns,
            settings={"async_insert": 0, **(settings or {})},
        )

    async def _query(self, sql: str, parameters: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        """Runs a query in a worker thread, so the event loop never waits on ClickHouse, and returns its rows."""
        return await asyncio.to_thread(lambda: list(self.client.query(sql, parameters=parameters or {}).result_rows))

    async def stored_log_keys(self, logs: list[Log]) -> set[tuple[int, int, str]]:
        """Returns ``(block_number, log_index, block_hash)`` of stored logs that may be some of ``logs``.

        A stored copy of a log has its topic and address, the sorting key's prefix, so the query reads only those runs.
        The pairs go in pieces: a chain-wide scan's window holds thousands, past one HTTP parameter's 128 KiB.
        """
        from_block = min(log.block_number for log in logs)
        to_block = max(log.block_number for log in logs)
        pairs = sorted({(log.topic0, log.address) for log in logs})
        keys: set[tuple[int, int, str]] = set()
        for start in range(0, len(pairs), MAX_PARAMETER_ADDRESSES):
            piece = pairs[start : start + MAX_PARAMETER_ADDRESSES]
            rows = await self._query(
                f"SELECT block_number, log_index, block_hash FROM {self.database}.logs "
                "WHERE topic0 IN {topics:Array(String)} AND address IN {addresses:Array(String)} "
                "AND block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64}",
                {
                    "topics": sorted({topic0 for topic0, _address in piece}),
                    "addresses": sorted({address for _topic0, address in piece}),
                    "from_block": from_block,
                    "to_block": to_block,
                },
            )
            keys.update((int(number), int(index), str(block_hash)) for number, index, block_hash in rows)
        return keys

    async def insert_logs(self, logs: list[Log], *, lane: str) -> tuple[int, int]:
        """Inserts the logs not already stored and returns ``(inserted, already_stored)``."""
        if not logs:
            return 0, 0
        async with self.write_lock:
            from_block = min(log.block_number for log in logs)
            to_block = max(log.block_number for log in logs)
            stored = await self.stored_log_keys(logs)
            fresh = [log for log in logs if log.key not in stored]
            if fresh:
                inserted_at = self.clock.next()
                rows = [_log_row(log, inserted_at, lane) for log in fresh]
                token = f"logs:{from_block}:{to_block}:{len(fresh)}:{fresh[0].key}:{fresh[-1].key}"
                await self._insert("logs", rows, LOG_COLUMNS, {"insert_deduplication_token": token})
            return len(fresh), len(logs) - len(fresh)

    async def insert_blocks(self, headers: list[Header], arrivals: dict[tuple[int, str], float] | None = None) -> int:
        """Inserts the block headers not already stored, each with when its head first reached us where a websocket
        delivered it, and returns how many went in."""
        arrivals = arrivals or {}
        if not headers:
            return 0
        async with self.write_lock:
            stored = await self._query(
                f"SELECT block_number, block_hash FROM {self.database}.blocks "
                "WHERE block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64}",
                {"from_block": min(header.number for header in headers), "to_block": max(header.number for header in headers)},
            )
            already = {(int(number), str(block_hash)) for number, block_hash in stored}
            headers = [header for header in headers if (header.number, header.hash) not in already]
            if not headers:
                return 0
            inserted_at = self.clock.next()
            rows = [
                [
                    header.number, header.hash, header.parent_hash, datetime.fromtimestamp(header.timestamp, tz=UTC),
                    header.gas_used, header.gas_limit, header.base_fee_per_gas, header.miner, inserted_at,
                    datetime.fromtimestamp(arrived, tz=UTC) if (arrived := arrivals.get((header.number, header.hash))) else None,
                ]
                for header in headers
            ]
            await self._insert("blocks", rows, BLOCK_COLUMNS)
            return len(rows)

    async def insert_calls(self, rows: list[CallRow]) -> int:
        """Inserts call results for blocks a read has not stored yet and returns how many rows went in."""
        if not rows:
            return 0
        async with self.write_lock:
            read_blocks = {(row.read, row.block_number) for row in rows}
            stored = await self._query(
                f"SELECT DISTINCT read, block_number FROM {self.database}.calls "
                "WHERE read IN {reads:Array(String)} AND block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64}",
                {
                    "reads": sorted({read for read, _number in read_blocks}),
                    "from_block": min(number for _read, number in read_blocks),
                    "to_block": max(number for _read, number in read_blocks),
                },
            )
            already = {(str(read), int(number)) for read, number in stored}
            fresh = [row for row in rows if (row.read, row.block_number) not in already]
            if fresh:
                inserted_at = self.clock.next()
                values = [
                    [
                        row.read, row.block_number, row.block_hash, datetime.fromtimestamp(row.block_time, tz=UTC),
                        row.address, row.function, row.input, row.success, row.output, inserted_at,
                    ]
                    for row in fresh
                ]
                await self._insert("calls", values, CALL_COLUMNS)
            return len(fresh)

    async def latest_calls(self, reads: list[str]) -> dict[tuple[str, str, str, str], tuple[bool, str]]:
        """Returns the newest stored ``(success, output)`` of every ``(read, address, function, input)`` of some reads."""
        rows = await self._query(
            "SELECT read, address, function, input, argMax(success, block_number), argMax(output, block_number) "
            f"FROM {self.database}.calls WHERE read IN {{reads:Array(String)}} GROUP BY read, address, function, input",
            {"reads": reads},
        )
        return {
            (str(read), str(address), str(function), str(data)): (bool(success), str(output))
            for read, address, function, data, success, output in rows
        }

    async def account_log_transactions(self, account_word: str, from_block: int, to_block: int) -> list[str]:
        """Returns the hashes of the stored logs' transactions that name an account at topic 1, 2 or 3."""
        rows = await self._query(
            f"SELECT DISTINCT transaction_hash FROM {self.database}.logs "
            "WHERE block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64} "
            "AND (topic1 = {word:String} OR topic2 = {word:String} OR topic3 = {word:String})",
            {"from_block": from_block, "to_block": to_block, "word": account_word},
        )
        return sorted(str(transaction_hash) for (transaction_hash,) in rows)

    async def account_unwraps(self, account_word: str, unwrap_topic: str, from_block: int, to_block: int) -> list[tuple[int, int]]:
        """Returns ``(block_number, wei)`` of the stored unwraps (WETH9 ``Withdrawal``) that paid an account."""
        rows = await self._query(
            f"SELECT block_number, data FROM {self.database}.logs "
            "WHERE block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64} "
            "AND topic0 = {topic:String} AND topic1 = {word:String}",
            {"from_block": from_block, "to_block": to_block, "topic": unwrap_topic, "word": account_word},
        )
        return [(int(number), int(str(data), 16)) for number, data in rows]

    async def stored_transactions(self, account: str, from_block: int, to_block: int) -> set[str]:
        """Returns the hashes of an account's stored transactions in a range."""
        rows = await self._query(
            f"SELECT transaction_hash FROM {self.database}.transactions "
            "WHERE account = {account:String} AND block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64}",
            {"account": account, "from_block": from_block, "to_block": to_block},
        )
        return {str(transaction_hash) for (transaction_hash,) in rows}

    async def insert_transactions(self, rows: list[TransactionRow]) -> int:
        """Inserts transactions not stored yet and returns how many went in."""
        if not rows:
            return 0
        async with self.write_lock:
            stored = await self.stored_transactions(
                rows[0].account, min(row.block_number for row in rows), max(row.block_number for row in rows)
            )
            fresh = [row for row in rows if row.transaction_hash not in stored]
            if fresh:
                inserted_at = self.clock.next()
                values = [
                    [
                        row.account, row.block_number, row.block_hash, datetime.fromtimestamp(row.block_time, tz=UTC),
                        row.transaction_hash, row.transaction_index, row.sender, row.recipient, row.nonce, row.value,
                        row.status, row.gas_used, row.effective_gas_price, row.l1_fee, row.fee, row.found_by,
                        row.raw_transaction, row.raw_receipt, inserted_at,
                    ]
                    for row in fresh
                ]
                await self._insert("transactions", values, TRANSACTION_COLUMNS)
            return len(fresh)

    async def event_log_transactions(
        self, topics: list[str], addresses: list[str] | None, from_block: int, to_block: int
    ) -> list[tuple[str, int, int]]:
        """Returns ``(transaction_hash, block_number, block_time)`` of the stored logs of some events of some addresses,
        or of every address when ``addresses`` is None.

        A transaction holding several of the logs (a route through two pools) is returned once. The topic and address
        filters are the sorting key's prefix, so only those runs are read.
        """
        query = (
            f"SELECT transaction_hash, min(block_number), toUnixTimestamp(min(block_time)) FROM {self.database}.logs "
            "WHERE topic0 IN {topics:Array(String)} AND block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64} "
        )
        window = {"topics": topics, "from_block": from_block, "to_block": to_block}
        if addresses is None:
            rows = await self._query(query + "GROUP BY transaction_hash", window)
        else:
            rows = []
            for start in range(0, len(addresses), MAX_PARAMETER_ADDRESSES):
                rows.extend(
                    await self._query(
                        query + "AND address IN {addresses:Array(String)} GROUP BY transaction_hash",
                        window | {"addresses": addresses[start : start + MAX_PARAMETER_ADDRESSES]},
                    )
                )
        found = {str(transaction_hash): (int(number), int(timestamp)) for transaction_hash, number, timestamp in rows}
        return sorted((transaction_hash, number, timestamp) for transaction_hash, (number, timestamp) in found.items())

    async def stored_event_transactions(self, from_block: int, to_block: int) -> set[str]:
        """Returns the hashes of the event transactions stored in a block range."""
        rows = await self._query(
            f"SELECT transaction_hash FROM {self.database}.event_transactions "
            "WHERE block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64}",
            {"from_block": from_block, "to_block": to_block},
        )
        return {str(transaction_hash) for (transaction_hash,) in rows}

    async def insert_event_transactions(self, rows: list[EventTransactionRow]) -> int:
        """Inserts event transactions not stored yet and returns how many went in."""
        if not rows:
            return 0
        async with self.write_lock:
            stored = await self.stored_event_transactions(
                min(row.block_number for row in rows), max(row.block_number for row in rows)
            )
            fresh = [row for row in rows if row.transaction_hash not in stored]
            if fresh:
                inserted_at = self.clock.next()
                values = [
                    [
                        row.block_number, row.block_hash, datetime.fromtimestamp(row.block_time, tz=UTC), row.transaction_hash,
                        row.transaction_index, row.sender, row.recipient, row.nonce, row.value, row.type, row.gas_limit,
                        row.gas_price, row.max_fee_per_gas, row.max_priority_fee_per_gas, row.selector, row.raw_transaction,
                        inserted_at,
                    ]
                    for row in fresh
                ]
                await self._insert("event_transactions", values, EVENT_TRANSACTION_COLUMNS)
            return len(fresh)

    async def block_times(self, numbers: list[int]) -> dict[int, int]:
        """Returns the timestamps this database already holds for some blocks: from ``blocks``, then from stored logs.

        A backfill over history mostly lands on blocks some stored log already stamped, so it needs few headers.
        """
        found: dict[int, int] = {}
        for table in ("blocks", "logs"):
            unknown = sorted(set(numbers) - set(found))
            # In pieces, since one HTTP parameter holds at most 128 KiB of block numbers. The range lets the block_number
            # minmax index skip most of a logs partition.
            for start in range(0, len(unknown), MAX_PARAMETER_NUMBERS):
                piece = unknown[start : start + MAX_PARAMETER_NUMBERS]
                rows = await self._query(
                    f"SELECT block_number, toUnixTimestamp(max(block_time)) FROM {self.database}.{table} "
                    "WHERE block_number BETWEEN {first:UInt64} AND {last:UInt64} "
                    "AND block_number IN {numbers:Array(UInt64)} AND block_time > 0 GROUP BY block_number",
                    {"first": piece[0], "last": piece[-1], "numbers": piece},
                )
                found.update({int(number): int(timestamp) for number, timestamp in rows})
        return found

    async def max_block(self) -> int | None:
        """Returns the newest stored header's number."""
        rows = await self._query(f"SELECT max(block_number), count() FROM {self.database}.blocks")
        newest, count = rows[0]
        return int(newest) if count else None

    async def stored_logs(self, from_block: int, to_block: int) -> list[tuple[int, int, str, str, str]]:
        """Returns ``(block_number, log_index, block_hash, address, topic0)`` of every stored log in a range."""
        rows = await self._query(
            f"SELECT block_number, log_index, block_hash, address, topic0 FROM {self.database}.logs "
            "WHERE block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64}",
            {"from_block": from_block, "to_block": to_block},
        )
        return [
            (int(number), int(index), str(block_hash), str(address), str(topic0))
            for number, index, block_hash, address, topic0 in rows
        ]

    async def stored_block_hashes(self, from_block: int, to_block: int) -> set[tuple[int, str]]:
        """Returns every ``(block_number, block_hash)`` that some raw table holds a row of in a range."""
        tables = ("logs", "calls", "transactions", "event_transactions")
        union = " UNION ALL ".join(
            f"SELECT block_number, block_hash FROM {self.database}.{table} "
            "WHERE block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64}"
            for table in tables
        )
        rows = await self._query(
            f"SELECT block_number, block_hash FROM ({union}) GROUP BY block_number, block_hash",
            {"from_block": from_block, "to_block": to_block},
        )
        return {(int(number), str(block_hash)) for number, block_hash in rows}

    async def record_contracts(self, rows: list[ContractRow]) -> None:
        """Appends registry changes."""
        if not rows:
            return
        async with self.write_lock:
            # Each row gets its own time, so the latest of two rows for one address is never ambiguous.
            values = [
                [
                    row.address, row.contract, row.start_block, row.active, row.source, row.label, row.created_block,
                    json.dumps(row.creation_args, sort_keys=True), self.clock.next(),
                ]
                for row in rows
            ]
            await self._insert("contracts", values, CONTRACT_COLUMNS)

    async def contracts(self) -> list[ContractRow]:
        """Returns the latest registry row of every address."""
        rows = await self._query(
            "SELECT address, argMax(contract, inserted_at), argMax(start_block, inserted_at), "
            "argMax(active, inserted_at), argMax(source, inserted_at), argMax(label, inserted_at), "
            "argMax(created_block, inserted_at), argMax(creation_args, inserted_at) "
            f"FROM {self.database}.contracts GROUP BY address",
        )
        return [
            ContractRow(
                address=address, contract=contract, start_block=int(start_block), active=bool(active), source=source,
                label=label, created_block=None if created is None else int(created),
                creation_args=json.loads(args or "{}"),
            )
            for address, contract, start_block, active, source, label, created, args in rows
        ]

    async def cursors(self) -> dict[str, Cursor]:
        """Returns every stream's cursor."""
        rows = await self._query(f"SELECT stream, from_block, to_block, until_block FROM {self.database}.cursors FINAL")
        return {
            stream: Cursor(stream=stream, from_block=int(low), to_block=int(high), until_block=int(until) or None)
            for stream, low, high, until in rows
        }

    async def save_cursors(self, cursors: list[Cursor]) -> None:
        """Records streams' new ranges."""
        if not cursors:
            return
        latest = {cursor.stream: cursor for cursor in cursors}
        async with self.write_lock:
            updated_at = self.clock.next()
            rows = [
                [cursor.stream, cursor.from_block, cursor.to_block, cursor.until_block or 0, updated_at]
                for cursor in latest.values()
            ]
            await self._insert("cursors", rows, ["stream", "from_block", "to_block", "until_block", "updated_at"])

    async def record_orphans(self, orphans: list[tuple[int, str, str]], *, detected_by: str) -> None:
        """Records ``(block_number, stored_hash, canonical_hash)`` of stored blocks that are off the chain."""
        if not orphans:
            return
        async with self.write_lock:
            inserted_at = self.clock.next()
            rows = [[number, stored, canonical, detected_by, inserted_at] for number, stored, canonical in orphans]
            await self._insert("orphaned_blocks", rows, ["block_number", "block_hash", "canonical_hash", "detected_by", "inserted_at"])

    async def orphans(self) -> set[tuple[int, str]]:
        """Returns the ``(block_number, block_hash)`` pairs already recorded as orphaned."""
        rows = await self._query(f"SELECT block_number, block_hash FROM {self.database}.orphaned_blocks")
        return {(int(number), str(block_hash)) for number, block_hash in rows}

    async def record_audit(self, audit: AuditRow) -> None:
        """Appends an audit result."""
        await self.record_audits([audit])

    async def record_audits(self, audits: list[AuditRow]) -> None:
        """Appends audit results in one insert."""
        if not audits:
            return
        async with self.write_lock:
            rows = [[*audit.model_dump().values(), self.clock.next()] for audit in audits]
            await self._insert("audits", rows, [*AuditRow.model_fields, "inserted_at"])
