"""The transactions behind stored events: who sent each swap, through what, at what priority fee.

A kind with ``transactions: {events: [Swap]}`` gets every Swap's transaction fetched by hash, in JSON-RPC batches, and
stored raw in ``event_transactions`` once per hash. The hashes come from the logs already in ClickHouse, so the lane
trails the kind's streams window by window and never asks the node for a block the ledger has not confirmed. By hash
is the cheap way: a Base block holding seven of our swaps is 670 KiB whole, their seven transactions 12 KiB, and
A public provider answers 200 hashes in one request.

A reverted transaction emits no log, so every transaction here succeeded and no receipt is fetched: a mined
transaction's ``gasPrice`` is already its effective price. Every window writes an ``audits`` row (``kind =
'event_transactions'``) with how many hashes it wanted, had, fetched and could not get.

A ``created`` kind has no members to look up, so it takes every stored log of its events, as the shared scan stored
them, and dbt's factory check decides which of their transactions a pool's view shows. Kinds sharing an event
(Uniswap v3 and its forks log the same Swap) find the transactions already stored by the first. Kinds stepping the same
window read its stored logs and transactions once a step, not once a kind.
"""

import asyncio
import hashlib

from chainindex.config import ChainConfig, ContractSpec
from chainindex.ledger import Ledger
from chainindex.logger import get_logger
from chainindex.metrics import Metrics
from chainindex.registry import CREATED, Registry
from chainindex.rpc import RpcError, RpcPool
from chainindex.store import AuditRow, ChainStore, Cursor, EventTransactionRow

logger = get_logger(__name__)

HASHES_PER_REQUEST = 100
ERROR_PAUSE_SECONDS = 5.0


class StepReads:
    """The stored logs and transactions one step has read, so kinds on the same window read each from ClickHouse once.

    Only this lane writes ``event_transactions``, so a window's stored hashes stay right once its fetched rows are added.
    """

    def __init__(self, store: ChainStore):
        self.store = store
        self.logs: dict[tuple[tuple[str, ...], tuple[str, ...] | None, int, int], list[tuple[str, int, int]]] = {}
        self.transactions: dict[tuple[int, int], set[str]] = {}

    async def logged(self, topics: list[str], addresses: list[str] | None, start: int, end: int) -> list[tuple[str, int, int]]:
        """Returns the transactions behind some events' stored logs in a window, as ``ChainStore.event_log_transactions``."""
        key = (tuple(topics), None if addresses is None else tuple(addresses), start, end)
        if key not in self.logs:
            self.logs[key] = await self.store.event_log_transactions(topics, addresses, start, end)
        return self.logs[key]

    async def stored(self, start: int, end: int) -> set[str]:
        """Returns the hashes of the event transactions stored in a window, including those this step stored."""
        if (start, end) not in self.transactions:
            self.transactions[start, end] = await self.store.stored_event_transactions(start, end)
        return self.transactions[start, end]


def cursor_key(contract: ContractSpec) -> str:
    """Names a kind's event transactions cursor after the events, so a changed list starts over."""
    digest = hashlib.sha256(",".join(sorted(contract.transaction_topics())).encode()).hexdigest()[:12]
    return f"event_transactions:{contract.name}:{digest}"


class EventTransactions:
    """Stores the transactions of every wanted event of every kind that asks, window by window behind its streams."""

    def __init__(
        self, config: ChainConfig, registry: Registry, store: ChainStore, ledger: Ledger, *, pool: RpcPool, metrics: Metrics
    ):
        self.config = config
        self.registry = registry
        self.store = store
        self.ledger = ledger
        self.pool = pool
        self.metrics = metrics
        self.kinds = [contract for contract in config.contracts if contract.transactions is not None]
        self.cursors: dict[str, Cursor] = {}

    async def load(self) -> None:
        """Reads the stored cursors."""
        stored = await self.store.cursors()
        for contract in self.kinds:
            key = cursor_key(contract)
            if key in stored:
                self.cursors[key] = stored[key]

    def covered(self, contract: ContractSpec) -> tuple[int, int] | None:
        """Returns ``(first, last)``: the blocks every stream of the kind has stored, or None while it has no streams."""
        holder = CREATED if contract.track == "created" else contract.name
        streams = [stream for stream in self.registry.streams() if stream.contract == holder]
        if not streams:
            return None
        first = min(stream.start_block for stream in streams)
        if contract.transactions is not None and contract.transactions.start_block is not None:
            first = max(first, contract.transactions.start_block)
        return first, min(self.ledger.cursor(stream).to_block for stream in streams)

    def cursor(self, contract: ContractSpec, first: int) -> Cursor:
        """Returns a kind's cursor, empty just before its first block."""
        key = cursor_key(contract)
        if key not in self.cursors:
            self.cursors[key] = Cursor(stream=key, from_block=first, to_block=first - 1)
        return self.cursors[key]

    async def step(self) -> bool:
        """Stores the next window of every kind and returns whether any kind is still behind its streams.

        The kinds' audits and cursors are written together, one insert each, even when a later kind fails: kinds
        sharing an event step every few seconds, and a row apiece made a ClickHouse part apiece."""
        behind = False
        audits: list[AuditRow] = []
        moved: list[Cursor] = []
        reads = StepReads(self.store)
        try:
            for contract in self.kinds:
                covered = self.covered(contract)
                if covered is None or contract.transactions is None:
                    continue
                first, last = covered
                cursor = self.cursor(contract, first)
                start = cursor.to_block + 1
                end = min(last, start + contract.transactions.window_blocks - 1)
                if end < start:
                    continue
                audits.append(await self.store_window(contract, start, end, reads))
                cursor.to_block = end
                moved.append(cursor)
                self.metrics.event_transactions_block.labels(contract=contract.name).set(end)
                behind = behind or end < last
        finally:
            await self.store.record_audits(audits)
            await self.store.save_cursors(moved)
        return behind

    async def store_window(self, contract: ContractSpec, start: int, end: int, reads: StepReads) -> AuditRow:
        """Fetches and stores the transactions of a kind's wanted events in ``[start, end]`` and returns the window's
        audit, for the caller to record."""
        addresses = None if contract.track == "created" else sorted(self.registry.members(contract.name))
        logged = await reads.logged(contract.transaction_topics(), addresses, start, end)
        stored = await reads.stored(start, end)
        stored_before = len(stored)
        wanted = [(transaction_hash, timestamp) for transaction_hash, _number, timestamp in logged if transaction_hash not in stored]
        rows, unknown = await self.fetch(wanted)
        inserted = await self.store.insert_event_transactions(rows)
        stored.update(row.transaction_hash for row in rows)
        self.metrics.event_transactions_rows.labels(contract=contract.name).inc(inserted)
        self.metrics.event_transactions_unknown.labels(contract=contract.name).inc(len(unknown))
        if unknown:
            logger.warning(f"{contract.name}: blocks {start}-{end}, the node does not know {len(unknown)} transaction(s), first {unknown[0]}")
        if rows or unknown:
            logger.info(f"{contract.name}: blocks {start}-{end}, {len(logged)} transaction(s) behind its events, {inserted} new")
        return AuditRow(
            kind="event_transactions", from_block=start, to_block=end, streams=len(addresses or []), rpc_logs=len(rows),
            stored_logs=stored_before, missing_logs=len(unknown), extra_logs=0, repaired_logs=inserted,
            detail=f"contract={contract.name} hashes={len(logged)} fetched={len(rows)} unknown={len(unknown)}",
        )

    async def fetch(self, wanted: list[tuple[str, int]]) -> tuple[list[EventTransactionRow], list[str]]:
        """Fetches transactions by hash, ``HASHES_PER_REQUEST`` a request and as many at once as the lane allows, and
        returns the rows and the unknown hashes."""
        pieces = [wanted[part : part + HASHES_PER_REQUEST] for part in range(0, len(wanted), HASHES_PER_REQUEST)]
        answered = await asyncio.gather(
            *(self.pool.batch([("eth_getTransactionByHash", [transaction_hash]) for transaction_hash, _time in piece]) for piece in pieces)
        )
        rows = []
        unknown = []
        for piece, answers in zip(pieces, answered):
            for (transaction_hash, block_time), answer in zip(piece, answers):
                if isinstance(answer, RpcError):
                    raise answer
                if answer is None or answer.get("blockNumber") is None:
                    unknown.append(transaction_hash)
                    continue
                rows.append(EventTransactionRow.from_rpc(answer, block_time))
        return rows, unknown

    async def run(self) -> None:
        """Stores event transactions forever."""
        await self.load()
        while True:
            try:
                behind = await self.step()
            except Exception as exc:
                logger.error(f"Event transactions step failed, retrying in {ERROR_PAUSE_SECONDS:.0f} s: {exc!r}")
                await asyncio.sleep(ERROR_PAUSE_SECONDS)
                continue
            if not behind:
                await asyncio.sleep(self.config.ledger.poll_seconds)
