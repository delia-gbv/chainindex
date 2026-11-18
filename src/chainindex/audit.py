"""The auditor: re-fetches stored ranges, compares them with ClickHouse, inserts what is missing, records forks."""

import asyncio
import random
from collections import deque

from chainindex.collect import collect
from chainindex.config import ChainConfig
from chainindex.fetch import LogFetcher, fetch_headers
from chainindex.ledger import Ledger
from chainindex.logger import get_logger
from chainindex.metrics import Metrics
from chainindex.models import Log
from chainindex.registry import CREATED, Registry, Stream
from chainindex.rpc import RpcPool
from chainindex.store import AuditRow, ChainStore

logger = get_logger(__name__)


async def _ignore_children(logs: list[Log]) -> None:
    """Leaves discovery to the ledger: an audit only compares."""


class Auditor:
    """Checks one window at a time: recent ones mostly, random historic ones sometimes, flagged ones first."""

    def __init__(
        self,
        config: ChainConfig,
        registry: Registry,
        store: ChainStore,
        ledger: Ledger,
        *,
        fetcher: LogFetcher,
        pool: RpcPool,
        metrics: Metrics,
        seed: int | None = None,
    ):
        self.config = config
        self.registry = registry
        self.store = store
        self.ledger = ledger
        self.fetcher = fetcher
        self.pool = pool
        self.metrics = metrics
        self.random = random.Random(seed)
        self.flagged: deque[tuple[int, int]] = deque(maxlen=100)

    def flag(self, from_block: int, to_block: int) -> None:
        """Queues a range for the next audit, for example one where the ledger and the stream disagreed."""
        self.flagged.append((from_block, to_block))

    def _covering(self, from_block: int, to_block: int) -> list[Stream]:
        return [
            stream for stream in self.registry.streams()
            if (cursor := self.ledger.cursors.get(stream.key)) is not None
            and cursor.from_block <= from_block and cursor.to_block >= to_block
        ]

    def choose(self) -> tuple[str, int, int] | None:
        """Picks the next window to audit, or None while nothing is stored."""
        if self.flagged:
            from_block, to_block = self.flagged.popleft()
            return "flagged", from_block, to_block
        tip = self.ledger.tip_block
        if tip is None:
            return None
        window = self.config.audit.window_blocks
        if self.random.random() >= self.config.audit.historic_share:
            end = tip - self.random.randint(0, max(0, self.config.audit.recent_blocks - window))
            return "recent", end - window + 1, end
        # Only log streams' ranges: the cursors table also holds the reads', the transactions' and the verifier's.
        stream_keys = {stream.key for stream in self.registry.streams()}
        spans = [
            cursor for key, cursor in self.ledger.cursors.items()
            if key in stream_keys and cursor.until_block is None and cursor.to_block - cursor.from_block >= window
        ]
        if not spans:
            return None
        cursor = self.random.choice(spans)
        start = self.random.randint(cursor.from_block, cursor.to_block - window + 1)
        return "historic", start, start + window - 1

    async def audit(self, kind: str, from_block: int, to_block: int) -> AuditRow | None:
        """Compares one window and repairs it, returning what was found."""
        streams = self._covering(from_block, to_block)
        if not streams:
            return None
        addresses = self.registry.addresses_of(streams)
        # The shared scan and a `track: any` kind's scan store every log with their topics, whoever emitted it, so they
        # are compared by topic: their emitters are not members.
        scanned_topics = {
            topic for stream in streams
            if stream.contract == CREATED or self.config.contract(stream.contract).track == "any"
            for topic in stream.topics or []
        }
        rpc_logs = await collect(self.registry, streams, from_block, to_block, self.fetcher, on_children=_ignore_children)
        rpc_keys = {log.key for log in rpc_logs}
        stored = [
            row for row in await self.store.stored_logs(from_block, to_block)
            if row[3] in addresses or row[4] in scanned_topics
        ]
        stored_keys = {(number, index, block_hash) for number, index, block_hash, _address, _topic0 in stored}
        missing = [log for log in rpc_logs if log.key not in stored_keys]
        extra = stored_keys - rpc_keys
        repaired = 0
        if missing:
            repaired, _already = await self.store.insert_logs(missing, lane="audit")
            self.metrics.audit_missing_logs.inc(repaired)
            logger.warning(f"Audit {kind} {from_block}-{to_block}: inserted {repaired} log(s) ClickHouse was missing")
        detail = ""
        if extra:
            detail = await self._explain_extra(extra)
            self.metrics.audit_extra_logs.inc(len(extra))
        row = AuditRow(
            kind=kind, from_block=from_block, to_block=to_block, streams=len(streams), rpc_logs=len(rpc_keys),
            stored_logs=len(stored_keys), missing_logs=len(missing), extra_logs=len(extra), repaired_logs=repaired,
            detail=detail,
        )
        await self.store.record_audit(row)
        result = "clean" if not missing and not extra else "repaired"
        self.metrics.audits.labels(kind=kind, result=result).inc()
        return row

    async def _explain_extra(self, extra: set[tuple[int, int, str]]) -> str:
        """Records stored blocks whose hash is no longer canonical; anything else is reported as unexplained."""
        numbers = sorted({number for number, _index, _hash in extra})
        canonical = {header.number: header.hash for header in await fetch_headers(self.pool, numbers)}
        known = await self.store.orphans()
        orphans = sorted(
            {
                (number, block_hash, canonical[number])
                for number, _index, block_hash in extra
                if block_hash != canonical[number] and (number, block_hash) not in known
            }
        )
        if orphans:
            await self.store.record_orphans(orphans, detected_by="audit")
            self.metrics.orphaned_blocks.inc(len(orphans))
            logger.warning(f"Audit recorded {len(orphans)} orphaned block(s), first {orphans[0][0]}")
        unexplained = sum(1 for number, _index, block_hash in extra if block_hash == canonical[number])
        if unexplained:
            logger.error(f"Audit: {unexplained} stored log(s) on canonical blocks that the RPC no longer returns")
        return f"orphaned_blocks={len(orphans)} unexplained_logs={unexplained}"

    async def run(self) -> None:
        """Audits forever."""
        while True:
            await asyncio.sleep(self.config.audit.every_seconds)
            choice = self.choose()
            if choice is None:
                continue
            kind, from_block, to_block = choice
            try:
                await self.audit(kind, from_block, to_block)
            except Exception as exc:
                logger.error(f"Audit of {from_block}-{to_block} failed: {exc!r}")
