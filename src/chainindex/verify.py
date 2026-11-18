"""The verifier: checks every stored block's hash against the chain, in order, and repairs forks it finds.

Logs are written once they are ``confirmations`` deep, so a reorg after that is rare. The verifier checks each block
that holds a stored row once it is ``verify.depth_seconds`` old: it asks the node for those blocks' headers, has the
auditor store the correct logs of any block whose hash changed, and records the block in ``orphaned_blocks``. Its
range is one cursor (``verify:blocks``) that moves forwards behind the head and, once that is done, backwards through
history.

The forward cursor waits for every stream within twice the depth of the head, so a stream that is catching up after a
restart, or resting after a failure, is checked once it writes. A stream further behind than that writes blocks old
enough to be final, which need no check.
"""

import asyncio
import time
from collections.abc import Iterable

from chainindex.audit import Auditor
from chainindex.config import ChainConfig
from chainindex.fetch import MissingHeader, fetch_headers
from chainindex.ledger import Ledger
from chainindex.logger import get_logger
from chainindex.metrics import Metrics
from chainindex.rpc import RpcPool
from chainindex.store import ChainStore, Cursor

logger = get_logger(__name__)

VERIFY_STREAM = "verify:blocks"


def block_ranges(numbers: Iterable[int]) -> list[tuple[int, int]]:
    """Groups block numbers into ``(first, last)`` runs of consecutive blocks."""
    ranges: list[tuple[int, int]] = []
    for number in sorted(set(numbers)):
        if ranges and number == ranges[-1][1] + 1:
            ranges[-1] = (ranges[-1][0], number)
        else:
            ranges.append((number, number))
    return ranges


class Verifier:
    """Checks the hash of every stored block once: new blocks as they get old enough, then history."""

    def __init__(
        self,
        config: ChainConfig,
        store: ChainStore,
        ledger: Ledger,
        auditor: Auditor,
        *,
        pool: RpcPool,
        metrics: Metrics,
        per_request: int = 50,
    ):
        self.config = config
        self.store = store
        self.ledger = ledger
        self.auditor = auditor
        self.pool = pool
        self.metrics = metrics
        self.per_request = per_request
        self.depth_blocks = int(config.verify.depth_seconds / config.block_seconds)
        self.cursor: Cursor | None = None
        # Set when the node has no headers below a block (pruned history): the history walk stops there.
        self.history_floor: int | None = None

    async def load(self) -> None:
        """Reads the verified range."""
        self.cursor = (await self.store.cursors()).get(VERIFY_STREAM)
        if self.cursor is not None:
            self.metrics.verify_block.set(self.cursor.to_block)
            self.metrics.verify_floor_block.set(self.cursor.from_block)

    async def check(self, start: int, end: int, *, kind: str) -> int:
        """Compares every stored block hash in a range with the chain, repairs the ones off it, and returns how many.

        The repairs run before the orphans are recorded, so a repair that fails is tried again on the next step.
        """
        stored = await self.store.stored_block_hashes(start, end)
        if not stored:
            return 0
        numbers = sorted({number for number, _block_hash in stored})
        headers = await fetch_headers(self.pool, numbers, per_batch=self.per_request)
        canonical = {header.number: header.hash for header in headers}
        self.metrics.verify_blocks.labels(kind=kind).inc(len(numbers))
        known = await self.store.orphans()
        orphans = sorted(
            (number, block_hash, canonical[number])
            for number, block_hash in stored
            if block_hash != canonical[number] and (number, block_hash) not in known
        )
        if not orphans:
            return 0
        logger.warning(f"Verify {kind} {start}-{end}: {len(orphans)} stored block(s) off the chain, first {orphans[0][0]}")
        for first, last in block_ranges(number for number, _stored, _canonical in orphans):
            # The auditor fetches the correct logs of those blocks for every stream that covers them.
            if await self.auditor.audit("verify", first, last) is None:
                self.metrics.verify_unrepaired.inc()
                logger.warning(f"Verify: no single stream covers blocks {first}-{last}, so their correct logs were not fetched")
        # The audit records the blocks of orphaned logs itself. The rest (calls, transactions) are recorded here.
        known = await self.store.orphans()
        remaining = [orphan for orphan in orphans if (orphan[0], orphan[1]) not in known]
        await self.store.record_orphans(remaining, detected_by="verify")
        self.metrics.orphaned_blocks.inc(len(remaining))
        return len(orphans)

    async def _save(self, cursor: Cursor) -> None:
        await self.store.save_cursors([cursor])
        self.metrics.verify_block.set(cursor.to_block)
        self.metrics.verify_floor_block.set(cursor.from_block)
        self.metrics.verify_last_check.set(time.time())

    def target(self, tip: int) -> int:
        """Returns the newest block to verify: old enough, and written by every stream near the head."""
        stream_keys = {stream.key for stream in self.ledger.registry.streams()}
        near = [
            cursor.to_block for key, cursor in self.ledger.cursors.items()
            if key in stream_keys and cursor.until_block is None and cursor.to_block >= tip - 2 * self.depth_blocks
        ]
        return min([tip - self.depth_blocks, *near])

    def earliest(self) -> int | None:
        """Returns the oldest block any stream, read or transaction lane has written."""
        starts = [
            cursor.from_block for key, cursor in self.ledger.cursors.items()
            if key != VERIFY_STREAM and cursor.to_block >= cursor.from_block
        ]
        return min(starts, default=None)

    async def step(self) -> bool:
        """Checks the next window, new blocks before history, and returns whether more is waiting."""
        tip = self.ledger.tip_block
        if tip is None or tip <= self.depth_blocks:
            return False
        target = self.target(tip)
        if self.cursor is None:
            # The first check starts here. History below it is walked backwards.
            self.cursor = Cursor(stream=VERIFY_STREAM, from_block=target + 1, to_block=target)
            await self._save(self.cursor)
            logger.info(f"The verifier starts at block {target + 1}. History below it is checked backwards")
        cursor = self.cursor
        self.metrics.verify_lag_blocks.set(max(0, tip - self.depth_blocks - cursor.to_block))
        window = self.config.verify.window_blocks
        if cursor.to_block < target:
            start = cursor.to_block + 1
            end = min(target, start + window - 1)
            await self.check(start, end, kind="tip")
            cursor.to_block = end
            await self._save(cursor)
            return end < target
        earliest = self.earliest()
        if not self.config.verify.history or earliest is None:
            return False
        floor = max(earliest, self.history_floor or 0)
        if cursor.from_block <= floor:
            return False
        end = cursor.from_block - 1
        start = max(floor, end - window + 1)
        try:
            await self.check(start, end, kind="history")
        except MissingHeader as exc:
            self.history_floor = cursor.from_block
            logger.warning(f"The node has no headers in {start}-{end}, so history is verified down to {cursor.from_block}: {exc}")
            return False
        cursor.from_block = start
        await self._save(cursor)
        await asyncio.sleep(self.config.verify.history_pause_seconds)
        return True

    async def run(self) -> None:
        """Verifies forever."""
        await self.load()
        while True:
            try:
                more = await self.step()
            except Exception as exc:
                logger.error(f"Verify step failed, retrying in {self.config.verify.every_seconds:.0f} s: {exc!r}")
                more = False
            if not more:
                await asyncio.sleep(self.config.verify.every_seconds)
