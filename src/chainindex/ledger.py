"""The ClickHouse ledger: confirmed logs of every stream, written insert-only, cursor by cursor.

Every stream (one address, or one contract kind scanned by topic) has a covered range. The tip lane advances the
streams already at the tip together, one window per poll. The backfill lane closes every other gap: a new contract,
an earlier start block, a catch-up after downtime. It also hands each stream to the tip lane once it has caught up.
"""

import asyncio
import time
from collections.abc import Callable

from chainindex.clickhouse import is_clickhouse_error
from chainindex.collect import collect
from chainindex.config import ChainConfig
from chainindex.fetch import LogFetcher
from chainindex.logger import get_logger
from chainindex.metrics import Metrics
from chainindex.models import Log
from chainindex.registry import Registry, Stream, address_stream_key, topic_stream_key
from chainindex.rpc import RpcPool
from chainindex.store import ChainStore, Cursor

logger = get_logger(__name__)

HASH_RETRIES = 3
HASH_RETRY_SECONDS = 2.0
ERROR_PAUSE_SECONDS = 5.0
STREAM_REST_SECONDS = 5.0
MAX_STREAM_REST_SECONDS = 600.0


class Ledger:
    """Writes confirmed logs to ClickHouse and keeps one cursor per stream."""

    def __init__(
        self,
        config: ChainConfig,
        registry: Registry,
        store: ChainStore,
        *,
        tip_fetcher: LogFetcher,
        backfill_fetcher: LogFetcher,
        head_pool: RpcPool,
        address_fetcher: LogFetcher | None = None,
        metrics: Metrics,
        canonical_hash: Callable[[int], str | None] = lambda number: None,
        on_tip_window: Callable[[list[Stream], int, int, list[Log]], None] = lambda streams, start, end, logs: None,
        on_unresolved_fork: Callable[[int, int], None] = lambda start, end: None,
    ):
        self.config = config
        self.registry = registry
        self.store = store
        self.tip_fetcher = tip_fetcher
        self.backfill_fetcher = backfill_fetcher
        # Address streams' history from another endpoint, when one is set. Topic scans stay on the backfill fetcher.
        self.address_fetcher = address_fetcher
        self.head_pool = head_pool
        self.metrics = metrics
        self.canonical_hash = canonical_hash
        self.on_tip_window = on_tip_window
        # A window written while its hashes were still disputed goes to the auditor, which re-fetches it later.
        self.on_unresolved_fork = on_unresolved_fork
        self.cursors: dict[str, Cursor] = {}
        self.tip_block: int | None = None
        self.confirmed_block: int | None = None
        # Backfill windows in blocks, adapted so a window holds about backfill_window_logs logs. Each topic scan has its
        # own, and the address streams share one, so a dense stream never sizes another's windows.
        self.backfill_windows: dict[str, int] = {}
        # Address streams a backfill worker is fetching now, which no other worker may take.
        self.in_flight: set[str] = set()
        # A topic scan's windows past its cursor: the ones being fetched, and the ones stored but not yet joined to the
        # cursor because an earlier window is still out. The cursor only ever covers a gapless run of windows.
        self.claimed: dict[str, list[tuple[int, int]]] = {}
        self.ahead: dict[str, list[tuple[int, int]]] = {}
        # Backfill streams whose last window failed: when they may be tried again, and how many failures in a row.
        self.resting: dict[str, tuple[float, int]] = {}
        # Streams that were in a failed shared window and run alone until they succeed.
        self.solo: set[str] = set()

    async def load(self) -> None:
        """Reads the stored cursors; the tip lane starts at the confirmed head on the first tip step.

        Starting it where the most advanced stream stopped would send the tip lane, which never sleeps while behind,
        through history in tip-sized windows. Whatever is behind the head is the backfill lane's, which shares windows.
        """
        self.cursors = await self.store.cursors()
        self.tip_block = None
        logger.info(f"Ledger loaded {len(self.cursors)} cursor(s)")

    def cursor(self, stream: Stream) -> Cursor:
        """Returns a stream's cursor, creating an empty one at its start block."""
        cursor = self.cursors.get(stream.key)
        if cursor is None:
            cursor = Cursor(stream=stream.key, from_block=stream.start_block, to_block=stream.start_block - 1)
            self.cursors[stream.key] = cursor
        elif cursor.to_block < cursor.from_block and (cursor.from_block, cursor.to_block) != (stream.start_block, stream.start_block - 1):
            # Nothing written yet, so an empty cursor sits just before its start: a moved start block moves it, and a
            # cursor some earlier version left below its start stops walking blocks it does not want.
            cursor.from_block = stream.start_block
            cursor.to_block = stream.start_block - 1
        if cursor.until_block is not None and stream.addresses is not None and not self.catch_up(stream):
            # A catch-up limit left from when the address belonged to a topic-scanned kind: fetched by address now, it
            # follows the tip like any other address.
            cursor.until_block = None
        return cursor

    def catch_up(self, stream: Stream) -> bool:
        """Tells whether a stream is the catch-up of a topic-scanned kind's member, which stops at its limit."""
        kinds = {contract.name: contract for contract in self.config.contracts}
        kind = kinds.get(stream.contract)
        return kind is not None and kind.fetch == "topics"

    async def plan(self) -> list[Stream]:
        """Returns every stream to index now, including catch-ups of addresses added to topic-scanned kinds."""
        streams = self.registry.streams()
        new_cursors = []
        for stream in [stream for stream in streams if stream.by_topic]:
            for address, start_block in sorted(self.registry.members(stream.contract).items()):
                key = address_stream_key(address)
                cursor = self.cursors.get(key)
                if cursor is None:
                    cursor = self._member_cursor(stream.contract, address, start_block)
                    new_cursors.append(cursor)
                if cursor.until_block is not None and not cursor.done:
                    streams.append(
                        Stream(
                            key=key, contract=stream.contract, start_block=start_block,
                            addresses=[address], topics=stream.topics,
                        )
                    )
        await self.store.save_cursors(new_cursors)
        return streams

    def _member_cursor(self, contract: str, address: str, start_block: int) -> Cursor:
        """Starts a topic-scanned member's cursor: a catch-up of the blocks the scan passed before it was a member."""
        scanned = self.cursors.get(topic_stream_key(contract))
        covered = scanned.to_block if scanned is not None else start_block - 1
        until = covered if covered >= start_block else start_block - 1
        cursor = Cursor(stream=address_stream_key(address), from_block=start_block, to_block=start_block - 1, until_block=until)
        self.cursors[cursor.stream] = cursor
        return cursor

    def _gate(self, stream: Stream) -> int | None:
        """Returns the block a topic stream may not pass (its slowest factory's end), or None."""
        factory_keys = self.registry.factory_streams(stream.contract) if stream.by_topic else []
        if not factory_keys:
            return None
        return min(self.cursors[key].to_block if key in self.cursors else -1 for key in factory_keys)

    def _target(self, stream: Stream) -> int:
        cursor = self.cursor(stream)
        target = cursor.until_block if cursor.until_block is not None else (self.tip_block or 0)
        gate = self._gate(stream)
        return target if gate is None else min(target, gate)

    async def update_head(self) -> int:
        """Reads the chain head and returns the newest block the ledger may write."""
        head = await self.head_pool.block_number()
        confirmed = head - self.config.ledger.confirmations
        self.confirmed_block = confirmed
        self.metrics.ledger_confirmed_block.set(confirmed)
        if self.tip_block is not None:
            self.metrics.ledger_lag_blocks.set(head - self.tip_block)
        return confirmed

    async def tip_step(self) -> bool:
        """Advances every stream at the tip by one window and returns whether the tip is still behind."""
        confirmed = await self.update_head()
        if self.tip_block is None:
            self.tip_block = confirmed
            logger.info(f"The tip lane starts at {confirmed}; every stream behind it is backfilled")
        streams = await self.plan()
        at_tip = [
            stream for stream in streams
            if self.cursor(stream).until_block is None and self.cursor(stream).to_block >= self.tip_block
        ]
        stragglers = self._stragglers(streams)
        end = min(confirmed, self.tip_block + self.config.ledger.tip_window_blocks)
        if end <= self.tip_block:
            if not stragglers:
                return False
            # No new block yet: only the stragglers have blocks to fetch, up to the tip.
            at_tip, end = [], self.tip_block
        at_tip += stragglers
        at_tip_keys = {stream.key for stream in at_tip}
        # A topic stream rides the tip only with every one of its factories.
        at_tip = [
            stream for stream in at_tip
            if not stream.by_topic or all(key in at_tip_keys for key in self.registry.factory_streams(stream.contract))
        ]
        self.metrics.ledger_streams.labels(lane="tip").set(len(at_tip))
        self.metrics.ledger_streams.labels(lane="backfill").set(len(streams) - len(at_tip))
        # The window starts at the furthest-behind straggler. Every other stream already has those blocks, and
        # process() drops what a stream's cursor already covers.
        start = min([self.tip_block] + [self.cursor(stream).to_block for stream in at_tip]) + 1
        # The stragglers are the backfill lane's too: while the tip lane has them, no backfill worker may take them.
        straggler_keys = {stream.key for stream in stragglers}
        self.in_flight |= straggler_keys
        try:
            logs = await self.process(at_tip, start, end, lane="tip", fetcher=self.tip_fetcher)
        except Exception as exc:
            # A ClickHouse failure comes after every stream's logs were fetched, and it fails every stream alike: going
            # one by one would only fetch each stream's logs again (1,725 requests on Base) and fail each write.
            if len(at_tip) <= 1 or is_clickhouse_error(exc):
                raise
            logger.warning(f"Tip window {start}-{end} failed, trying its streams one by one: {exc!r}")
            at_tip, logs = await self._tip_one_by_one(at_tip, start, end)
            if not at_tip:
                # Nothing can be served (the provider is down): the window is retried whole, and the tip stays put.
                raise
        finally:
            self.in_flight -= straggler_keys
        if end > self.tip_block:
            # The cross-check compares only the new blocks, which every stream in the window now has in full.
            self.on_tip_window(at_tip, self.tip_block + 1, end, [log for log in logs if log.block_number > self.tip_block])
        self.tip_block = end
        self.metrics.ledger_tip_block.set(end)
        self.metrics.ledger_last_write.set(time.time())
        return end < confirmed

    async def _tip_one_by_one(self, streams: list[Stream], start: int, end: int) -> tuple[list[Stream], list[Log]]:
        """Writes a tip window stream by stream and returns the streams that made it, with their logs.

        A stream that fails alone keeps its cursor, so it falls behind the tip and the backfill lane, which rests a
        stream the provider cannot serve, takes it over. Factories go first: a topic scan whose factory failed waits
        too, or it would pass children the factory has not announced yet.
        """
        factory_keys = {key for stream in streams if stream.by_topic for key in self.registry.factory_streams(stream.contract)}
        ordered = sorted(streams, key=lambda stream: stream.key not in factory_keys)
        failed: set[str] = set()
        written: list[Stream] = []
        logs: list[Log] = []
        for stream in ordered:
            if stream.by_topic and failed & set(self.registry.factory_streams(stream.contract)):
                failed.add(stream.key)
                continue
            try:
                logs.extend(await self.process([stream], start, end, lane="tip", fetcher=self.tip_fetcher))
                written.append(stream)
            except Exception as exc:
                failed.add(stream.key)
                # Marked as failing, so the tip lane does not take it along again before the backfill lane serves it.
                self.resting.setdefault(stream.key, (0.0, 0))
                logger.warning(f"Tip window {start}-{end} failed for {stream.key}; the backfill lane takes it over: {exc!r}")
        return written, logs

    def _stragglers(self, streams: list[Stream]) -> list[Stream]:
        """Returns the address streams just behind the tip that no backfill worker holds, for the tip lane to take."""
        return [stream for stream in streams if stream.key not in self.in_flight and self._joins_tip(stream)]

    def _joins_tip(self, stream: Stream) -> bool:
        """Tells whether an address stream is within one backfill window of the tip, which the tip lane takes along.

        The backfill lane aims a stream at the tip block of the moment it starts. With enough streams (a thousand or
        more), one pass over them takes longer than the tip takes to move, so they would trail it forever.
        """
        if self.tip_block is None:
            return False
        cursor = self.cursor(stream)
        return (
            not stream.by_topic and cursor.until_block is None
            # A stream that failed lately stays with the backfill lane, which rests it.
            and stream.key not in self.resting and stream.key not in self.solo
            # A child gated behind its factory's scan may not pass it.
            and self._target(stream) >= self.tip_block
            and 0 < self.tip_block - cursor.to_block <= self.window(stream)
        )

    def _behind(self, streams: list[Stream]) -> list[tuple[Stream, int]]:
        """Returns the streams the backfill lane owes blocks, each with its forward target."""
        work = []
        for stream in streams:
            cursor = self.cursor(stream)
            target = self._target(stream)
            forward = cursor.to_block < target
            backward = cursor.from_block > stream.start_block and cursor.to_block >= cursor.from_block
            if forward or backward:
                work.append((stream, target))
        return work

    async def backfill_step(self) -> bool:
        """Fetches one backfill window and returns whether any work was left."""
        if self.tip_block is None:
            return False
        streams = await self.plan()
        work = self._behind(streams)
        behind_blocks = sum(max(0, target - self.cursor(stream).to_block) for stream, target in work)
        self.metrics.ledger_behind_blocks.set(behind_blocks)
        if not work:
            return False
        factory_keys = {
            key for stream in streams if stream.by_topic for key in self.registry.factory_streams(stream.contract)
        }

        def priority(item: tuple[Stream, int]) -> tuple[int, int, int]:
            stream, target = item
            # A stream one window from its target is only catching up (a restart, a short outage): it goes before any
            # long history, or a contract added from years back holds every other stream off the tip until it is done.
            catching_up = target - self.cursor(stream).to_block <= self.window(stream)
            return (0 if stream.key in factory_keys else 1, 0 if catching_up else 1, self.cursor(stream).to_block)

        now = time.monotonic()
        work = [
            item for item in work
            if self.resting.get(item[0].key, (0.0, 0))[0] <= now and self._takeable(*item) and not self._joins_tip(item[0])
        ]
        if not work:
            return False
        stream, target = min(work, key=priority)
        window = self.window(stream)
        cursor = self.cursor(stream)
        group = [stream]
        backward = cursor.from_block > stream.start_block and cursor.to_block >= cursor.from_block
        if stream.by_topic and not backward:
            # A topic scan can have several windows out at once, each starting where the last claimed one ends.
            start = self._next_start(stream)
            end = min(target, start + window - 1)
        elif backward:
            start, end = max(stream.start_block, cursor.from_block - window), cursor.from_block - 1
        else:
            start = cursor.to_block + 1
            end = min(target, start + window - 1)
            if end < start:
                return False
            if not stream.by_topic and stream.key not in self.solo:
                # Every address stream with the same topics whose next block falls in this window rides along. Streams
                # that start at different blocks then end the window together and share every request after it.
                # What a member already has (or does not want) in the window is dropped before writing.
                group = [
                    other for other, other_target in work
                    if not other.by_topic and other.topics == stream.topics and other.filters == stream.filters
                    and start <= self.cursor(other).to_block + 1 <= end and other_target >= end
                    and other.key not in self.solo
                ]
        # Nothing has awaited since the work was listed, so no other worker can have taken these blocks meanwhile.
        keys = {member.key for member in group}
        pipelined = stream.by_topic and not backward
        if pipelined:
            self.claimed.setdefault(stream.key, []).append((start, end))
        else:
            self.in_flight |= keys
        fetcher = self.address_fetcher if self.address_fetcher is not None and not stream.by_topic else self.backfill_fetcher
        try:
            logs = await self.process(
                group, start, end, lane="backfill", fetcher=fetcher, backward=backward, move_cursors=not pipelined
            )
            if pipelined:
                self.ahead.setdefault(stream.key, []).append((start, end))
                await self._advance(stream)
        except Exception as exc:
            if is_clickhouse_error(exc):
                # ClickHouse fails every stream alike: running each member alone would only fetch its logs again, and
                # resting them would keep them off the lane for up to ten minutes after ClickHouse is back. The worker
                # pauses and the window is fetched again whole.
                raise
            if len(group) > 1:
                # Which member the provider refused is unknown: each runs alone once, and only the failing one rests.
                self.solo.update(member.key for member in group)
                logger.warning(f"Backfill of {len(group)} streams at {start}-{end} failed, trying them one by one: {exc!r}")
                return True
            # One stream the provider cannot serve (pruned history, say) must not hold up every other one.
            for member in group:
                _until, failures = self.resting.get(member.key, (0.0, 0))
                rest = min(MAX_STREAM_REST_SECONDS, STREAM_REST_SECONDS * 2**failures)
                self.resting[member.key] = (time.monotonic() + rest, failures + 1)
            logger.warning(f"Backfill of {len(group)} stream(s) at {start}-{end} failed, resting them: {exc!r}")
            return True
        finally:
            if pipelined:
                self.claimed[stream.key].remove((start, end))
            else:
                self.in_flight -= keys
        for member in group:
            self.resting.pop(member.key, None)
            self.solo.discard(member.key)
        self._adapt_window(stream, end - start + 1, len(logs))
        return True

    def _takeable(self, stream: Stream, target: int) -> bool:
        """Tells whether a worker may take a stream now: an address stream once, a topic scan once per free window."""
        if not stream.by_topic:
            return stream.key not in self.in_flight
        cursor = self.cursor(stream)
        if cursor.from_block > stream.start_block and cursor.to_block >= cursor.from_block:
            # Backward windows are not pipelined.
            return not self.claimed.get(stream.key)
        claims = len(self.claimed.get(stream.key, []))
        return claims < self.config.ledger.scan_windows_in_flight and self._next_start(stream) <= target

    def _next_start(self, stream: Stream) -> int:
        """Returns the first block after a topic scan's cursor that no window has claimed or stored."""
        start = self.cursor(stream).to_block + 1
        for first, last in sorted(self.claimed.get(stream.key, []) + self.ahead.get(stream.key, [])):
            if first <= start:
                start = max(start, last + 1)
        return start

    async def _advance(self, stream: Stream) -> None:
        """Moves a topic scan's cursor over the stored windows that now join it without a gap."""
        cursor = self.cursor(stream)
        ahead = sorted(self.ahead.get(stream.key, []))
        moved = False
        while ahead and ahead[0][0] <= cursor.to_block + 1:
            start, end = ahead.pop(0)
            if cursor.to_block < cursor.from_block:
                cursor.from_block = max(stream.start_block, min(cursor.from_block, start))
            cursor.to_block = max(cursor.to_block, end)
            moved = True
        self.ahead[stream.key] = ahead
        if moved:
            await self.store.save_cursors([cursor])

    def _window_key(self, stream: Stream) -> str:
        return stream.key if stream.by_topic else "addresses"

    def window(self, stream: Stream) -> int:
        """Returns the backfill window a stream's next fetch spans, in blocks."""
        return self.backfill_windows.get(self._window_key(stream), self.config.ledger.backfill_window_blocks)

    def _adapt_window(self, stream: Stream, blocks: int, logs: int) -> None:
        """Sizes a stream's next backfill window from its last one's log density."""
        target = self.config.ledger.backfill_window_logs
        largest = self.config.ledger.backfill_window_blocks
        if logs > target:
            self.backfill_windows[self._window_key(stream)] = max(1, blocks * target // logs)
        elif logs < target // 2:
            self.backfill_windows[self._window_key(stream)] = min(largest, max(self.window(stream), blocks) * 2)

    async def process(
        self,
        streams: list[Stream],
        start: int,
        end: int,
        *,
        lane: str,
        fetcher: LogFetcher,
        backward: bool = False,
        move_cursors: bool = True,
    ) -> list[Log]:
        """Fetches, checks and writes one window for some streams, moves their cursors unless told not to, and returns
        the logs."""
        logs = await self._fetch_checked(streams, start, end, fetcher)
        if not backward:
            # A stream that joined a window part-way has its blocks up to its cursor already (or before its start).
            covered = {
                address: self.cursor(stream).to_block
                for stream in streams if stream.addresses is not None for address in stream.addresses
            }
            logs = [log for log in logs if log.block_number > covered.get(log.address, start - 1)]
        inserted, already = await self.store.insert_logs(logs, lane=lane)
        self.metrics.ledger_logs.labels(lane=lane).inc(inserted)
        self.metrics.ledger_logs_already_stored.labels(lane=lane).inc(already)
        self.metrics.ledger_windows.labels(lane=lane).inc()
        moved = []
        for stream in streams if move_cursors else []:
            cursor = self.cursor(stream)
            if backward:
                cursor.from_block = start
            else:
                if cursor.to_block < cursor.from_block:
                    cursor.from_block = max(stream.start_block, min(cursor.from_block, start))
                # A window can end before a stream's start (it rode along on another's window): never move backwards.
                cursor.to_block = max(cursor.to_block, end)
            moved.append(cursor)
        if moved:
            await self.store.save_cursors(moved)
        if inserted or lane != "tip":
            logger.info(f"{lane}: blocks {start}-{end}, {len(streams)} stream(s), {inserted} log(s) written, {already} already stored")
        return logs

    async def _fetch_checked(self, streams: list[Stream], start: int, end: int, fetcher: LogFetcher) -> list[Log]:
        for attempt in range(HASH_RETRIES):
            logs = await self._fetch(streams, start, end, fetcher)
            forked = [log for log in logs if (known := self.canonical_hash(log.block_number)) and known != log.block_hash]
            if not forked:
                return logs
            self.metrics.ledger_hash_mismatches.inc()
            logger.warning(
                f"Blocks {start}-{end}: {len(forked)} log(s) carry a block hash the live chain does not know "
                f"(first at {forked[0].block_number}); fetching again (attempt {attempt + 1})"
            )
            await asyncio.sleep(HASH_RETRY_SECONDS)
        logger.error(
            f"Blocks {start}-{end}: block hashes still disagree with the live chain; writing what the node returned "
            "and auditing the range"
        )
        self.metrics.ledger_unresolved_forks.inc()
        self.on_unresolved_fork(start, end)
        return logs

    async def _fetch(self, streams: list[Stream], start: int, end: int, fetcher: LogFetcher) -> list[Log]:
        return await collect(self.registry, streams, start, end, fetcher, on_children=self._record_children)

    async def _record_children(self, logs: list[Log]) -> None:
        """Records the children factories announced before any of their logs are written."""
        discovered = self.registry.discover(logs)
        if not discovered:
            return
        await self.store.record_contracts(discovered)
        self.registry.apply(discovered)
        # Decided now, before this window moves the scan: a child created ahead of the scan needs no catch-up.
        cursors = [
            self._member_cursor(row.contract, row.address, row.start_block)
            for row in discovered
            if self.config.contract(row.contract).fetch == "topics" and address_stream_key(row.address) not in self.cursors
        ]
        await self.store.save_cursors(cursors)

    async def run_tip(self) -> None:
        """Runs the tip lane forever."""
        while True:
            try:
                behind = await self.tip_step()
            except Exception as exc:
                logger.error(f"Tip lane step failed, retrying in {ERROR_PAUSE_SECONDS:.0f} s: {exc!r}")
                await asyncio.sleep(ERROR_PAUSE_SECONDS)
                continue
            if not behind:
                await asyncio.sleep(self.config.ledger.poll_seconds)

    async def run_backfill(self) -> None:
        """Runs the backfill lane forever, with as many workers as the chain file allows, each on its own streams."""
        await asyncio.gather(*(self._backfill_worker() for _worker in range(self.config.ledger.backfill_workers)))

    async def _backfill_worker(self) -> None:
        while True:
            try:
                worked = await self.backfill_step()
            except Exception as exc:
                logger.error(f"Backfill lane step failed, retrying in {ERROR_PAUSE_SECONDS:.0f} s: {exc!r}")
                await asyncio.sleep(ERROR_PAUSE_SECONDS)
                continue
            if not worked:
                await asyncio.sleep(self.config.ledger.poll_seconds)

