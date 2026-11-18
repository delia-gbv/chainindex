"""One chain's service: the ledger, the live stream, the auditor and the registry, each supervised on its own."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from aiohttp import web

from chainindex.audit import Auditor
from chainindex.clickhouse import get_client
from chainindex.config import LANES, Lane, LaneSpec, Settings, endpoint_url
from chainindex.event_transactions import EventTransactions
from chainindex.fetch import LogFetcher
from chainindex.hub import Hub
from chainindex.ledger import Ledger
from chainindex.live import BlockRecorder, LiveChain, SocketFeed
from chainindex.logger import get_logger
from chainindex.metrics import Metrics
from chainindex.models import Log
from chainindex.reads import Reader
from chainindex.registry import Registry, Stream
from chainindex.rpc import RpcPool, Throttle
from chainindex.server import make_app
from chainindex.store import ChainStore
from chainindex.transactions import Transactions
from chainindex.verify import Verifier

logger = get_logger(__name__)

MAX_RESTART_SECONDS = 60.0


async def supervise(name: str, run: Callable[[], Awaitable[None]], metrics: Metrics) -> None:
    """Runs a component forever, restarting it with growing pauses when it fails, so one failure stops nothing else."""
    failures = 0
    while True:
        started = time.monotonic()
        try:
            await run()
            logger.error(f"{name} stopped on its own; restarting it")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"{name} crashed: {exc!r}")
        metrics.component_restarts.labels(component=name).inc()
        if time.monotonic() - started > 600:
            failures = 0
        wait = min(MAX_RESTART_SECONDS, 2.0**failures)
        failures += 1
        await asyncio.sleep(wait)


class ChainService:
    """Builds and runs every component of one chain."""

    def __init__(self, settings: Settings, *, client: Any = None):
        self.settings = settings
        self.config = settings.chain_config()
        self.metrics = Metrics("chainindex")
        throttle = Throttle()
        optional: dict[Lane, LaneSpec | None] = {
            "headers": self.config.rpc.headers, "reads": self.config.rpc.reads, "addresses": self.config.rpc.addresses,
            "transactions": self.config.rpc.transactions,
        }
        budgets = {lane: self.config.rpc.lane(lane) for lane in LANES} | {
            lane: budget for lane, budget in optional.items() if budget is not None
        }
        self.pools = {
            lane: RpcPool(
                lane,
                endpoint_url(self.config.rpc.lane_http(lane), settings.RPC_API_KEY),
                budget,
                # An optional lane on another provider has its own throttles, which pause only that lane.
                # The default endpoint shares the provider's throttles, so one push-back slows every background lane.
                throttle=throttle if lane in LANES or budget.http is None else Throttle(),
                metrics=self.metrics,
            )
            for lane, budget in budgets.items()
        }
        if client is None:
            # One HTTP session per query, so the lanes' threads never collide in a ClickHouse session.
            client = get_client(settings, database="default", autogenerate_session_id=False)
        self.store = ChainStore(client, self.config.database)
        self.registry = Registry(self.config)
        self.hub = Hub(
            ring_blocks=max(1, int(self.config.live.ring_seconds / self.config.block_seconds)),
            queue_size=self.config.live.subscriber_queue,
            metrics=self.metrics,
            ring_max_logs=self.config.live.ring_max_logs,
            pending_max_logs=self.config.live.pending_max_logs,
        )
        self.live = LiveChain(
            self.config, self.registry, self.hub, fetcher=self.fetcher("stream"), pool=self.pools["stream"], metrics=self.metrics
        )
        self.ledger = Ledger(
            self.config,
            self.registry,
            self.store,
            tip_fetcher=self.fetcher("ledger"),
            backfill_fetcher=self.fetcher("backfill"),
            address_fetcher=self.fetcher("addresses") if "addresses" in self.pools else None,
            head_pool=self.pools["ledger"],
            metrics=self.metrics,
            canonical_hash=self.live.canonical_hash,
            on_tip_window=self.cross_check,
            on_unresolved_fork=lambda start, end: self.auditor.flag(start, end),
        )
        self.auditor = Auditor(
            self.config, self.registry, self.store, self.ledger,
            fetcher=self.fetcher("backfill"), pool=self.pools["backfill"], metrics=self.metrics,
        )
        ws_url = endpoint_url(self.config.rpc.ws, settings.RPC_API_KEY) if self.config.rpc.ws else None
        self.sockets = [
            SocketFeed(f"ws-{index}", ws_url, self.live, metrics=self.metrics) for index in range(self.config.live.websockets)
        ] if ws_url else []
        self.reader = Reader(
            self.config, self.registry, self.store, pool=self.pools.get("reads", self.pools["ledger"]), metrics=self.metrics
        )
        # Nonces and balances are read at past blocks: the reads lane when set, since it is the one that keeps state.
        self.transactions = Transactions(
            self.config, self.registry, self.store, self.ledger, pool=self.pools.get("reads", self.pools["backfill"])
        )
        # The swappers' transactions, paced by their own lane where the chain file sets one, so a long backfill of
        # them never slows the ledger.
        self.event_transactions = EventTransactions(
            self.config, self.registry, self.store, self.ledger,
            pool=self.pools.get("transactions", self.pools["backfill"]), metrics=self.metrics,
        )
        # Every stored block's hash is checked once against the headers lane where the chain file names one, since it
        # answers the biggest header batches.
        self.verifier = Verifier(
            self.config, self.store, self.ledger, self.auditor,
            pool=self.pools.get("headers", self.pools["backfill"]), metrics=self.metrics,
            per_request=self.config.rpc.headers_per_request,
        )
        # Skipped headers come from the headers lane where the chain file names one: an official RPC that throttles each
        # item of a header batch would otherwise leave the recorder retrying the same gap for hours.
        headers: Lane = "headers" if "headers" in self.pools else "backfill"
        self.recorder = BlockRecorder(
            self.live, self.store, self.pools[headers], confirmations=self.config.ledger.confirmations,
            per_request=self.config.rpc.headers_per_request if headers == "headers" else 50,
            # A headers lane fills a day's gap in minutes (500 headers a request), so a restart resumes instead of
            # leaving the gap. Without one only the live ring's gap is filled.
            max_gap=int(86400 / self.config.block_seconds) if headers == "headers" else None,
        )

    def fetcher(self, lane: Lane) -> LogFetcher:
        """Builds a log fetcher on one lane's pool."""
        return LogFetcher(
            self.pools[lane],
            addresses_per_request=self.config.rpc.lane_addresses_per_request(lane),
            max_blocks=self.config.rpc.max_blocks_per_request,
            known_timestamps=self.known_timestamps,
            headers_pool=self.pools.get("headers"),
            headers_per_request=self.config.rpc.headers_per_request,
        )

    async def known_timestamps(self, numbers: list[int]) -> dict[int, int]:
        """Finds block timestamps without asking the node: the live chain's heads, then what the database stored."""
        found = {number: header.timestamp for number in numbers if (header := self.live.headers.get(number))}
        unknown = [number for number in numbers if number not in found]
        if unknown:
            found.update(await self.store.block_times(unknown))
        return found

    def cross_check(self, streams: list[Stream], start: int, end: int, logs: list[Log]) -> None:
        """Compares a ledger tip window with what the stream sealed for the same blocks; a mismatch is audited."""
        sealed = self.hub.sealed_keys(start, end, self.registry.addresses_of(streams))
        if sealed is None:
            return
        written = {log.key for log in logs}
        if sealed == written:
            return
        self.metrics.cross_check_mismatches.inc()
        logger.warning(
            f"Blocks {start}-{end}: the ledger has {len(written - sealed)} log(s) the stream did not seal and the "
            f"stream sealed {len(sealed - written)} the ledger did not fetch; auditing the range"
        )
        self.auditor.flag(start, end)

    async def prepare(self) -> None:
        """Creates the tables, reconciles the registry with the chain file, and loads the cursors."""
        await asyncio.to_thread(self.store.create)
        changes = self.registry.reconcile(await self.store.contracts())
        await self.store.record_contracts(changes)
        if changes:
            logger.info(f"Registry: {len(changes)} change(s) from the chain file recorded")
        await self.ledger.load()

    async def refresh_registry(self) -> None:
        """Picks up ``chainindex contracts add`` changes without a restart.

        A read ClickHouse refuses is tried again on the next refresh, as every other loop retries its own step, so
        ``component_restarts_total`` counts crashes, not ClickHouse blips."""
        while True:
            await asyncio.sleep(self.settings.REGISTRY_REFRESH_SECONDS)
            version = self.registry.version
            try:
                rows = await self.store.contracts()
            except Exception as exc:
                logger.error(f"Registry refresh failed, retrying in {self.settings.REGISTRY_REFRESH_SECONDS:.0f} s: {exc!r}")
                continue
            self.registry.load(rows)
            if self.registry.version != version:
                logger.info(f"Registry changed: {len(self.registry.active())} active address(es)")

    def status(self) -> dict[str, Any]:
        """Summarizes the service for ``/status``."""
        head = self.live.head
        return {
            "chain": self.config.name,
            "head": head.number if head is not None else None,
            "sealed": self.live.sealed,
            "ledger_confirmed": self.ledger.confirmed_block,
            "ledger_tip": self.ledger.tip_block,
            "verified": [self.verifier.cursor.from_block, self.verifier.cursor.to_block] if self.verifier.cursor else None,
            "streams": len(self.registry.streams()),
            "addresses": len(self.registry.active()),
            "unrecorded_children": len(self.registry.unrecorded),
            "subscribers": len(self.hub.subscribers),
            "ring_start": self.hub.ring_start,
        }

    def ready(self) -> bool:
        """Tells whether the stream has warmed its ring, so resuming subscribers can be served."""
        return not self.config.live.enabled or not self.config.live.logs or self.hub.sealed is not None

    async def run(self) -> None:
        """Runs every component until cancelled."""
        await self.prepare()
        self.metrics.serve(self.settings.METRICS_PORT)
        runner = web.AppRunner(make_app(self.hub, status=self.status, ready=self.ready))
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", self.config.live.port).start()
        components: dict[str, Callable[[], Awaitable[None]]] = {
            "ledger-tip": self.ledger.run_tip,
            "ledger-backfill": self.ledger.run_backfill,
            "auditor": self.auditor.run,
            "registry": self.refresh_registry,
        }
        if self.config.verify.enabled:
            components["verifier"] = self.verifier.run
        if self.config.reads:
            components["reads"] = self.reader.run
        if self.config.accounts:
            components["transactions"] = self.transactions.run
        if self.event_transactions.kinds:
            components["event-transactions"] = self.event_transactions.run
        if self.config.live.enabled:
            if self.config.live.logs:
                components["live-seal"] = self.live.run_seal
            components.update({socket.name: socket.run for socket in self.sockets})
            components["blocks"] = self.recorder.run
        logger.info(
            f"chainindex {self.config.name} (chain {self.config.chain_id}) running: {', '.join(components)}; "
            f"{len(self.registry.active())} address(es) in {len(self.registry.streams())} stream(s)"
        )
        try:
            await asyncio.gather(*(supervise(name, run, self.metrics) for name, run in components.items()))
        finally:
            await runner.cleanup()
            for pool in self.pools.values():
                await pool.close()
