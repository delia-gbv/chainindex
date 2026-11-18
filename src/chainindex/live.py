"""The live chain: websocket heads and logs first, every block sealed over HTTP, reorgs resolved against HTTP.

Logs reach subscribers the moment the first websocket delivers them. The seal then fetches each block range over
HTTP with the same filters, publishes whatever the sockets missed, and declares the range complete. A head that
does not extend the chain is checked against an HTTP node before it is believed.
"""

import asyncio
import json
import time
from typing import Any

from websockets.asyncio.client import ClientConnection, connect

from chainindex.collect import address_groups, collect
from chainindex.config import ChainConfig
from chainindex.events import HeadEvent, LogEvent
from chainindex.fetch import LogFetcher, LogQuery, chunks, fetch_header, fetch_headers
from chainindex.hub import Hub
from chainindex.logger import get_logger
from chainindex.metrics import Metrics
from chainindex.models import Header, Log
from chainindex.registry import Registry
from chainindex.rpc import RpcPool, RpcUnavailable
from chainindex.store import ChainStore

logger = get_logger(__name__)

MAX_REORG_DEPTH = 256
ERROR_PAUSE_SECONDS = 2.0
RECONNECT_SECONDS = (1.0, 2.0, 4.0, 8.0, 15.0, 30.0)


class LiveChain:
    """Tracks the canonical tip, publishes logs as they arrive, and seals block ranges over HTTP."""

    def __init__(
        self,
        config: ChainConfig,
        registry: Registry,
        hub: Hub,
        *,
        fetcher: LogFetcher,
        pool: RpcPool,
        metrics: Metrics,
    ):
        self.config = config
        self.registry = registry
        self.hub = hub
        self.fetcher = fetcher
        self.pool = pool
        self.metrics = metrics
        self.headers: dict[int, Header] = {}
        # When each block's head first reached us over a websocket, by (number, hash): our clock, unlike the header's
        # whole-second stamp, which about ten blocks of a 0.1 s chain share.
        self.arrivals: dict[tuple[int, str], float] = {}
        self.head: Header | None = None
        self.sealed: int | None = None
        self.last_head_at = 0.0
        self.lock = asyncio.Lock()
        self.ring_blocks = max(1, int(config.live.ring_seconds / config.block_seconds))
        self.background: set[asyncio.Task[None]] = set()

    def canonical_hash(self, block_number: int) -> str | None:
        """Returns the hash of a recent canonical block, or None when it is not in memory."""
        header = self.headers.get(block_number)
        return header.hash if header is not None else None

    def _set_head(self, header: Header) -> None:
        self.headers[header.number] = header
        self.head = header
        self.last_head_at = time.monotonic()
        oldest = header.number - self.ring_blocks
        for number in [number for number in self.headers if number < oldest]:
            del self.headers[number]
        for key in [key for key in self.arrivals if key[0] < oldest]:
            del self.arrivals[key]
        self.metrics.live_head_block.set(header.number)
        self.metrics.live_last_head.set(time.time())
        self.hub.publish_head(
            HeadEvent(block_number=header.number, block_hash=header.hash, parent_hash=header.parent_hash, timestamp=header.timestamp)
        )

    def on_head(self, header: Header, socket: str, received_at: float | None = None) -> None:
        """Takes a head from any source: extends the chain at once, or checks a fork against HTTP in the background.

        ``received_at`` is when a websocket delivered it; the first socket's time is kept."""
        if received_at is not None and (header.number, header.hash) not in self.arrivals:
            self.arrivals[(header.number, header.hash)] = received_at
            self.metrics.live_head_arrival_seconds.observe(received_at - header.timestamp)
        if self.headers.get(header.number) == header:
            return
        tip = self.head
        if tip is not None and header.number < tip.number and header.number not in self.headers:
            # A block a jump skipped, arriving late from another socket: it fills the gap and moves nothing.
            child, parent = self.headers.get(header.number + 1), self.headers.get(header.number - 1)
            if (child is None or child.parent_hash == header.hash) and (parent is None or parent.hash == header.parent_hash):
                self.headers[header.number] = header
                return
        extends = tip is not None and header.number == tip.number + 1 and header.parent_hash == tip.hash
        jumps = tip is not None and header.number > tip.number + 1
        if tip is None or extends or jumps:
            if jumps and tip is not None:
                logger.info(f"Heads jumped {tip.number} -> {header.number} on {socket}; the seal fetches the gap")
            self._set_head(header)
            return
        task = asyncio.create_task(self._check_fork(header))
        self.background.add(task)
        task.add_done_callback(self.background.discard)

    async def _check_fork(self, header: Header) -> None:
        async with self.lock:
            if self.headers.get(header.number) == header:
                return
            try:
                canonical = await fetch_header(self.pool, header.number)
            except RpcUnavailable as exc:
                # The HTTP node has not seen the block yet: the next heads settle which chain is canonical.
                logger.info(f"Ignoring head {header.number} {header.hash[:10]}: {exc}")
                return
            if canonical.hash != header.hash:
                logger.info(f"Ignoring head {header.number} {header.hash[:10]}: HTTP says {canonical.hash[:10]}")
                return
            await self._reorg_to(canonical)

    async def _reorg_to(self, header: Header) -> None:
        """Rolls the chain back to where ``header``'s ancestry meets it and makes ``header`` the head."""
        branch = [header]
        cursor = header
        for _depth in range(MAX_REORG_DEPTH):
            known = self.headers.get(cursor.number - 1)
            if known is None or known.hash == cursor.parent_hash:
                break
            cursor = await fetch_header(self.pool, cursor.number - 1)
            branch.append(cursor)
        else:
            logger.error(f"Reorg deeper than {MAX_REORG_DEPTH} blocks below {header.number}; rolling back that far")
        fork_point = cursor.number - 1
        for number in [number for number in self.headers if number > fork_point]:
            del self.headers[number]
        depth = (self.head.number if self.head is not None else fork_point) - fork_point
        if self.sealed is not None and self.sealed > fork_point:
            self.sealed = fork_point
        self.metrics.live_reorgs.inc()
        logger.warning(f"Reorg: rolled back {depth} block(s) to {fork_point}, new head {header.number} {header.hash[:10]}")
        self.hub.reorg(fork_point)
        for item in reversed(branch):
            self._set_head(item)

    def contract_of(self, log: Log) -> str | None:
        """Returns the kind a live log belongs to, applying topic streams' membership and start blocks."""
        contract = self.registry.contract_of(log.address)
        if contract is None:
            return None
        if log.block_number < self.registry.members(contract).get(log.address, 0):
            return None
        return contract

    def to_event(self, log: Log, contract: str, *, sealed: bool) -> LogEvent:
        """Decodes a log into a stream event; logs of events the ABI lacks keep empty args."""
        decoded = self.registry.decoder.decode(contract, log)
        return LogEvent(
            block_number=log.block_number, block_hash=log.block_hash, block_timestamp=log.block_timestamp,
            transaction_hash=log.transaction_hash, transaction_index=log.transaction_index, log_index=log.log_index,
            address=log.address, contract=contract, event=decoded.event if decoded is not None else "",
            args=decoded.args if decoded is not None else {}, topics=list(log.topics), data=log.data, sealed=sealed,
        )

    def on_log(self, log: Log, socket: str) -> None:
        """Publishes a websocket log at once unless it is a duplicate, already sealed, or from another fork."""
        if log.removed:
            # A removed log means a fork. The head that caused it arrives on the head subscription.
            return
        if self.sealed is not None and log.block_number <= self.sealed:
            return
        if log.key in self.hub.pending:
            return
        known = self.headers.get(log.block_number)
        if known is not None and known.hash != log.block_hash:
            return
        contract = self.contract_of(log)
        if contract is None:
            return
        self.hub.publish_log(self.to_event(log, contract, sealed=False))
        self.metrics.live_ws_first.labels(socket=socket).inc()
        if log.block_timestamp is not None:
            self.metrics.live_publish_seconds.observe(max(0.0, time.time() - log.block_timestamp))

    async def _register_children(self, logs: list[Log]) -> None:
        discovered = self.registry.discover(logs)
        if discovered:
            self.registry.apply(discovered, recorded=False)

    async def seal_step(self) -> bool:
        """Seals the next range up to the head and returns whether more is left."""
        stale_after = max(5.0, 3 * self.config.block_seconds)
        if self.head is None or time.monotonic() - self.last_head_at > stale_after:
            # No websocket head lately: poll HTTP so the stream keeps moving.
            self.on_head(await fetch_header(self.pool, "latest"), "http")
        async with self.lock:
            if self.head is None:
                return False
            target = self.head.number
            start = self.sealed + 1 if self.sealed is not None else max(0, target - self.ring_blocks + 1)
            if target < start:
                return False
            end = min(target, start + self.config.rpc.max_blocks_per_request - 1)
            logs = await collect(
                self.registry, self.registry.streams(), start, end, self.fetcher, on_children=self._register_children
            )
            forked = [log for log in logs if (known := self.canonical_hash(log.block_number)) and known != log.block_hash]
            if forked:
                canonical = await fetch_header(self.pool, forked[0].block_number)
                if canonical.hash == forked[0].block_hash:
                    await self._reorg_to(canonical)
                # Either way the range is sealed on a later step, once the chain and the node agree.
                return False
            end_header = self.headers.get(end) or await fetch_header(self.pool, end)
            events = []
            for log in logs:
                contract = self.contract_of(log)
                if contract is not None:
                    events.append(self.to_event(log, contract, sealed=True))
            late = [event for event in events if event.key not in self.hub.pending]
            warming = self.sealed is None
            for event in late:
                self.hub.publish_log(event)
            if late and not warming:
                self.metrics.live_sealed_late.inc(len(late))
                logger.info(f"Seal {start}-{end} found {len(late)} log(s) no websocket delivered")
            self.hub.seal(start, end, end_header.hash, events)
            self.sealed = end
            self.metrics.live_sealed_block.set(end)
            return end < target

    async def run_seal(self) -> None:
        """Seals forever."""
        while True:
            try:
                behind = await self.seal_step()
            except Exception as exc:
                logger.error(f"Seal step failed, retrying in {ERROR_PAUSE_SECONDS:.0f} s: {exc!r}")
                await asyncio.sleep(ERROR_PAUSE_SECONDS)
                continue
            if not behind:
                await asyncio.sleep(self.config.live.seal_seconds)


class SocketFeed:
    """One websocket subscribed to heads and the registry's logs, reconnecting forever."""

    def __init__(self, name: str, url: str, live: LiveChain, *, metrics: Metrics):
        self.name = name
        self.url = url
        self.live = live
        self.metrics = metrics
        self.next_id = 0
        self.head_subscription: str | None = None
        self.log_subscriptions: set[str] = set()
        self.registry_version = -1
        # Connections in a row that dropped before subscribing. It sets the wait before the next one.
        self.failures = 0
        self.stale_seconds = max(15.0, 10 * live.config.block_seconds)

    async def run(self) -> None:
        """Keeps the socket connected and subscribed."""
        while True:
            try:
                async with connect(self.url, ping_interval=20, ping_timeout=20, max_size=2**24, open_timeout=15) as socket:
                    await self._serve(socket)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics.live_ws_connected.labels(socket=self.name).set(0)
                self.metrics.live_ws_reconnects.labels(socket=self.name).inc()
                wait = RECONNECT_SECONDS[min(self.failures, len(RECONNECT_SECONDS) - 1)]
                self.failures += 1
                logger.warning(f"Websocket {self.name} dropped ({type(exc).__name__}: {exc}); reconnecting in {wait:.0f} s")
                await asyncio.sleep(wait)

    async def _call(self, socket: ClientConnection, method: str, params: list[Any]) -> Any:
        """Sends a request and reads until its answer, handling notifications that arrive meanwhile."""
        self.next_id += 1
        request_id = self.next_id
        await socket.send(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}))
        while True:
            message = json.loads(await asyncio.wait_for(socket.recv(), self.stale_seconds))
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(f"{method} refused: {message['error']}")
                return message.get("result")
            self._dispatch(message)

    def _log_filters(self) -> list[dict[str, Any]]:
        streams = self.live.registry.streams()
        filters: list[dict[str, Any]] = [{"topics": [stream.topics]} for stream in streams if stream.addresses is None]
        for first, addresses in address_groups(streams):
            query = LogQuery(topics=first.topics, filters=first.filters)
            for part in chunks(sorted(addresses), self.live.config.rpc.ws_addresses_per_subscription):
                for topics in query.topic_filters():
                    log_filter: dict[str, Any] = {"address": part}
                    if topics is not None:
                        log_filter["topics"] = topics
                    filters.append(log_filter)
        return filters

    async def _subscribe_logs(self, socket: ClientConnection) -> None:
        for subscription in sorted(self.log_subscriptions):
            await self._call(socket, "eth_unsubscribe", [subscription])
        self.log_subscriptions = set()
        self.registry_version = self.live.registry.version
        for log_filter in self._log_filters():
            self.log_subscriptions.add(await self._call(socket, "eth_subscribe", ["logs", log_filter]))

    async def _serve(self, socket: ClientConnection) -> None:
        self.log_subscriptions = set()
        self.head_subscription = await self._call(socket, "eth_subscribe", ["newHeads"])
        if self.live.config.live.logs:
            await self._subscribe_logs(socket)
        self.metrics.live_ws_connected.labels(socket=self.name).set(1)
        # A socket that subscribed was healthy, so its drop starts the backoff over instead of waiting the longest.
        self.failures = 0
        logger.info(f"Websocket {self.name} subscribed: heads and {len(self.log_subscriptions)} log filter(s)")
        while True:
            if self.live.config.live.logs and self.live.registry.version != self.registry_version:
                await self._subscribe_logs(socket)
            message = json.loads(await asyncio.wait_for(socket.recv(), self.stale_seconds))
            self._dispatch(message)

    def _dispatch(self, message: dict[str, Any]) -> None:
        if message.get("method") != "eth_subscription":
            return
        params = message.get("params") or {}
        result = params.get("result")
        if not result:
            # PublicNode sends an empty notification every hour or so.
            return
        subscription = params.get("subscription")
        if subscription == self.head_subscription:
            self.live.on_head(Header.from_rpc(result), self.name, received_at=time.time())
        elif subscription in self.log_subscriptions:
            self.live.on_log(Log.from_rpc(result), self.name)


class BlockRecorder:
    """Writes the headers of confirmed blocks to ``blocks``, filling what the feeds skipped over HTTP."""

    def __init__(
        self, live: LiveChain, store: ChainStore, pool: RpcPool, *, confirmations: int, per_request: int = 50, max_gap: int | None = None
    ):
        self.live = live
        self.store = store
        self.pool = pool
        self.confirmations = confirmations
        self.per_request = per_request
        self.recorded: Header | None = None
        # A gap after the stored headers longer than this is left, and recording restarts at the tip.
        self.max_gap = max_gap if max_gap is not None else live.ring_blocks
        # The last stamped second's headers of the previous step: its blocks may not all be in yet. None until the
        # first step, whose first second may have started before it.
        self.open_second: list[Header] | None = None

    async def step(self) -> None:
        """Records the next confirmed headers."""
        if self.live.head is None:
            return
        confirmed = self.live.head.number - self.confirmations
        if self.recorded is None:
            stored = await self.store.max_block()
            start = stored + 1 if stored is not None and confirmed - stored <= self.max_gap else confirmed
            if stored is not None and start == confirmed:
                logger.warning(f"blocks has a gap after {stored}: headers are only recorded from {confirmed} on")
        else:
            start = self.recorded.number + 1
        end = min(confirmed, start + 2000)
        if end < start:
            return
        headers = {number: header for number in range(start, end + 1) if (header := self.live.headers.get(number))}
        missing = [number for number in range(start, end + 1) if number not in headers]
        for header in await fetch_headers(self.pool, missing, per_batch=self.per_request):
            headers[header.number] = header
        ordered = [headers[number] for number in range(start, end + 1)]
        previous = self.recorded
        for header in ordered:
            if previous is not None and header.parent_hash != previous.hash:
                # Memory and HTTP disagree across a fork: take the whole range from HTTP.
                ordered = await fetch_headers(self.pool, range(start, end + 1), per_batch=self.per_request)
                break
            previous = header
        await self.store.insert_blocks(ordered, self.live.arrivals)
        self.recorded = ordered[-1]
        self.observe_latency(ordered)

    def observe_latency(self, ordered: list[Header]) -> None:
        """Observes each arrived block's latency from its estimated time, once every block sharing its stamp is known.

        A stamp is a whole second (about ten blocks of a 0.1 s chain share one), so a block's time is estimated as its stamp
        plus its place among them: the n-th of k at stamp + n / k."""
        seconds: dict[int, list[Header]] = {}
        for header in (self.open_second or []) + ordered:
            seconds.setdefault(header.timestamp, []).append(header)
        stamps = sorted(seconds)
        complete = stamps[:-1] if self.open_second is not None else stamps[1:-1]
        for stamp in complete:
            blocks = seconds[stamp]
            for place, header in enumerate(blocks):
                arrived = self.live.arrivals.get((header.number, header.hash))
                if arrived is not None:
                    self.live.metrics.live_head_latency_seconds.observe(arrived - (stamp + place / len(blocks)))
        self.open_second = seconds[stamps[-1]] if stamps else []

    async def run(self) -> None:
        """Records headers forever."""
        while True:
            try:
                await self.step()
            except Exception as exc:
                logger.error(f"Block recorder step failed: {exc!r}")
            await asyncio.sleep(max(5.0, 5 * self.live.config.block_seconds))
