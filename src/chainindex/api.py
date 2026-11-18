"""A public, read-only HTTP API over chainindex's ClickHouse tables: fixed queries only, every input bound and capped.

``chainindex api`` serves, for each chain in ``API_CHAINS``:

- ``GET /v1/chains``: the chains, their contract kinds and the events each kind decodes.
- ``GET /v1/{chain}/status``: how far the ledger and the verifier have got, forks found, audits, block arrival times.
- ``GET /v1/{chain}/events/{contract}/{event}``: decoded events, newest first, from the dbt views (``events_<chain>``),
  by ``address`` and/or a block range. Without an address the range is capped, so no request scans a whole history.

Nobody can send SQL: identifiers come from the chain files and are checked again, values are bound parameters, and
every query runs with a time, row and memory limit on a ClickHouse user that should be read-only (``readonly = 2``).
Each client IP gets a request budget, and only a few queries run at once.
"""

import asyncio
import ipaddress
import re
import time
from datetime import date, datetime
from typing import Any

from aiohttp import web

from chainindex.clickhouse import get_client, safe_database
from chainindex.config import ChainConfig, Settings, load_chain
from chainindex.logger import get_logger

logger = get_logger(__name__)

ADDRESS = re.compile(r"0x[0-9a-f]{40}")
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*")
# A JSON number past 2^53 loses digits in JavaScript, so bigger integers (token amounts) are sent as strings.
MAX_SAFE_INTEGER = 2**53
STATUS_CACHE_SECONDS = 15.0
# Clients kept in the rate limiter's table; past this the idlest are forgotten.
MAX_TRACKED_CLIENTS = 10000


class ApiSettings(Settings):
    """The API's settings on top of the chain service's: which chains it serves and how much one request may cost."""

    API_CHAINS: str = "base"
    # Contract kinds not to serve, comma-separated: your own wallet's kinds, say. Deny them in ClickHouse's grants too.
    API_HIDDEN_CONTRACTS: str = ""
    API_PORT: int = 8080
    API_MAX_ROWS: int = 1000
    API_DEFAULT_ROWS: int = 100
    # The widest block range, in chain time, of a request without an address.
    API_MAX_RANGE_SECONDS: float = 3600.0
    API_QUERY_SECONDS: int = 10
    API_QUERY_MEMORY_BYTES: int = 1 << 30
    API_QUERY_ROWS_READ: int = 100_000_000
    API_CONCURRENT_QUERIES: int = 4
    API_REQUESTS_PER_MINUTE: float = 60.0
    # The header a proxy puts the client's address in (Cloudflare: CF-Connecting-IP), and the proxies' own addresses,
    # comma-separated CIDRs. The header is believed only from those proxies; otherwise the peer address counts.
    API_CLIENT_IP_HEADER: str = ""
    API_TRUSTED_PROXIES: str = ""

    def chains(self) -> list[ChainConfig]:
        """Loads every served chain, without its hidden contract kinds."""
        hidden = {name.strip() for name in self.API_HIDDEN_CONTRACTS.split(",") if name.strip()}
        chains = [load_chain(name.strip()) for name in self.API_CHAINS.split(",") if name.strip()]
        for chain in chains:
            chain.contracts = [contract for contract in chain.contracts if contract.name not in hidden]
        return chains


class ApiError(Exception):
    """A request the API refuses, with the HTTP status and the reason it tells the client."""

    def __init__(self, status: int, reason: str):
        super().__init__(reason)
        self.status = status
        self.reason = reason


class RateLimiter:
    """Gives each client a bucket of requests that refills at a steady rate."""

    def __init__(self, per_minute: float, *, burst: float | None = None):
        self.rate = per_minute / 60.0
        self.burst = burst if burst is not None else max(1.0, per_minute / 6)
        self.buckets: dict[str, tuple[float, float]] = {}

    def allow(self, client: str, now: float | None = None) -> bool:
        """Takes one request from a client's bucket and tells whether there was one to take."""
        now = time.monotonic() if now is None else now
        tokens, last = self.buckets.get(client, (self.burst, now))
        tokens = min(self.burst, tokens + (now - last) * self.rate)
        if len(self.buckets) >= MAX_TRACKED_CLIENTS and client not in self.buckets:
            idlest = sorted(self.buckets, key=lambda key: self.buckets[key][1])[: MAX_TRACKED_CLIENTS // 10]
            for key in idlest:
                del self.buckets[key]
        if tokens < 1:
            self.buckets[client] = (tokens, now)
            return False
        self.buckets[client] = (tokens - 1, now)
        return True


def json_value(value: Any) -> Any:
    """Makes a ClickHouse value safe for JSON: big integers as strings, times as ISO text."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and abs(value) >= MAX_SAFE_INTEGER:
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def block_number(text: str | None, name: str) -> int | None:
    """Parses a block number query parameter."""
    if text is None or text == "":
        return None
    if not (text.isascii() and text.isdigit() and len(text) <= 12):
        raise ApiError(400, f"{name} must be a block number")
    return int(text)


def ip_address(text: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    """Parses an address, reading an IPv4 address a dual-stack socket or proxy wrote as IPv6 (::ffff:a.b.c.d) as IPv4."""
    parsed = ipaddress.ip_address(text)
    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
        return parsed.ipv4_mapped
    return parsed


def client_key(address: str) -> str:
    """Returns the key a client's budget is kept under: its IPv4 address, or its IPv6 /64, which one host can rotate
    through."""
    try:
        parsed = ip_address(address)
    except ValueError:
        return address
    if parsed.version == 6:
        return str(ipaddress.ip_network(f"{parsed}/64", strict=False))
    return str(parsed)


class ChainIndexApi:
    """Answers the API's fixed queries against one ClickHouse."""

    def __init__(self, settings: ApiSettings, *, client: Any = None):
        self.settings = settings
        self.chains = {chain.name: chain for chain in settings.chains()}
        if client is None:
            client = get_client(settings, database="default", autogenerate_session_id=False)
        self.client = client
        self.limiter = RateLimiter(settings.API_REQUESTS_PER_MINUTE)
        self.slots = asyncio.Semaphore(settings.API_CONCURRENT_QUERIES)
        # Clients with a request in flight: one each, so no single client can hold every slot.
        self.busy: set[str] = set()
        self.proxies = [ipaddress.ip_network(cidr.strip()) for cidr in settings.API_TRUSTED_PROXIES.split(",") if cidr.strip()]
        self.status_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.status_lock = asyncio.Lock()

    async def query(self, sql: str, parameters: dict[str, Any]) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Runs one bounded query, a few at a time, and returns its column names and rows."""
        limits = {
            "max_execution_time": self.settings.API_QUERY_SECONDS,
            "max_memory_usage": self.settings.API_QUERY_MEMORY_BYTES,
            "max_result_rows": self.settings.API_MAX_ROWS + 1,
            "result_overflow_mode": "throw",
            "max_rows_to_read": self.settings.API_QUERY_ROWS_READ,
            "read_overflow_mode": "throw",
        }
        try:
            await asyncio.wait_for(self.slots.acquire(), timeout=2.0)
        except TimeoutError as exc:
            raise ApiError(503, "busy, try again shortly") from exc
        try:
            result = await asyncio.to_thread(lambda: self.client.query(sql, parameters=parameters, settings=limits))
        except Exception as exc:
            # The server's message can name tables and settings, so the client hears only that it failed.
            logger.warning(f"API query failed: {exc!r}")
            if "UNKNOWN_TABLE" in str(exc) or "UNKNOWN_DATABASE" in str(exc):
                raise ApiError(404, "the decoded views are not built yet (dbt build)") from exc
            raise ApiError(504, "the query failed or ran out of time; narrow the block range") from exc
        finally:
            self.slots.release()
        return list(result.column_names), list(result.result_rows)

    def chain(self, name: str) -> ChainConfig:
        """Returns a served chain by name."""
        chain = self.chains.get(name)
        if chain is None:
            raise ApiError(404, f"no chain {name!r}; see /v1/chains")
        return chain

    def chains_page(self) -> dict[str, Any]:
        """Describes every served chain and what it decodes."""
        return {
            "chains": [
                {
                    "name": chain.name,
                    "chain_id": chain.chain_id,
                    "block_seconds": chain.block_seconds,
                    "contracts": [
                        {"name": contract.name, "track": contract.track, "events": [event.name for event in contract.event_specs()]}
                        for contract in chain.contracts
                    ],
                }
                for chain in self.chains.values()
            ]
        }

    async def status(self, name: str) -> dict[str, Any]:
        """Summarizes a chain's indexing from its own tables, cached for a few seconds."""
        chain = self.chain(name)
        cached = self.status_cache.get(name)
        if cached is not None and time.monotonic() - cached[0] < STATUS_CACHE_SECONDS:
            return cached[1]
        # One refresh at a time: the requests that waited for it get its page.
        async with self.status_lock:
            cached = self.status_cache.get(name)
            if cached is not None and time.monotonic() - cached[0] < STATUS_CACHE_SECONDS:
                return cached[1]
            page = await self._status_page(chain)
            self.status_cache[name] = (time.monotonic(), page)
            return page

    async def _status_page(self, chain: ChainConfig) -> dict[str, Any]:
        """Builds a chain's status page from its tables."""
        database = safe_database(chain.database)
        _columns, cursors = await self.query(
            f"SELECT stream, from_block, to_block FROM {database}.cursors FINAL "
            "WHERE stream LIKE 'address:%' OR stream LIKE 'topics:%' OR stream = 'verify:blocks'",
            {},
        )
        streams = [(str(stream), int(low), int(high)) for stream, low, high in cursors]
        verified = next(((low, high) for stream, low, high in streams if stream == "verify:blocks"), None)
        ledger = [high for stream, _low, high in streams if stream != "verify:blocks"]
        _columns, orphans = await self.query(
            f"SELECT block_number, detected_by, inserted_at FROM {database}.orphaned_blocks ORDER BY inserted_at DESC LIMIT 5",
            {},
        )
        _columns, orphan_count = await self.query(f"SELECT count() FROM {database}.orphaned_blocks", {})
        _columns, audits = await self.query(
            f"SELECT kind, count(), countIf(missing_logs > 0 OR extra_logs > 0) FROM {database}.audits "
            "WHERE inserted_at > now() - INTERVAL 1 DAY GROUP BY kind ORDER BY kind",
            {},
        )
        _columns, arrivals = await self.query(
            "SELECT count(), quantiles(0.5, 0.95)(dateDiff('millisecond', block_time, received_at) / 1000) "
            f"FROM {database}.blocks WHERE block_time > now() - INTERVAL 1 HOUR AND received_at IS NOT NULL",
            {},
        )
        arrived, quantiles = arrivals[0]
        page = {
            "chain": chain.name,
            "ledger_block": max(ledger, default=None),
            "streams": len(ledger),
            "verified": {"from_block": verified[0], "to_block": verified[1]} if verified else None,
            "orphaned_blocks": int(orphan_count[0][0]),
            "recent_orphans": [
                {"block_number": int(number), "detected_by": str(detected_by), "at": json_value(at)} for number, detected_by, at in orphans
            ],
            "audits_last_day": [{"kind": str(kind), "windows": int(windows), "repaired": int(repaired)} for kind, windows, repaired in audits],
            # Seconds from a block's whole-second stamp to its head reaching the indexer over a websocket.
            "head_arrival_last_hour": {"blocks": int(arrived), "p50_seconds": json_value(quantiles[0]), "p95_seconds": json_value(quantiles[1])}
            if arrived else None,
        }
        return page

    async def events(self, name: str, contract_name: str, event_name: str, request: web.Request) -> dict[str, Any]:
        """Returns decoded events of one kind, newest first, by address and block range."""
        chain = self.chain(name)
        contract = next((item for item in chain.contracts if item.name == contract_name), None)
        if contract is None:
            raise ApiError(404, f"{chain.name} indexes no contract kind {contract_name!r}; see /v1/chains")
        event = next((item for item in contract.event_specs() if item.name == event_name or item.table == event_name), None)
        if event is None:
            raise ApiError(404, f"{contract.name} decodes no event {event_name!r}; see /v1/chains")
        table = f"{contract.name}__{event.table}"
        if not IDENTIFIER.fullmatch(table):
            raise ApiError(404, f"{table} is not a queryable view")
        address = (request.query.get("address") or "").lower() or None
        if address is not None and not ADDRESS.fullmatch(address):
            raise ApiError(400, "address must be 0x and 40 hex digits")
        limit = request.query.get("limit") or str(self.settings.API_DEFAULT_ROWS)
        if not (limit.isascii() and limit.isdigit() and len(limit) <= 6) or not 1 <= int(limit) <= self.settings.API_MAX_ROWS:
            raise ApiError(400, f"limit must be between 1 and {self.settings.API_MAX_ROWS}")
        to_block = block_number(request.query.get("to_block"), "to_block")
        from_block = block_number(request.query.get("from_block"), "from_block")
        if to_block is None:
            ledger_block = (await self.status(chain.name))["ledger_block"]
            if ledger_block is None:
                raise ApiError(503, f"{chain.name} has nothing indexed yet")
            to_block = ledger_block
        max_range = int(self.settings.API_MAX_RANGE_SECONDS / chain.block_seconds)
        if from_block is None:
            from_block = 0 if address is not None else max(0, to_block - max_range + 1)
        if from_block > to_block:
            raise ApiError(400, "from_block is after to_block")
        if address is None and to_block - from_block + 1 > max_range:
            raise ApiError(400, f"without an address a request spans at most {max_range} blocks ({self.settings.API_MAX_RANGE_SECONDS:.0f} s)")
        sql = (
            f"SELECT * FROM {safe_database(chain.decoded_schema)}.{table} "
            "WHERE block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64} "
            + ("AND address = {address:String} " if address is not None else "")
            + "ORDER BY block_number DESC, log_index DESC LIMIT {limit:UInt32}"
        )
        parameters: dict[str, Any] = {"from_block": from_block, "to_block": to_block, "limit": int(limit)}
        if address is not None:
            parameters["address"] = address
        columns, rows = await self.query(sql, parameters)
        return {
            "chain": chain.name,
            "contract": contract.name,
            "event": event.name,
            "signature": event.signature,
            "from_block": from_block,
            "to_block": to_block,
            "columns": columns,
            "rows": [[json_value(value) for value in row] for row in rows],
        }

    def client_of(self, request: web.Request) -> str:
        """Returns the key a request's budget is charged to: the proxy's word for the client, from a trusted proxy only."""
        peer = request.remote or "unknown"
        header = self.settings.API_CLIENT_IP_HEADER
        if header and self.trusted(peer):
            # The proxy appends the address it saw, so the last entry is the one it vouches for.
            forwarded = request.headers.get(header, "").split(",")[-1].strip()
            if forwarded:
                return client_key(forwarded)
        return client_key(peer)

    def trusted(self, peer: str) -> bool:
        """Tells whether a peer is one of the configured proxies."""
        try:
            address = ip_address(peer)
        except ValueError:
            return False
        return any(address in network for network in self.proxies)

    def app(self) -> web.Application:
        """Builds the aiohttp application."""

        @web.middleware
        async def guard(request: web.Request, handler: Any) -> web.StreamResponse:
            client = self.client_of(request)
            response: web.StreamResponse
            if request.path == "/healthz":
                response = await handler(request)
            elif not self.limiter.allow(client) or client in self.busy:
                response = web.json_response({"error": "too many requests"}, status=429, headers={"Retry-After": "10"})
            else:
                self.busy.add(client)
                try:
                    response = await handler(request)
                except ApiError as exc:
                    response = web.json_response({"error": exc.reason}, status=exc.status)
                finally:
                    self.busy.discard(client)
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Cache-Control"] = "public, max-age=5" if response.status == 200 else "no-store"
            return response

        async def index(request: web.Request) -> web.Response:
            return web.json_response(
                {
                    "name": "chainindex",
                    "source": "https://github.com/delia-gbv/chainindex",
                    "endpoints": [
                        "/v1/chains",
                        "/v1/{chain}/status",
                        "/v1/{chain}/events/{contract}/{event}?address=0x...&from_block=&to_block=&limit=",
                    ],
                }
            )

        async def healthz(request: web.Request) -> web.Response:
            return web.Response(text="ok")

        async def chains(request: web.Request) -> web.Response:
            return web.json_response(self.chains_page())

        async def status(request: web.Request) -> web.Response:
            return web.json_response(await self.status(request.match_info["chain"]))

        async def events(request: web.Request) -> web.Response:
            info = request.match_info
            return web.json_response(await self.events(info["chain"], info["contract"], info["event"], request))

        app = web.Application(middlewares=[guard])
        app.add_routes(
            [
                web.get("/", index),
                web.get("/healthz", healthz),
                web.get("/v1/chains", chains),
                web.get("/v1/{chain}/status", status),
                web.get("/v1/{chain}/events/{contract}/{event}", events),
            ]
        )
        return app


async def serve(settings: ApiSettings) -> None:
    """Runs the API until cancelled."""
    api = ChainIndexApi(settings)
    runner = web.AppRunner(api.app())
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", settings.API_PORT).start()
    logger.info(f"chainindex API serving {', '.join(api.chains)} on :{settings.API_PORT}")
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
