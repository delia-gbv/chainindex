"""JSON-RPC over HTTP: one pool per lane, paced, resting its endpoint after failures, sharing one throttle per process."""

import asyncio
import itertools
import time
from typing import Any

import aiohttp

from chainindex.config import Lane, LaneSpec
from chainindex.logger import get_logger
from chainindex.metrics import Metrics

logger = get_logger(__name__)

# Not "exceeded": "query limit exceeded" is a range error, not a throttle.
RATE_LIMIT_WORDS = ("rate limit", "rate-limit", "too many requests", "throttl")
# A brief throttle is waited out: PublicNode limits by source IP.
THROTTLE_WAIT_SECONDS = (0.5, 1.0, 2.0, 4.0, 8.0)
# A bot wall (Cloudflare's "Just a moment..." page in front of some public RPCs) blocks the caller for minutes, so every
# background lane stops this long instead of retrying into it.
CHALLENGE_WAIT_SECONDS = 60.0
# How long a failed endpoint rests, doubling per consecutive failure.
REST_SECONDS = 2.0
MAX_REST_SECONDS = 60.0
# A kept-alive connection the node closed just as a request went out on it fails with one of these. The node is fine,
# so the request goes out again at once on a new connection instead of resting the lane. A refused connection is
# ClientConnectorError, an outage, and still rests it.
DROPPED_CONNECTION = ("ClientOSError", "ServerDisconnectedError")


class RpcError(Exception):
    """The node answered a request with a JSON-RPC error."""

    def __init__(self, code: int, message: str, method: str):
        super().__init__(f"{method} failed with {code}: {message}")
        self.code = code
        self.message = message
        self.method = method


class RpcUnavailable(Exception):
    """No endpoint of a lane answered a request after every retry."""


def is_challenge(status: int, text: str) -> bool:
    """Tells whether a refusal is a bot wall's HTML page, which blocks the caller rather than the request."""
    return status == 403 and "<html" in text[:500].lower()


def is_batch_refused(message: str) -> bool:
    """Tells whether an error refuses a JSON-RPC batch for its size (HyperEVM's PublicNode: "Exceeded max limit of 20")."""
    lowered = message.lower()
    return "batch" in lowered and ("too large" in lowered or "limit" in lowered)


def is_rate_limited(status: int, message: str) -> bool:
    """Tells whether a response means the caller is being throttled."""
    lowered = message.lower()
    return status == 429 or any(word in lowered for word in RATE_LIMIT_WORDS)


class Throttle:
    """Process-wide throttle state: when a provider pushes back, every lane but the stream slows down."""

    def __init__(self) -> None:
        self.paused_until = 0.0

    def hit(self, wait_seconds: float) -> None:
        """Records a throttle response and pauses the background lanes for at least ``wait_seconds``."""
        self.paused_until = max(self.paused_until, time.monotonic() + wait_seconds)

    async def wait(self, lane: Lane) -> None:
        """Waits out a pause, except on the stream lane, which bots depend on."""
        if lane == "stream":
            return
        delay = self.paused_until - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)


class Pacer:
    """Spaces requests to a rate, allowing a burst of one second's worth."""

    def __init__(self, requests_per_second: float):
        self.interval = 1.0 / requests_per_second
        self.burst = max(1.0, requests_per_second)
        self.tokens = self.burst
        self.updated = time.monotonic()
        self.lock = asyncio.Lock()

    async def take(self) -> None:
        """Waits until a request may go out."""
        async with self.lock:
            now = time.monotonic()
            self.tokens = min(self.burst, self.tokens + (now - self.updated) / self.interval)
            self.updated = now
            if self.tokens < 1:
                await asyncio.sleep((1 - self.tokens) * self.interval)
                self.updated = time.monotonic()
                self.tokens = 1
            self.tokens -= 1


class RpcPool:
    """Sends a lane's requests to its endpoint, resting it after transport errors."""

    def __init__(
        self,
        lane: Lane,
        url: str,
        budget: LaneSpec,
        *,
        throttle: Throttle,
        metrics: Metrics,
        timeout_seconds: float = 30.0,
        max_attempts: int = 8,
    ):
        self.lane = lane
        # The URL carries the key, so logs and metrics name the lane instead.
        self.url = url
        self.failures = 0
        self.resting_until = 0.0
        self.pacer = Pacer(budget.requests_per_second)
        self.concurrency = asyncio.Semaphore(budget.max_concurrency)
        self.throttle = throttle
        self.metrics = metrics
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.max_attempts = max_attempts
        self.ids = itertools.count(1)
        self.session: aiohttp.ClientSession | None = None

    async def close(self) -> None:
        """Closes the HTTP session."""
        if self.session is not None:
            await self.session.close()
            self.session = None

    async def request(self, method: str, params: list[Any]) -> Any:
        """Sends one call and returns its result, raising RpcError when the node answers with an error."""
        (answer,) = await self._send([(method, params)], label=method)
        if isinstance(answer, RpcError):
            raise answer
        return answer

    async def batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        """Sends calls as one JSON-RPC batch; failed calls come back as RpcError values."""
        if not calls:
            return []
        label = calls[0][0] if len({method for method, _ in calls}) == 1 else "batch"
        return await self._send(calls, label=label)

    async def block_number(self) -> int:
        """Returns the endpoint's latest block number."""
        return int(await self.request("eth_blockNumber", []), 16)

    def _rest(self) -> float:
        """Benches the endpoint after a failure and returns for how long."""
        now = time.monotonic()
        if self.resting_until > now:
            # Another request in flight already failed and rested the lane: this is the same outage, so it does not
            # double the rest. Otherwise one blip under four in-flight requests would rest the lane 16 s, not 2.
            return self.resting_until - now
        self.failures += 1
        seconds = min(MAX_REST_SECONDS, REST_SECONDS * 2 ** (self.failures - 1))
        self.resting_until = time.monotonic() + seconds
        return seconds

    async def _send(self, calls: list[tuple[str, list[Any]]], *, label: str) -> list[Any]:
        throttled = 0
        redialed = False
        last_error = ""
        for _attempt in range(self.max_attempts):
            wait = self.resting_until - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            await self.throttle.wait(self.lane)
            await self.pacer.take()
            started = time.monotonic()
            async with self.concurrency:
                outcome, payload = await self._post(calls)
            self.metrics.rpc_seconds.labels(lane=self.lane, method=label).observe(time.monotonic() - started)
            self.metrics.rpc_requests.labels(lane=self.lane, method=label, outcome=outcome).inc()
            if outcome == "ok":
                self.failures = 0
                return payload
            if outcome == "throttled":
                wait_seconds = payload or THROTTLE_WAIT_SECONDS[min(throttled, len(THROTTLE_WAIT_SECONDS) - 1)]
                throttled += 1
                self.throttle.hit(wait_seconds)
                logger.info(f"The {self.lane} lane was throttled on {label}; retrying in {wait_seconds:.1f} s")
                await asyncio.sleep(wait_seconds)
                continue
            last_error = payload
            if last_error in DROPPED_CONNECTION and not redialed:
                redialed = True
                logger.info(f"The {self.lane} lane's connection dropped on {label} ({last_error}); retrying on a new one")
                continue
            rest = self._rest()
            logger.warning(f"The {self.lane} lane failed {label} ({last_error}); resting it {rest:.0f} s")
        raise RpcUnavailable(f"{self.lane} lane gave up on {label} after {self.max_attempts} attempts: {last_error}")

    async def _post(self, calls: list[tuple[str, list[Any]]]) -> tuple[str, Any]:
        """Posts one request and classifies it as ok, throttled (payload: wait seconds) or failed (payload: reason)."""
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=self.timeout)
        ids = [next(self.ids) for _ in calls]
        body: Any = [
            {"jsonrpc": "2.0", "id": call_id, "method": method, "params": params}
            for call_id, (method, params) in zip(ids, calls)
        ]
        if len(body) == 1:
            body = body[0]
        try:
            async with self.session.post(self.url, json=body) as response:
                text = await response.text()
                if is_rate_limited(response.status, text if response.status >= 400 else ""):
                    retry_after = response.headers.get("Retry-After", "")
                    return "throttled", float(retry_after) if retry_after.isdigit() else None
                if is_challenge(response.status, text):
                    return "throttled", CHALLENGE_WAIT_SECONDS
                if response.status in (400, 403, 413):
                    # A refusal of the request itself (PublicNode's "Request blocked" past 9 addresses) is an answer,
                    # so the caller can make the request smaller, not an outage to rest the endpoint for.
                    refusal = RpcError(-response.status, f"HTTP {response.status}: {text[:200]}", calls[0][0])
                    return "ok", [refusal for _call in calls]
                if response.status >= 400:
                    return "failed", f"HTTP {response.status}"
                answers = await response.json(content_type=None)
        except (TimeoutError, aiohttp.ClientError, ValueError) as exc:
            return "failed", type(exc).__name__
        if isinstance(answers, dict) and answers.get("id") is None and "error" in answers:
            # Some providers refuse a whole request with one error that answers no call.
            message = str(answers["error"].get("message", ""))
            if is_batch_refused(message):
                # A batch refused for its size is an answer too: the caller sends smaller ones, and the lane keeps going.
                refusal = RpcError(int(answers["error"].get("code", 0)), message, calls[0][0])
                return "ok", [refusal for _call in calls]
            return ("throttled", None) if is_rate_limited(0, message) else ("failed", message)
        if isinstance(answers, dict):
            answers = [answers]
        by_id = {answer.get("id"): answer for answer in answers if isinstance(answer, dict)}
        results = []
        for call_id, (method, _params) in zip(ids, calls):
            answer = by_id.get(call_id)
            if answer is None:
                return "failed", f"no answer for {method}"
            error = answer.get("error")
            if error is None:
                results.append(answer.get("result"))
                continue
            message = str(error.get("message", ""))
            if is_rate_limited(0, message):
                return "throttled", None
            results.append(RpcError(int(error.get("code", 0)), message, method))
        return "ok", results
