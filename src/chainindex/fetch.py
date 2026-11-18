"""``eth_getLogs`` over block ranges: address chunks, adaptive windows, and checks that the node saw the range."""

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from pydantic import BaseModel

from chainindex.logger import get_logger
from chainindex.models import Header, Log
from chainindex.rpc import RpcError, RpcPool, RpcUnavailable, is_batch_refused

logger = get_logger(__name__)

RANGE_ERROR_WORDS = (
    "range", "too many", "limit", "exceed", "response size", "max results", "more than", "timeout", "timed out",
    "too large", "too big",
)
NODE_BEHIND_RETRIES = 5
MAX_CACHED_TIMESTAMPS = 100_000
NODE_BEHIND_WAIT_SECONDS = 1.0
# Requests answered at a shrunk address chunk before one request tries twice as many: a blip is forgotten a few windows
# later, and a real limit costs one refused request per this many.
ADDRESS_CHUNK_PROBE_AFTER = 100


class NodeBehind(Exception):
    """The endpoint's head is below the end of a requested range, so its answer may be incomplete."""


# A full ``eth_getLogs`` topics array: per position, the alternatives it accepts, or None for any.
TopicFilter = list[list[str] | None]


class LogQuery(BaseModel):
    """A log filter: addresses (None for any) and topic0 alternatives (None for any).

    ``filters`` replaces ``topics`` with positional filters, and the query returns the logs any of them matches.
    """

    addresses: list[str] | None = None
    topics: list[str] | None = None
    filters: list[TopicFilter] | None = None

    def topic_filters(self) -> list[TopicFilter | None]:
        """Returns the topics array of every request a window needs: one per filter, or one for topic0."""
        if self.filters is not None:
            return list(self.filters)
        return [[self.topics] if self.topics is not None else None]


def is_range_error(error: RpcError) -> bool:
    """Tells whether a node refused a request for its size rather than its content."""
    message = error.message.lower()
    return error.code == -32005 or any(word in message for word in RANGE_ERROR_WORDS)


def chunks(items: list[str], size: int) -> list[list[str]]:
    """Splits a list into consecutive pieces of at most ``size``."""
    return [items[start : start + size] for start in range(0, len(items), size)]


class LogFetcher:
    """Fetches logs for a lane, learning the largest window and address chunk its provider accepts."""

    def __init__(
        self,
        pool: RpcPool,
        *,
        addresses_per_request: int,
        max_blocks: int,
        known_timestamps: Callable[[list[int]], Awaitable[dict[int, int]]] | None = None,
        headers_pool: RpcPool | None = None,
        headers_per_request: int = 50,
    ):
        self.pool = pool
        # Headers for blocks the logs leave unstamped: from their own endpoint when one is set.
        self.headers_pool = headers_pool or pool
        self.headers_per_request = headers_per_request
        # Where timestamps come from before any header is fetched: live heads, then the stored blocks and logs.
        self.known_timestamps = known_timestamps
        self.addresses_per_request = addresses_per_request
        self.max_addresses_per_request = addresses_per_request
        self.answered_since_shrink = 0
        self.max_blocks = max_blocks
        self.window = max_blocks
        # Block timestamps for nodes that leave them out of logs, least recently used first.
        self.timestamps: OrderedDict[int, int] = OrderedDict()

    async def fetch(self, query: LogQuery, from_block: int, to_block: int) -> list[Log]:
        """Returns every log matching the query in ``[from_block, to_block]``, sorted and deduplicated."""
        if to_block < from_block:
            return []
        if query.addresses is not None and not query.addresses:
            return []
        address_chunks: list[list[str] | None]
        if query.addresses is None:
            address_chunks = [None]
        else:
            address_chunks = list(self._address_chunks(sorted(set(query.addresses))))
        parts = await asyncio.gather(
            *(
                self._fetch_chunk(addresses, topics, from_block, to_block)
                for addresses in address_chunks
                for topics in query.topic_filters()
            )
        )
        unique = {log.key: log for part in parts for log in part}
        logs = sorted(unique.values(), key=lambda log: (log.block_number, log.log_index))
        return await self._with_timestamps(logs)

    async def _fetch_chunk(
        self, addresses: list[str] | None, topics: TopicFilter | None, from_block: int, to_block: int
    ) -> list[Log]:
        logs: list[Log] = []
        start = from_block
        while start <= to_block:
            end = min(to_block, start + self.window - 1)
            logs.extend(await self._fetch_window(addresses, topics, start, end))
            start = end + 1
        return logs

    async def _fetch_window(
        self, addresses: list[str] | None, topics: TopicFilter | None, start: int, end: int
    ) -> list[Log]:
        try:
            logs = await self._get_logs_checked(addresses, topics, start, end)
        except RpcError as error:
            span = end - start + 1
            many_addresses = addresses is not None and len(addresses) > 1
            if span > 1 and (is_range_error(error) or not many_addresses):
                self.window = max(1, span // 2)
                logger.info(f"{self.pool.lane} window shrinks to {self.window} blocks after: {error.message}")
                middle = start + span // 2 - 1
                left = await self._fetch_window(addresses, topics, start, middle)
                return left + await self._fetch_window(addresses, topics, middle + 1, end)
            if many_addresses and addresses is not None:
                half = len(addresses) // 2
                self.addresses_per_request = max(1, min(self.addresses_per_request, half))
                self.answered_since_shrink = 0
                logger.info(f"{self.pool.lane} address chunk shrinks to {self.addresses_per_request} after: {error.message}")
                left = await self._fetch_window(addresses[:half], topics, start, end)
                return left + await self._fetch_window(addresses[half:], topics, start, end)
            raise
        if self.window < self.max_blocks:
            self.window = min(self.max_blocks, self.window * 2)
        if addresses is not None and len(addresses) > self.addresses_per_request:
            # A larger chunk was answered (the probe, or one sent before a shrink): the provider takes that many.
            self.addresses_per_request = len(addresses)
            self.answered_since_shrink = 0
            logger.info(f"{self.pool.lane} address chunk grows back to {self.addresses_per_request}")
        elif addresses is not None and len(addresses) == self.addresses_per_request:
            self.answered_since_shrink += 1
        return logs

    def _address_chunks(self, addresses: list[str]) -> list[list[str]]:
        """Splits addresses into chunks of the current size; once enough have been answered at a shrunk size, the first
        chunk is twice as big, so one transient error ("internal filter error") does not shrink every later window."""
        size = self.addresses_per_request
        if size >= self.max_addresses_per_request or self.answered_since_shrink < ADDRESS_CHUNK_PROBE_AFTER:
            return chunks(addresses, size)
        self.answered_since_shrink = 0
        probe = min(self.max_addresses_per_request, size * 2)
        return [addresses[:probe], *chunks(addresses[probe:], size)]

    async def _get_logs_checked(
        self, addresses: list[str] | None, topics: TopicFilter | None, start: int, end: int
    ) -> list[Log]:
        """Asks for logs and the node's head in one batch, retrying while the node has not reached ``end``."""
        log_filter: dict[str, object] = {"fromBlock": hex(start), "toBlock": hex(end)}
        if addresses is not None:
            log_filter["address"] = addresses
        if topics is not None:
            log_filter["topics"] = topics
        for _attempt in range(NODE_BEHIND_RETRIES):
            logs_answer, head_answer = await self.pool.batch(
                [("eth_getLogs", [log_filter]), ("eth_blockNumber", [])]
            )
            if isinstance(logs_answer, RpcError):
                raise logs_answer
            if isinstance(head_answer, RpcError):
                raise head_answer
            if int(head_answer, 16) >= end:
                return [Log.from_rpc(item) for item in logs_answer if start <= int(item["blockNumber"], 16) <= end]
            await asyncio.sleep(NODE_BEHIND_WAIT_SECONDS)
        raise NodeBehind(f"{self.pool.lane} node stayed below block {end}")

    async def _with_timestamps(self, logs: list[Log]) -> list[Log]:
        """Fills block timestamps for nodes that leave ``blockTimestamp`` out of logs."""
        wanted = sorted({log.block_number for log in logs if log.block_timestamp is None})
        if not wanted:
            return logs
        found = {number: self.timestamps[number] for number in wanted if number in self.timestamps}
        for number in found:
            self.timestamps.move_to_end(number)
        if self.known_timestamps is not None:
            unknown = [number for number in wanted if number not in found]
            if unknown:
                found.update(await self.known_timestamps(unknown))
        missing = [number for number in wanted if number not in found]
        for header in await fetch_headers(self.headers_pool, missing, per_batch=self.headers_per_request):
            found[header.number] = header.timestamp
            self.timestamps[header.number] = header.timestamp
        while len(self.timestamps) > MAX_CACHED_TIMESTAMPS:
            self.timestamps.popitem(last=False)
        return [
            log if log.block_timestamp is not None else log.model_copy(update={"block_timestamp": found[log.block_number]})
            for log in logs
        ]


class MissingHeader(RpcUnavailable):
    """The node answered that it has no such block: pruned history, or a block it has not seen yet."""


async def fetch_headers(pool: RpcPool, numbers: Iterable[int], *, per_batch: int = 50) -> list[Header]:
    """Fetches headers by number in JSON-RPC batches, as many at once as the pool allows, in the order asked."""
    wanted = list(numbers)
    parts = [wanted[start : start + per_batch] for start in range(0, len(wanted), per_batch)]
    answered = await asyncio.gather(*(_header_batch(pool, part) for part in parts))
    headers: list[Header] = []
    for part, answers in zip(parts, answered):
        for number, answer in zip(part, answers):
            if isinstance(answer, RpcError):
                raise RpcUnavailable(f"{pool.lane} lane failed to fetch the header of block {number}: {answer}")
            if answer is None:
                raise MissingHeader(f"{pool.lane} lane has no header for block {number}")
            headers.append(Header.from_rpc(answer))
    return headers


async def _header_batch(pool: RpcPool, numbers: list[int]) -> list[Any]:
    """Fetches one batch of headers, halving it while the provider refuses it as too large."""
    answers = await pool.batch([("eth_getBlockByNumber", [hex(number), False]) for number in numbers])
    refused = bool(answers) and all(isinstance(answer, RpcError) and is_batch_refused(answer.message) for answer in answers)
    if not refused or len(numbers) == 1:
        return answers
    half = len(numbers) // 2
    first, second = await asyncio.gather(_header_batch(pool, numbers[:half]), _header_batch(pool, numbers[half:]))
    return first + second


async def fetch_header(pool: RpcPool, block: int | str) -> Header:
    """Fetches one header by number or tag (``latest``, ``safe``, ``finalized``)."""
    tag = hex(block) if isinstance(block, int) else block
    answer = await pool.request("eth_getBlockByNumber", [tag, False])
    if answer is None:
        raise RpcUnavailable(f"{pool.lane} lane has no header for {tag}")
    return Header.from_rpc(answer)
