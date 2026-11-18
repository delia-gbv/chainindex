"""LogFetcher against an in-memory node serving real Base logs and synthetic ones."""

import asyncio
import json
import math
from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path

import pytest

from chainindex import fetch
from chainindex.fetch import (
    NODE_BEHIND_RETRIES,
    LogFetcher,
    LogQuery,
    NodeBehind,
    chunks,
    fetch_headers,
    is_range_error,
)
from chainindex.models import Log
from chainindex.rpc import RpcError

BASE_LOGS: list[dict] = json.loads((Path(__file__).parent / "fixtures" / "base_logs.json").read_text())
BASE_ADDRESSES = sorted({item["address"] for item in BASE_LOGS})
BASE_FIRST_BLOCK = min(int(item["blockNumber"], 16) for item in BASE_LOGS)
BASE_LAST_BLOCK = max(int(item["blockNumber"], 16) for item in BASE_LOGS)
BASE_TIMESTAMPS = {int(item["blockNumber"], 16): int(item["blockTimestamp"], 16) for item in BASE_LOGS}

Refusal = Callable[[dict], RpcError | None]


def never_refuse(log_filter: dict) -> RpcError | None:
    return None


def synthetic_log(block_number: int, address: str = "0x00000000000000000000000000000000000000aa") -> dict:
    """Builds a raw ``eth_getLogs`` item, one per block, with a timestamp."""
    return {
        "address": address,
        "topics": ["0xdd"],
        "data": "0x",
        "blockHash": f"0x{block_number:064x}",
        "blockNumber": hex(block_number),
        "blockTimestamp": hex(1_700_000_000 + block_number),
        "transactionHash": f"0x{block_number:064x}",
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "removed": False,
    }


def expected(items: list[dict]) -> list[Log]:
    """Returns raw logs as the fetcher should: parsed, deduplicated and in chain order."""
    unique = {log.key: log for log in (Log.from_rpc(item) for item in items)}
    return sorted(unique.values(), key=lambda log: (log.block_number, log.log_index))


class MockPool:
    """Serves ``eth_getLogs``, ``eth_blockNumber`` and ``eth_getBlockByNumber`` from raw logs held in memory."""

    lane = "backfill"

    def __init__(
        self,
        logs: list[dict],
        *,
        head: int | None = None,
        head_rises_by: int = 0,
        refuse: Refusal = never_refuse,
        timestamps: dict[int, int] | None = None,
    ):
        self.logs = logs
        self.head = head if head is not None else max(int(item["blockNumber"], 16) for item in logs)
        self.head_rises_by = head_rises_by
        self.refuse = refuse
        self.timestamps = timestamps or {}
        self.log_filters: list[dict] = []
        self.header_requests: list[int] = []

    async def batch(self, calls: list[tuple[str, list]]) -> list:
        return [self.answer(method, params) for method, params in calls]

    async def request(self, method: str, params: list):
        result = self.answer(method, params)
        if isinstance(result, RpcError):
            raise result
        return result

    def answer(self, method: str, params: list):
        if method == "eth_blockNumber":
            head = self.head
            self.head += self.head_rises_by
            return hex(head)
        if method == "eth_getBlockByNumber":
            number = int(params[0], 16)
            self.header_requests.append(number)
            block_hash = f"0x{number:064x}"
            return {"number": hex(number), "hash": block_hash, "parentHash": block_hash, "timestamp": hex(self.timestamps[number])}
        log_filter = params[0]
        self.log_filters.append(log_filter)
        refusal = self.refuse(log_filter)
        if refusal is not None:
            return refusal
        return [item for item in self.logs if self.matches(log_filter, item)]

    def matches(self, log_filter: dict, item: dict) -> bool:
        block_number = int(item["blockNumber"], 16)
        in_range = int(log_filter["fromBlock"], 16) <= block_number <= int(log_filter["toBlock"], 16)
        addresses = log_filter.get("address")
        return in_range and (addresses is None or item["address"] in addresses)


def span(log_filter: dict) -> int:
    return int(log_filter["toBlock"], 16) - int(log_filter["fromBlock"], 16) + 1


@pytest.mark.parametrize("addresses_per_request", [1, 3, 7, 8])
async def test_addresses_are_fetched_in_chunks_and_merged_sorted_and_deduplicated(addresses_per_request):
    # The node repeats some logs, as overlapping answers would.
    pool = MockPool(BASE_LOGS + BASE_LOGS[:10])
    fetcher = LogFetcher(pool, addresses_per_request=addresses_per_request, max_blocks=BASE_LAST_BLOCK - BASE_FIRST_BLOCK + 1)
    logs = await fetcher.fetch(LogQuery(addresses=BASE_ADDRESSES), BASE_FIRST_BLOCK, BASE_LAST_BLOCK)
    assert logs == expected(BASE_LOGS)
    assert len(pool.log_filters) == math.ceil(len(BASE_ADDRESSES) / addresses_per_request)


async def test_a_range_error_splits_the_window_which_grows_back_after_answers():
    def refuse_wide(log_filter: dict) -> RpcError | None:
        if span(log_filter) > 250:
            return RpcError(-32005, "query returned more than 10000 results", "eth_getLogs")
        return None

    raw_logs = [synthetic_log(block_number) for block_number in range(0, 1000, 7)]
    pool = MockPool(raw_logs, head=999, refuse=refuse_wide)
    fetcher = LogFetcher(pool, addresses_per_request=8, max_blocks=1000)
    logs = await fetcher.fetch(LogQuery(), 0, 999)
    assert logs == expected(raw_logs)
    answered = [span(log_filter) for log_filter in pool.log_filters if refuse_wide(log_filter) is None]
    assert answered == [250, 250, 250, 250]
    assert fetcher.window == 1000


async def test_a_refusal_of_many_addresses_halves_the_address_chunk_and_remembers_it():
    def refuse_crowds(log_filter: dict) -> RpcError | None:
        if len(log_filter["address"]) > 2:
            return RpcError(-32000, "Request blocked", "eth_getLogs")
        return None

    addresses = [f"0x{index:040x}" for index in range(8)]
    raw_logs = [synthetic_log(block_number, address) for block_number, address in enumerate(addresses)]
    pool = MockPool(raw_logs, refuse=refuse_crowds)
    fetcher = LogFetcher(pool, addresses_per_request=8, max_blocks=100)
    assert await fetcher.fetch(LogQuery(addresses=addresses), 0, 7) == expected(raw_logs)
    assert fetcher.addresses_per_request == 2

    pool.log_filters.clear()
    await fetcher.fetch(LogQuery(addresses=addresses), 0, 7)
    assert [len(log_filter["address"]) for log_filter in pool.log_filters] == [2, 2, 2, 2]


async def test_an_address_chunk_shrunk_by_a_blip_grows_back_to_the_configured_one(monkeypatch):
    monkeypatch.setattr("chainindex.fetch.ADDRESS_CHUNK_PROBE_AFTER", 4)
    blip = {"on": True}

    def refuse_during_the_blip(log_filter: dict) -> RpcError | None:
        if blip["on"] and len(log_filter["address"]) > 1:
            return RpcError(-32000, "internal filter error", "eth_getLogs")
        return None

    addresses = [f"0x{index:040x}" for index in range(16)]
    raw_logs = [synthetic_log(block_number, address) for block_number, address in enumerate(addresses)]
    pool = MockPool(raw_logs, refuse=refuse_during_the_blip)
    fetcher = LogFetcher(pool, addresses_per_request=8, max_blocks=100)
    assert await fetcher.fetch(LogQuery(addresses=addresses), 0, 15) == expected(raw_logs)
    assert fetcher.addresses_per_request == 1

    blip["on"] = False
    requests = []
    while fetcher.addresses_per_request < 8 and len(requests) < 20:
        pool.log_filters.clear()
        assert await fetcher.fetch(LogQuery(addresses=addresses), 0, 15) == expected(raw_logs)
        requests.append(len(pool.log_filters))
    # Each probe is one chunk of twice the size, sent once four requests were answered at the shrunk one.
    assert requests == [16, 15, 8, 7, 4, 3]
    assert fetcher.addresses_per_request == 8


async def test_a_real_address_limit_costs_one_refused_request_a_probe():
    def refuse_crowds(log_filter: dict) -> RpcError | None:
        if len(log_filter["address"]) > 2:
            return RpcError(-32000, "Request blocked", "eth_getLogs")
        return None

    addresses = [f"0x{index:040x}" for index in range(400)]
    pool = MockPool([synthetic_log(0, addresses[0])], head=100, refuse=refuse_crowds)
    fetcher = LogFetcher(pool, addresses_per_request=8, max_blocks=100)
    await fetcher.fetch(LogQuery(addresses=addresses), 0, 0)
    assert fetcher.addresses_per_request == 2

    refused = []
    for _window in range(10):
        pool.log_filters.clear()
        await fetcher.fetch(LogQuery(addresses=addresses), 0, 0)
        refused.append(sum(refuse_crowds(log_filter) is not None for log_filter in pool.log_filters))
    assert fetcher.addresses_per_request == 2
    # 200 requests a window: the first answers the 100 a probe waits for, then every window sends one probe.
    assert refused == [0] + [1] * 9


async def test_a_node_that_catches_up_during_the_retries_answers_the_range(monkeypatch):
    monkeypatch.setattr("chainindex.fetch.NODE_BEHIND_WAIT_SECONDS", 0)
    raw_logs = [synthetic_log(block_number) for block_number in range(10)]
    pool = MockPool(raw_logs, head=7, head_rises_by=1)
    fetcher = LogFetcher(pool, addresses_per_request=8, max_blocks=100)
    assert await fetcher.fetch(LogQuery(), 0, 9) == expected(raw_logs)
    assert len(pool.log_filters) == 3


async def test_a_node_that_stays_below_the_range_end_raises_node_behind(monkeypatch):
    monkeypatch.setattr("chainindex.fetch.NODE_BEHIND_WAIT_SECONDS", 0)
    pool = MockPool([synthetic_log(block_number) for block_number in range(10)], head=8)
    fetcher = LogFetcher(pool, addresses_per_request=8, max_blocks=100)
    with pytest.raises(NodeBehind, match="below block 9"):
        await fetcher.fetch(LogQuery(), 0, 9)
    assert len(pool.log_filters) == NODE_BEHIND_RETRIES


async def test_missing_block_timestamps_are_filled_from_headers_once_per_block():
    raw_logs = [{key: value for key, value in item.items() if key != "blockTimestamp"} for item in BASE_LOGS]
    pool = MockPool(raw_logs, timestamps=BASE_TIMESTAMPS)
    fetcher = LogFetcher(pool, addresses_per_request=8, max_blocks=10_000)
    query = LogQuery(addresses=BASE_ADDRESSES)
    assert await fetcher.fetch(query, BASE_FIRST_BLOCK, BASE_LAST_BLOCK) == expected(BASE_LOGS)
    await fetcher.fetch(query, BASE_FIRST_BLOCK, BASE_LAST_BLOCK)
    assert sorted(pool.header_requests) == sorted(BASE_TIMESTAMPS)


async def test_missing_block_timestamps_come_from_the_headers_endpoint_in_its_batch_size():
    raw_logs = [{key: value for key, value in item.items() if key != "blockTimestamp"} for item in BASE_LOGS]
    pool = MockPool(raw_logs, timestamps=BASE_TIMESTAMPS)
    headers = MockPool([], head=BASE_LAST_BLOCK, timestamps=BASE_TIMESTAMPS)
    sizes: list[int] = []
    answer_batch = headers.batch

    async def batch(calls):
        sizes.append(len(calls))
        return await answer_batch(calls)

    headers.batch = batch
    fetcher = LogFetcher(pool, addresses_per_request=8, max_blocks=10_000, headers_pool=headers, headers_per_request=2)

    assert await fetcher.fetch(LogQuery(addresses=BASE_ADDRESSES), BASE_FIRST_BLOCK, BASE_LAST_BLOCK) == expected(BASE_LOGS)
    assert pool.header_requests == []
    assert sorted(headers.header_requests) == sorted(BASE_TIMESTAMPS)
    assert max(sizes) == 2


async def test_header_batches_go_out_together_and_come_back_in_the_order_asked():
    numbers = sorted(BASE_TIMESTAMPS)
    headers = MockPool([], head=BASE_LAST_BLOCK, timestamps=BASE_TIMESTAMPS)
    in_flight = peak = 0
    answer_batch = headers.batch

    async def batch(calls):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0)
        in_flight -= 1
        return await answer_batch(calls)

    headers.batch = batch

    fetched = await fetch_headers(headers, numbers, per_batch=1)

    assert [header.number for header in fetched] == numbers
    assert peak == len(numbers)


@pytest.mark.parametrize(
    ("items", "size", "pieces"),
    [
        (["a", "b", "c", "d", "e"], 2, [["a", "b"], ["c", "d"], ["e"]]),
        (["a", "b"], 5, [["a", "b"]]),
        ([], 3, []),
    ],
)
def test_chunks_splits_a_list_into_consecutive_pieces_of_at_most_the_size(items, size, pieces):
    assert chunks(items, size) == pieces


@pytest.mark.parametrize(
    ("code", "message", "about_size"),
    [
        (-32005, "anything at all", True),
        (-32000, "query returned more than 10000 results", True),
        (-32602, "block range too large", True),
        (-32000, "Log response size exceeded", True),
        (-32000, "log query timed out", True),
        (-32008, "Response is too big", True),
        (-32000, "Request blocked", False),
        (3, "execution reverted", False),
    ],
)
def test_is_range_error_tells_size_refusals_from_other_errors(code, message, about_size):
    assert is_range_error(RpcError(code, message, "eth_getLogs")) == about_size


async def test_missing_block_timestamps_are_filled_when_the_cache_overflows():
    raw_logs = [{key: value for key, value in item.items() if key != "blockTimestamp"} for item in BASE_LOGS]
    pool = MockPool(raw_logs, timestamps=BASE_TIMESTAMPS)
    fetcher = LogFetcher(pool, addresses_per_request=8, max_blocks=10_000)
    fetcher.timestamps = OrderedDict((block_number, 0) for block_number in range(fetch.MAX_CACHED_TIMESTAMPS))
    logs = await fetcher.fetch(LogQuery(addresses=BASE_ADDRESSES), BASE_FIRST_BLOCK, BASE_LAST_BLOCK)
    assert logs == expected(BASE_LOGS)
    # The oldest entries made room: the cache stays at its cap and keeps what was just fetched.
    assert len(fetcher.timestamps) == fetch.MAX_CACHED_TIMESTAMPS
    assert BASE_LAST_BLOCK in fetcher.timestamps and 0 not in fetcher.timestamps


async def test_a_header_batch_refused_for_its_size_is_halved_until_the_provider_takes_it():
    numbers = sorted(BASE_TIMESTAMPS)
    headers = MockPool([], head=BASE_LAST_BLOCK, timestamps=BASE_TIMESTAMPS)
    answer_batch = headers.batch
    sizes: list[int] = []

    async def batch(calls):
        sizes.append(len(calls))
        if len(calls) > 2:
            return [RpcError(-32010, "The batch request was too large", "eth_getBlockByNumber") for _call in calls]
        return await answer_batch(calls)

    headers.batch = batch

    fetched = await fetch_headers(headers, numbers, per_batch=len(numbers))

    assert [header.number for header in fetched] == numbers
    assert max(size for size in sizes if size <= 2) <= 2
