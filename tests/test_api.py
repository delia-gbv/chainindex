"""The public API answers only its fixed queries, binds every value, caps every request, and leaks no server errors."""

from typing import Any

import pytest
from aiohttp.test_utils import TestClient, TestServer

from chainindex.api import ApiSettings, ChainIndexApi, RateLimiter, client_key, json_value

POOL = "0x" + "ab" * 20


class MockResult:
    """A ClickHouse result: column names and rows."""

    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]):
        self.column_names = columns
        self.result_rows = rows


class MockClickHouse:
    """Records every query and answers from canned rows; ``fail`` makes every query raise that message."""

    def __init__(self, *, fail: str = ""):
        self.fail = fail
        self.queries: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def query(self, sql: str, parameters: dict[str, Any], settings: dict[str, Any]) -> MockResult:
        self.queries.append((sql, parameters, settings))
        if self.fail:
            raise RuntimeError(self.fail)
        if "FROM chain_base.cursors" in sql:
            return MockResult(["stream", "from_block", "to_block"], [("topics:aerodrome_cl_factory", 1, 5000), ("verify:blocks", 10, 4000)])
        if "count()" in sql and "orphaned_blocks" in sql:
            return MockResult(["count()"], [(0,)])
        if "quantiles" in sql:
            return MockResult(["count()", "q"], [(0, [0.0, 0.0])])
        if "events_base." in sql:
            return MockResult(["block_number", "amount0"], [(4999, 10**30)])
        return MockResult([], [])


def settings(**overrides: Any) -> ApiSettings:
    return ApiSettings(_env_file=None, API_CHAINS="base", **overrides)


async def serve(api: ChainIndexApi) -> TestClient:
    client = TestClient(TestServer(api.app()))
    await client.start_server()
    return client


async def get(api: ChainIndexApi, path: str) -> tuple[int, dict[str, Any]]:
    client = await serve(api)
    try:
        response = await client.get(path)
        return response.status, await response.json()
    finally:
        await client.close()


async def test_events_bind_the_address_and_blocks_and_send_big_numbers_as_text():
    clickhouse = MockClickHouse()
    api = ChainIndexApi(settings(), client=clickhouse)

    status, page = await get(api, f"/v1/base/events/aerodrome_cl_pool/Swap?address={POOL}&from_block=100&to_block=200&limit=5")

    assert status == 200
    sql, parameters, limits = clickhouse.queries[-1]
    assert "FROM events_base.aerodrome_cl_pool__swap " in sql
    assert POOL not in sql
    assert parameters == {"from_block": 100, "to_block": 200, "limit": 5, "address": POOL}
    assert limits["max_execution_time"] == 10
    assert page["rows"] == [[4999, str(10**30)]]


async def test_without_an_address_the_range_ends_at_the_ledger_and_spans_at_most_the_cap():
    clickhouse = MockClickHouse()
    api = ChainIndexApi(settings(API_MAX_RANGE_SECONDS=20), client=clickhouse)

    status, page = await get(api, "/v1/base/events/aerodrome_cl_pool/Swap")
    refused, error = await get(api, "/v1/base/events/aerodrome_cl_pool/Swap?from_block=0&to_block=4000")

    assert status == 200
    assert (page["from_block"], page["to_block"]) == (4901, 5000)
    assert refused == 400
    assert "at most 100 blocks" in error["error"]


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/v1/solana/status", 404),
        ("/v1/base/events/no_such_kind/Swap", 404),
        ("/v1/base/events/aerodrome_cl_pool/NoSuchEvent", 404),
        ("/v1/base/events/aerodrome_cl_pool/Swap?address=0x1234", 400),
        ("/v1/base/events/aerodrome_cl_pool/Swap?address=' OR 1=1 --", 400),
        ("/v1/base/events/aerodrome_cl_pool/Swap?limit=100000", 400),
        ("/v1/base/events/aerodrome_cl_pool/Swap?to_block=abc", 400),
    ],
)
async def test_requests_outside_the_fixed_queries_are_refused_before_any_sql_runs(path, expected):
    clickhouse = MockClickHouse()
    api = ChainIndexApi(settings(), client=clickhouse)

    status, _page = await get(api, path)

    assert status == expected
    assert not any("events_base." in sql for sql, _parameters, _limits in clickhouse.queries)


@pytest.mark.parametrize(
    "failure, expected",
    [
        ("Code: 159. TIMEOUT_EXCEEDED on secret_host:9000", 504),
        ("Code: 60. DB::Exception: Table events_base.secret_table does not exist. (UNKNOWN_TABLE)", 404),
    ],
)
async def test_a_failed_query_tells_the_client_nothing_about_the_server(failure, expected):
    api = ChainIndexApi(settings(), client=MockClickHouse(fail=failure))

    status, page = await get(api, f"/v1/base/events/aerodrome_cl_pool/Swap?address={POOL}&to_block=10")

    assert status == expected
    assert "secret" not in page["error"]


async def test_a_client_past_its_budget_is_told_to_slow_down():
    api = ChainIndexApi(settings(API_REQUESTS_PER_MINUTE=6), client=MockClickHouse())
    client = await serve(api)
    try:
        statuses = [(await client.get("/v1/chains")).status for _request in range(3)]
    finally:
        await client.close()

    assert statuses == [200, 429, 429]


def test_a_bucket_refills_at_its_rate():
    limiter = RateLimiter(60, burst=1)

    assert limiter.allow("a", now=0.0)
    assert not limiter.allow("a", now=0.5)
    assert limiter.allow("a", now=1.6)
    assert limiter.allow("b", now=0.5)


def test_big_integers_go_out_as_text_and_small_ones_as_numbers():
    assert json_value(2**60) == str(2**60)
    assert json_value(5) == 5
    assert json_value(True) is True


async def test_status_reads_the_cursors_and_caches_the_page():
    clickhouse = MockClickHouse()
    api = ChainIndexApi(settings(), client=clickhouse)

    status, page = await get(api, "/v1/base/status")
    asked = len(clickhouse.queries)
    await get(api, "/v1/base/status")

    assert status == 200
    assert page["ledger_block"] == 5000
    assert page["verified"] == {"from_block": 10, "to_block": 4000}
    assert len(clickhouse.queries) == asked


async def test_a_hidden_contract_kind_is_neither_listed_nor_served():
    clickhouse = MockClickHouse()
    api = ChainIndexApi(settings(API_HIDDEN_CONTRACTS="aerodrome_cl_pool"), client=clickhouse)

    status, page = await get(api, f"/v1/base/events/aerodrome_cl_pool/Swap?address={POOL}")
    _status, chains = await get(api, "/v1/chains")

    assert status == 404
    assert "aerodrome_cl_pool" not in {contract["name"] for contract in chains["chains"][0]["contracts"]}


async def test_the_client_address_header_counts_only_from_a_trusted_proxy():
    trusting = ChainIndexApi(settings(API_CLIENT_IP_HEADER="X-Forwarded-For", API_TRUSTED_PROXIES="127.0.0.0/8", API_REQUESTS_PER_MINUTE=6), client=MockClickHouse())
    doubting = ChainIndexApi(settings(API_CLIENT_IP_HEADER="X-Forwarded-For", API_REQUESTS_PER_MINUTE=6), client=MockClickHouse())

    async def statuses(api: ChainIndexApi) -> list[int]:
        client = await serve(api)
        try:
            return [
                (await client.get("/v1/chains", headers={"X-Forwarded-For": f"203.0.113.{spoof}, 198.51.100.{index}"})).status
                for index, spoof in enumerate(range(3))
            ]
        finally:
            await client.close()

    assert await statuses(trusting) == [200, 200, 200]
    assert await statuses(doubting) == [200, 429, 429]


def test_an_ipv6_client_is_one_client_across_its_64():
    assert client_key("2001:db8:1:2:aaaa::1") == client_key("2001:db8:1:2:bbbb::9") == "2001:db8:1:2::/64"
    assert client_key("198.51.100.7") == "198.51.100.7"


def test_an_ipv4_client_written_as_ipv6_is_its_own_ipv4_client():
    # A dual-stack proxy writes IPv4 clients as ::ffff:a.b.c.d; as IPv6 they would all share the bucket ::/64.
    assert client_key("::ffff:198.51.100.7") == "198.51.100.7"
    assert client_key("::ffff:203.0.113.9") == "203.0.113.9"


async def test_ipv4_clients_behind_a_dual_stack_proxy_each_get_their_own_budget():
    api = ChainIndexApi(
        settings(API_CLIENT_IP_HEADER="X-Forwarded-For", API_TRUSTED_PROXIES="127.0.0.0/8", API_REQUESTS_PER_MINUTE=6),
        client=MockClickHouse(),
    )
    client = await serve(api)
    try:
        greedy = [(await client.get("/v1/chains", headers={"X-Forwarded-For": "::ffff:203.0.113.1"})).status for _ in range(2)]
        other = (await client.get("/v1/chains", headers={"X-Forwarded-For": "::ffff:198.51.100.2"})).status
    finally:
        await client.close()

    assert greedy == [200, 429]
    assert other == 200


def test_a_proxy_reaching_the_api_at_an_ipv4_mapped_address_is_trusted():
    api = ChainIndexApi(settings(API_TRUSTED_PROXIES="10.0.0.0/8"), client=MockClickHouse())

    assert api.trusted("::ffff:10.1.2.3")
    assert not api.trusted("::ffff:192.0.2.1")


@pytest.mark.parametrize("query", ["to_block=%C2%B2", "to_block=" + "9" * 30, "limit=%C2%B2"])
async def test_numbers_that_are_not_plain_digits_are_refused(query):
    status, _page = await get(ChainIndexApi(settings(), client=MockClickHouse()), f"/v1/base/events/aerodrome_cl_pool/Swap?address={POOL}&{query}")

    assert status == 400


async def test_only_successful_answers_may_be_cached():
    client = await serve(ChainIndexApi(settings(), client=MockClickHouse()))
    try:
        found = await client.get("/v1/chains")
        missing = await client.get("/v1/solana/status")
    finally:
        await client.close()

    assert found.headers["Cache-Control"] == "public, max-age=5"
    assert missing.headers["Cache-Control"] == "no-store"
