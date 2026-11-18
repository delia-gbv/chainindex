"""Chain files load, and contradictory ones are refused before anything runs."""

import asyncio
from importlib import resources
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from chainindex.config import (
    FunctionSpec,
    Settings,
    endpoint_url,
    load_chain,
    packaged_chains,
)
from chainindex.metrics import Metrics
from chainindex.service import ChainService, supervise

from .doubles import MockChain, MockStore, address, chain_config

LANES = Path(__file__).parent / "fixtures" / "chains" / "lanes.yaml"

FACTORY = {
    "name": "factory", "abi": "UniswapV3Factory", "addresses": [{"address": address(0xF), "start_block": 1}],
    "creates": {"event": "PoolCreated", "param": "pool", "contract": "pool"},
}


@pytest.mark.parametrize("name", packaged_chains())
def test_every_packaged_chain_loads(name):
    chain = load_chain(name)

    assert chain.database == f"chain_{name}"
    assert chain.contracts


@pytest.mark.parametrize(
    "contracts, complaint",
    [
        ([FACTORY, {"name": "pool", "abi": "UniswapV3Pool", "track": "created"}], "needs fetch: topics"),
        ([{"name": "pool", "abi": "UniswapV3Pool", "track": "created", "fetch": "topics"}], "needs a factory"),
        ([{"name": "pool", "abi": "UniswapV3Pool", "track": "all", "fetch": "topics"}], "needs a factory"),
        ([FACTORY, {"name": "pool", "abi": "UniswapV3Pool", "track": "matching"}], "needs a match or match_contract rule"),
        (
            [FACTORY, {"name": "pool", "abi": "UniswapV3Pool", "track": "matching", "match": {"owner": ["0x1"]}}],
            "is not a parameter",
        ),
        (
            [{**FACTORY, "creates": {"event": "PoolCreated", "param": "fee", "contract": "pool"}}, {"name": "pool", "abi": "UniswapV3Pool"}],
            "not an address parameter",
        ),
        ([{"name": "pool", "abi": "UniswapV3Pool", "addresses": [{"address": address(1)}]}], "has no start_block"),
        (
            [
                {"name": "pool", "abi": "UniswapV3Pool", "start_block": 1, "addresses": [{"address": address(1)}]},
                {"name": "other", "abi": "UniswapV3Pool", "start_block": 1, "addresses": [{"address": address(1)}]},
            ],
            "is listed under",
        ),
        ([{"name": "pool", "abi": "UniswapV3Pool", "events": ["Sweep"]}], "has no events"),
        ([{"name": "Pool", "abi": "UniswapV3Pool"}], "lowercase snake_case"),
        ([FACTORY, {"name": "pool", "abi": "UniswapV3Pool", "track": "matching", "match_contract": {"pool": "nowhere"}}], "no contract named"),
        ([{"name": "token", "events": ["Transfer"]}], "has no abi"),
        ([FACTORY, {"name": "pool", "track": "created", "fetch": "topics"}], "needs topics to ask for"),
    ],
)
def test_a_contradictory_chain_file_is_refused(contracts, complaint):
    with pytest.raises(ValidationError, match=complaint):
        chain_config(contracts)


POOL_KIND = [{"name": "pool", "abi": "UniswapV3Pool"}]


@pytest.mark.parametrize(
    "read, complaint",
    [
        ({"name": "state", "contract": "pool"}, "either calls or a tick_map"),
        ({"name": "state", "contract": "elsewhere", "calls": ["fee() returns (uint24 fee)"]}, "no contract named"),
        ({"name": "state", "contract": "pool", "calls": ["fee() returns (uint24)"]}, "name every output"),
        ({"name": "state", "contract": "pool", "calls": ["name() returns (string name)"]}, "one-word types only"),
        ({"name": "state", "contract": "pool", "calls": ["balanceOf(address) returns (uint256 balance)"]}, "takes arguments"),
    ],
)
def test_a_read_the_lane_cannot_run_is_refused(read, complaint):
    with pytest.raises(ValidationError, match=complaint):
        chain_config(POOL_KIND, reads=[read])


def test_a_read_signature_gives_its_selector_and_outputs():
    function = FunctionSpec.parse("slot0() returns (uint160 sqrt_price_x96, int24 tick)")

    assert function.selector == "0x3850c7bd"
    assert function.outputs == [("uint160", "sqrt_price_x96"), ("int24", "tick")]


@pytest.mark.parametrize(
    "key, expected",
    [("secret", "https://base-rpc.publicnode.com/secret"), ("", "https://base-rpc.publicnode.com")],
)
def test_an_endpoint_template_takes_a_key_or_drops_it(key, expected):
    assert endpoint_url("https://base-rpc.publicnode.com/{key}", key) == expected


def test_a_deployment_can_run_the_ledger_alone_with_a_slower_backfill():
    settings = Settings(CHAIN="base", LIVE_ENABLED=False, BACKFILL_REQUESTS_PER_SECOND=1.5, _env_file=None)

    config = settings.chain_config()

    assert config.live.enabled is False
    assert config.rpc.backfill.requests_per_second == 1.5


def test_a_heads_only_deployment_is_ready_without_sealing_and_subscribes_to_no_logs(monkeypatch):
    monkeypatch.setattr("chainindex.service.Metrics", lambda name: Metrics(name, isolated=True))
    service = ChainService(Settings(CHAIN=str(LANES), LIVE_ENABLED=True, LIVE_LOGS=False, _env_file=None), client=object())

    assert service.config.live.logs is False
    assert service.ready()
    assert service.hub.sealed is None


@pytest.mark.parametrize(("websocket", "sockets"), [(False, 0), (True, 2)])
def test_a_chain_file_without_a_websocket_runs_no_sockets(websocket, sockets, monkeypatch, tmp_path):
    monkeypatch.setattr("chainindex.service.Metrics", lambda name: Metrics(name, isolated=True))
    chain = yaml.safe_load(Path(str(resources.files("chainindex") / "chains" / "base.yaml")).read_text())
    if not websocket:
        del chain["rpc"]["ws"]
    chain_file = tmp_path / "chain.yaml"
    chain_file.write_text(yaml.safe_dump(chain))
    service = ChainService(Settings(CHAIN=str(chain_file), _env_file=None), client=object())

    assert len(service.sockets) == sockets


@pytest.mark.parametrize(("chain", "reads_lane"), [(str(LANES), "reads"), ("base", "ledger")])
def test_reads_headers_and_the_block_recorder_get_their_own_pools_only_where_the_chain_file_names_them(chain, reads_lane, monkeypatch):
    monkeypatch.setattr("chainindex.service.Metrics", lambda name: Metrics(name, isolated=True))
    service = ChainService(Settings(CHAIN=chain, RPC_API_KEY="key", _env_file=None), client=object())

    assert ("headers" in service.pools) == (chain == str(LANES))
    assert service.reader.pool.lane == reads_lane
    assert service.recorder.pool.lane == ("headers" if chain == str(LANES) else "backfill")
    # Another provider's throttles must not pause the chain's own RPC's lanes, and the other way round.
    assert (service.reader.pool.throttle is service.pools["backfill"].throttle) == (reads_lane == "ledger")


async def test_block_times_come_from_live_heads_then_stored_headers_then_stored_logs(monkeypatch):
    monkeypatch.setattr("chainindex.service.Metrics", lambda name: Metrics(name, isolated=True))
    service = ChainService(Settings(CHAIN=str(LANES), _env_file=None), client=object())
    chain = MockChain(head=100)
    service.store = MockStore()
    service.live.headers[10] = chain.header(10)
    await service.store.insert_blocks([chain.header(20)])
    await service.store.insert_logs([chain.add_log(address(1), 30, [])], lane="backfill")

    found = await service.known_timestamps([10, 20, 30, 40])

    assert found == {10: 1_700_000_010, 20: 1_700_000_020, 30: 1_700_000_030}


async def test_a_registry_refresh_clickhouse_refuses_is_retried_on_the_next_refresh_not_counted_as_a_crash(monkeypatch):
    monkeypatch.setattr("chainindex.service.Metrics", lambda name: Metrics(name, isolated=True))
    service = ChainService(Settings(CHAIN=str(LANES), REGISTRY_REFRESH_SECONDS=0.01, _env_file=None), client=object())
    service.store = MockStore()
    calls = 0
    stored = service.store.contracts

    async def contracts():
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise ConnectionError("ClickHouse is restarting")
        return await stored()

    service.store.contracts = contracts  # type: ignore[method-assign]
    supervised = asyncio.create_task(supervise("registry", service.refresh_registry, service.metrics))
    while calls < 4:
        await asyncio.sleep(0.01)
    supervised.cancel()

    assert service.metrics.component_restarts.labels(component="registry")._value.get() == 0
