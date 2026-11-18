"""The registry turns the chain file and runtime changes into streams."""

from pathlib import Path

from eth_abi import encode

from chainindex.abi import load_abi_events
from chainindex.models import Log
from chainindex.registry import Registry
from chainindex.store import ContractRow

from .doubles import CREATED_CONTRACTS, address, chain_config, word

POOL_A = address(0xA)
POOL_B = address(0xB)
FACTORY = address(0xF)
CONTRACTS = [
    {
        "name": "factory", "abi": "UniswapV3Factory", "addresses": [{"address": FACTORY, "start_block": 5}],
        "creates": {"event": "PoolCreated", "param": "pool", "contract": "pool"},
    },
    {"name": "pool", "abi": "UniswapV3Pool", "start_block": 100, "addresses": [{"address": POOL_A, "label": "A"}]},
    {"name": "scanned", "abi": "AerodromePool", "fetch": "topics", "start_block": 300, "events": ["Swap"]},
]


def test_listed_addresses_get_their_own_streams_and_topic_kinds_one_stream():
    registry = Registry(chain_config(CONTRACTS))
    registry.reconcile([])
    registry.apply([ContractRow(address=POOL_B, contract="scanned", start_block=200, source="cli")])

    streams = {stream.key: stream for stream in registry.streams()}

    assert set(streams) == {f"address:{FACTORY}", f"address:{POOL_A}", "topics:scanned"}
    assert streams[f"address:{POOL_A}"].topics is None
    assert streams["topics:scanned"].start_block == 200
    assert streams["topics:scanned"].topics == [registry.config.contract("scanned").topics()[0]]


def test_reconciling_records_new_and_changed_config_rows_and_retires_dropped_ones():
    registry = Registry(chain_config(CONTRACTS))
    stored = [
        ContractRow(address=POOL_A, contract="pool", start_block=50, source="config", label="A"),
        ContractRow(address=POOL_B, contract="pool", start_block=100, source="config"),
        ContractRow(address=address(0xC), contract="pool", start_block=100, source="cli"),
    ]

    changes = registry.reconcile(stored)

    assert sorted((row.address, row.start_block, row.active) for row in changes) == [
        (POOL_A, 100, True), (POOL_B, 100, False), (FACTORY, 5, True),
    ]
    assert registry.contract_of(address(0xC)) == "pool"


def test_reloading_from_clickhouse_keeps_children_found_at_the_tip():
    registry = Registry(chain_config(CONTRACTS))
    registry.reconcile([])
    child = ContractRow(address=POOL_B, contract="pool", start_block=900, source="factory")
    registry.apply([child], recorded=False)

    registry.load([])

    assert registry.contract_of(POOL_B) == "pool"
    assert registry.unrecorded == {POOL_B}


GAUGE_CONTRACTS = [
    {
        "name": "voter", "abi": "AerodromeVoter", "events": ["GaugeCreated"],
        "addresses": [{"address": FACTORY, "start_block": 5}],
        "creates": {"event": "GaugeCreated", "param": "gauge", "contract": "gauge"},
    },
    {"name": "pool", "abi": "AerodromeCLPool", "start_block": 100, "addresses": [{"address": POOL_A}]},
    {"name": "gauge", "abi": "AerodromeCLGauge", "events": [], "track": "matching", "match_contract": {"pool": "pool"}},
]


def gauge_created(pool: str, gauge: str) -> Log:
    spec = next(event for event in load_abi_events("AerodromeVoter") if event.name == "GaugeCreated")
    data = encode(["address"] * 5, [pool, address(0x21), address(0x22), gauge, address(0x23)])
    return Log(
        block_number=200, block_hash="0x" + "00" * 32, block_timestamp=1, transaction_hash="0x" + "11" * 32,
        transaction_index=0, log_index=0, address=FACTORY,
        topics=(spec.topic0, word(address(0x31)), word(address(0x32)), word(address(0x33))), data="0x" + data.hex(),
    )


def test_a_child_whose_parameter_names_an_indexed_contract_is_kept_and_a_registry_only_kind_has_no_stream():
    registry = Registry(chain_config(GAUGE_CONTRACTS))
    registry.reconcile([])

    found = registry.discover([gauge_created(POOL_A, address(0x91)), gauge_created(POOL_B, address(0x92))])
    registry.apply(found)

    assert [row.address for row in found] == [address(0x91)]
    assert registry.contract_of(address(0x91)) == "gauge"
    assert "address:" + address(0x91) not in {stream.key for stream in registry.streams()}


def pool_created(pool: str) -> Log:
    spec = next(event for event in load_abi_events("UniswapV3Factory") if event.name == "PoolCreated")
    return Log(
        block_number=300, block_hash="0x" + "00" * 32, block_timestamp=1, transaction_hash="0x" + "22" * 32,
        transaction_index=0, log_index=0, address=FACTORY,
        topics=(spec.topic0, word(address(0x41)), word(address(0x42)), word(3000)),
        data="0x" + encode(["int24", "address"], [60, pool]).hex(),
    )


def created_scan(registry: Registry):
    return next(stream for stream in registry.streams() if stream.by_topic)


def test_created_kinds_share_one_chain_wide_scan_from_their_earliest_factory():
    registry = Registry(chain_config(CREATED_CONTRACTS))
    registry.reconcile([])

    scans = [stream for stream in registry.streams() if stream.by_topic]

    config = registry.config
    assert len(scans) == 1
    assert scans[0].start_block == 5
    assert set(scans[0].topics) == set(config.contract("pool").topics()) | set(config.contract("gauge").topics())
    assert sorted(registry.factory_streams(scans[0].contract)) == [f"address:{address(0xE)}", f"address:{FACTORY}"]


def test_the_created_scan_keeps_a_log_whoever_emitted_it_and_leaves_the_check_to_dbt():
    registry = Registry(chain_config(CREATED_CONTRACTS))
    registry.reconcile([])
    stranger = Log(
        block_number=400, block_hash="0x" + "00" * 32, block_timestamp=1, transaction_hash="0x" + "33" * 32,
        transaction_index=0, log_index=0, address=address(0x99), topics=(registry.config.contract("pool").topics()[0],),
        data="0x",
    )

    assert registry.keep(created_scan(registry), [stranger]) == [stranger]


def test_the_created_scan_starts_over_under_a_new_cursor_when_its_topics_change():
    whole = Registry(chain_config(CREATED_CONTRACTS))
    whole.reconcile([])
    pools_only = Registry(chain_config([CREATED_CONTRACTS[0], CREATED_CONTRACTS[2]]))
    pools_only.reconcile([])

    assert created_scan(whole).key != created_scan(pools_only).key


def test_a_created_kind_records_no_children_and_retires_the_ones_it_has():
    registry = Registry(chain_config(CREATED_CONTRACTS))
    stored = ContractRow(address=POOL_A, contract="pool", start_block=300, source="factory", created_block=300)

    changes = registry.reconcile([stored])

    assert registry.discover([pool_created(POOL_B)]) == []
    assert [(row.address, row.active) for row in changes if row.contract == "pool"] == [(POOL_A, False)]
    assert registry.members("pool") == {}


def test_a_refetched_kind_fetches_its_history_again_under_fresh_cursors():
    contracts = [dict(CONTRACTS[1], refetch=2)]
    registry = Registry(chain_config(contracts))
    registry.reconcile([])

    (stream,) = registry.streams()

    assert stream.key == f"address:{POOL_A}:refetch2"
    assert stream.start_block == 100


def test_a_listed_address_replaces_a_created_kinds_old_child_row():
    contracts = [
        {"name": "factory", "abi": "UniswapV3Factory", "addresses": [{"address": FACTORY, "start_block": 5}],
         "creates": {"event": "PoolCreated", "param": "pool", "contract": "scanned"}},
        {"name": "scanned", "abi": "UniswapV3Pool", "track": "created", "fetch": "topics", "start_block": 5},
        {"name": "pool", "abi": "UniswapV3Pool", "addresses": [{"address": POOL_A, "start_block": 100}]},
    ]
    registry = Registry(chain_config(contracts))
    old_child = ContractRow(address=POOL_A, contract="scanned", start_block=90, source="factory")

    registry.reconcile([old_child])

    assert registry.contract_of(POOL_A) == "pool"
    assert [stream.key for stream in registry.streams() if stream.addresses == [POOL_A]] == [f"address:{POOL_A}"]


TRANSFERS = {"name": "transfers", "abi": "ERC20", "track": "any", "fetch": "topics", "start_block": 50}


def test_an_any_kind_scans_from_its_start_and_keeps_a_log_whoever_emitted_it():
    registry = Registry(chain_config([TRANSFERS]))
    registry.reconcile([])
    scan = next(stream for stream in registry.streams() if stream.contract == "transfers")
    stranger = Log(
        block_number=400, block_hash="0x" + "00" * 32, block_timestamp=1, transaction_hash="0x" + "33" * 32,
        transaction_index=0, log_index=0, address=address(0x99), topics=(registry.config.contract("transfers").topics()[0],),
        data="0x",
    )

    assert scan.start_block == 50
    assert scan.by_topic
    assert registry.keep(scan, [stranger]) == [stranger]


def test_a_refetched_factory_gates_its_children_by_the_cursor_it_writes():
    contracts = [
        {"name": "factory", "abi": "UniswapV3Factory", "addresses": [{"address": FACTORY, "start_block": 5}], "refetch": 1,
         "creates": {"event": "PoolCreated", "param": "pool", "contract": "pool"}},
        {"name": "pool", "abi": "UniswapV3Pool", "track": "created", "fetch": "topics"},
    ]
    registry = Registry(chain_config(contracts))
    registry.reconcile([])

    factory_keys = {stream.key for stream in registry.streams() if stream.contract == "factory"}

    assert set(registry.factory_streams("pool")) == factory_keys == {f"address:{FACTORY}:refetch1"}


def test_a_factory_announcing_its_child_in_a_camel_case_parameter_records_it(monkeypatch):
    monkeypatch.setenv("CHAININDEX_ABIS_DIR", str(Path(__file__).parent / "fixtures" / "abis"))
    child_factory = address(0xE)
    registry = Registry(chain_config([
        {
            "name": "child_factory", "abi": "ChildFactory",
            "addresses": [{"address": child_factory, "start_block": 5}],
            "creates": {"event": "ChildCreated", "param": "childManager", "contract": "child"},
        },
        {"name": "child", "abi": "ERC20", "track": "all"},
    ]))
    registry.reconcile([])
    spec = next(event for event in load_abi_events("ChildFactory") if event.name == "ChildCreated")
    created = Log(
        block_number=300, block_hash="0x" + "00" * 32, block_timestamp=1, transaction_hash="0x" + "22" * 32,
        transaction_index=0, log_index=0, address=child_factory, topics=(spec.topic0, word(POOL_A), word(address(0x93))), data="0x",
    )

    found = registry.discover([created])

    assert [(row.address, row.contract) for row in found] == [(address(0x93), "child")]
