"""The generated dbt models are current and decode each ABI type from the right word."""

from pathlib import Path

import pytest

from chainindex import dbtgen
from chainindex.abi import EventInput, EventSpec
from chainindex.config import load_chain, packaged_chains

from .doubles import CREATED_CONTRACTS, chain_config

MODELS_DIR = Path(__file__).parents[1] / "dbt" / "models"


def test_the_checked_in_models_match_the_chain_files_and_abis():
    chains = [load_chain(name) for name in packaged_chains()]

    assert dbtgen.stale(chains, MODELS_DIR) == []


@pytest.mark.parametrize("name", packaged_chains())
def test_every_model_is_a_view_that_decodes_on_read(name):
    files = dbtgen.render(load_chain(name))

    models = [content for file, content in files.items() if file.endswith(".sql")]
    assert all("materialized='view'" in content for content in models)
    assert not any("is_incremental" in content for content in models)


def test_a_kind_without_an_abi_gets_one_undecoded_view_of_its_addresses_logs():
    chain = chain_config([{"name": "token", "start_block": 1, "addresses": [{"address": "0x" + "c0" * 20}]}])

    files = dbtgen.render(chain)

    assert [file for file in files if file.startswith("testnet__token")] == ["testnet__token__logs.sql"]
    sql = files["testnet__token__logs.sql"]
    assert "topic0,\n    topic1,\n    topic2,\n    topic3,\n    data" in sql
    assert "where contract = 'token'" in sql
    assert "topic0 = " not in sql


@pytest.mark.parametrize(
    "abi_type, expected",
    [
        ("address", "concat('0x', substring(W, 25, 40))"),
        ("uint24", "toUInt32(reinterpretAsUInt256(reverse(unhex(W))))"),
        ("int24", "toInt32(reinterpretAsInt256(reverse(unhex(W))))"),
        ("uint256", "reinterpretAsUInt256(reverse(unhex(W)))"),
        ("bytes32", "concat('0x', substring(W, 1, 64))"),
        ("bool", "toBool(reinterpretAsUInt256(reverse(unhex(W))) != 0)"),
        ("uint256[2]", None),
    ],
)
def test_static_types_decode_from_one_word(abi_type, expected):
    assert dbtgen.static_value(abi_type, "W") == expected


def test_a_parameter_sql_cannot_decode_is_left_out_without_shifting_the_ones_after_it():
    event = EventSpec(
        name="Odd", signature="Odd((uint256,address),uint8[],uint256)", topic0="0x00",
        inputs=[
            EventInput(name="pair", abi_type="(uint256,address)", indexed=False),
            EventInput(name="list", abi_type="uint8[]", indexed=False),
            EventInput(name="amount", abi_type="uint256", indexed=False),
        ],
    )

    columns, skipped = dbtgen.event_columns(event)

    # The static tuple takes two head words and the dynamic array one, so amount is word 3.
    assert columns == [("amount", "reinterpretAsUInt256(reverse(unhex(substring(data, 195, 64))))", "uint256")]
    assert len(skipped) == 2


def test_a_parameter_named_like_a_log_column_is_renamed():
    event = EventSpec(
        name="Moved", signature="Moved(address)", topic0="0x00",
        inputs=[EventInput(name="address", abi_type="address", indexed=True)],
    )

    columns, _skipped = dbtgen.event_columns(event)

    assert [name for name, _sql, _type in columns] == ["address_arg"]


@pytest.mark.parametrize(("kind", "creation"), [("pool", "testnet__factory__pool_created"), ("gauge", "testnet__voter__gauge_created")])
def test_a_created_kinds_model_keeps_only_addresses_its_factory_announced(kind, creation):
    chain = chain_config(CREATED_CONTRACTS)
    contract = chain.contract(kind)

    sql = dbtgen.event_model(chain, contract, contract.event_specs()[0])

    assert f"from {{{{ ref('{creation}') }}}}" in sql
    assert "testnet__contracts" not in sql


def test_an_any_kinds_model_keeps_every_emitter():
    chain = chain_config([{"name": "transfers", "abi": "ERC20", "track": "any", "fetch": "topics", "start_block": 50}])
    contract = chain.contract("transfers")

    sql = dbtgen.event_model(chain, contract, contract.event_specs()[0])

    assert "address in" not in sql
