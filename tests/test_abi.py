"""ABI events get the right topics and column names, and real logs decode."""

import json
from pathlib import Path

import pytest

from chainindex.abi import Decoder, load_abi_events, snake_case
from chainindex.models import Log

FIXTURE = Path(__file__).parent / "fixtures" / "base_logs.json"
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


@pytest.mark.parametrize(
    "abi, event, topic0",
    [
        # Verified against live Base logs.
        ("AerodromeCLFactory", "PoolCreated", "0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2"),
        ("AerodromeCLPool", "Swap", SWAP_TOPIC),
        ("AerodromePoolFactory", "PoolCreated", "0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e"),
        ("AerodromePool", "Fees", "0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602"),
    ],
)
def test_events_get_the_topic_the_chain_emits(abi, event, topic0):
    assert {spec.name: spec.topic0 for spec in load_abi_events(abi)}[event] == topic0


@pytest.mark.parametrize("name, column", [("sqrtPriceX96", "sqrt_price_x96"), ("PoolCreated", "pool_created"), ("tick", "tick")])
def test_abi_names_become_snake_case(name, column):
    assert snake_case(name) == column


def test_real_swaps_decode_with_signed_amounts_and_ticks():
    decoder = Decoder({"aerodrome_cl_pool": load_abi_events("AerodromeCLPool")})
    swaps = [Log.from_rpc(item) for item in json.loads(FIXTURE.read_text()) if item["topics"][0] == SWAP_TOPIC]

    decoded = [decoder.decode("aerodrome_cl_pool", log) for log in swaps]

    assert swaps and all(item is not None for item in decoded)
    for item in decoded:
        assert item is not None
        assert set(item.args) == {"sender", "recipient", "amount0", "amount1", "sqrt_price_x96", "liquidity", "tick"}
        assert (item.args["amount0"] < 0) != (item.args["amount1"] < 0)
        assert -887272 <= item.args["tick"] <= 887272
        assert item.args["sender"].startswith("0x") and len(item.args["sender"]) == 42


def test_a_log_whose_layout_does_not_fit_the_event_is_not_decoded():
    decoder = Decoder({"aerodrome_cl_pool": load_abi_events("AerodromeCLPool")})
    item = json.loads(FIXTURE.read_text())[0]
    truncated = Log.from_rpc({**item, "topics": [SWAP_TOPIC], "data": "0x"})

    assert decoder.decode("aerodrome_cl_pool", truncated) is None
