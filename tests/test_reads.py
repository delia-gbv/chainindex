"""The reads lane calls every address of a kind at confirmed blocks through Multicall3 and stores raw results that
changed."""

import pytest
from eth_abi import decode, encode

from chainindex.metrics import Metrics
from chainindex.reads import (
    MULTICALL3,
    Reader,
    decode_aggregate3,
    encode_aggregate3,
    int_word,
    read_cursor_key,
)
from chainindex.registry import Registry
from chainindex.rpc import RpcError

from .doubles import MockChain, MockStore, address, chain_config

POOL_A = address(0xA)
POOL_B = address(0xB)
SLOT0 = "0x3850c7bd"
LIQUIDITY = "0x1a686502"
TICK_SPACING = "0xd0c93a7c"
TICKS = "0xf30dba93"


class MockStateChain(MockChain):
    """A chain that also answers Multicall3 calls from a table of return data by (address, calldata prefix)."""

    def __init__(self, head: int, answers: dict[tuple[str, str], bytes]):
        super().__init__(head)
        self.answers = answers
        self.calls_seen: list[tuple[int, str, str]] = []

    async def batch(self, calls):
        answers = []
        plain = []
        for method, params in calls:
            if method != "eth_call":
                plain.append((method, params))
                continue
            (triples,) = decode(["(address,bool,bytes)[]"], bytes.fromhex(params[0]["data"][10:]))
            results = []
            for target, _allow_failure, data in triples:
                calldata = "0x" + data.hex()
                self.calls_seen.append((int(params[1], 16), target.lower(), calldata))
                found = self.answers.get((target.lower(), calldata[:10]))
                results.append((found is not None, found or b""))
            answers.append(("eth_call", "0x" + encode(["(bool,bytes)[]"], [results]).hex()))
        plain_answers = iter(await super().batch(plain))
        return [next(plain_answers) if answer is None else answer[1] for answer in [None if method != "eth_call" else answers.pop(0) for method, _params in calls]]


def slot0(price: int, tick: int) -> bytes:
    return encode(["uint160", "int24", "uint16", "uint16", "uint16", "bool"], [price, tick, 0, 0, 0, True])


STATE_READ = {
    "name": "state", "contract": "pool", "every_blocks": 5,
    "calls": ["slot0() returns (uint160 sqrt_price_x96, int24 tick)", "liquidity() returns (uint128 liquidity)"],
}
POOLS = [{"name": "pool", "abi": "AerodromeCLPool", "start_block": 1, "addresses": [{"address": POOL_A}, {"address": POOL_B}]}]


def build(chain: MockChain, reads: list[dict], **ledger) -> tuple[Reader, MockStore]:
    config = chain_config(POOLS, reads=reads, **({"ledger": {"confirmations": 10, **ledger}} if ledger else {}))
    registry = Registry(config)
    registry.reconcile([])
    store = MockStore()
    return Reader(config, registry, store, pool=chain, metrics=Metrics("test", isolated=True)), store


def test_aggregate3_round_trips_calls_and_results():
    encoded = encode_aggregate3([(POOL_A, SLOT0)])
    (triples,) = decode(["(address,bool,bytes)[]"], bytes.fromhex(encoded[10:]))
    results = decode_aggregate3("0x" + encode(["(bool,bytes)[]"], [[(True, b"\x01"), (False, b"")]]).hex())

    assert [(target.lower(), allow, data.hex()) for target, allow, data in triples] == [(POOL_A, True, SLOT0[2:])]
    assert results == [(True, "0x01"), (False, "0x")]


async def test_a_read_stores_one_raw_row_per_address_and_call_with_the_blocks_hash_and_time():
    chain = MockStateChain(head=100, answers={(POOL_A, SLOT0): slot0(2**96, -5), (POOL_A, LIQUIDITY): encode(["uint128"], [7])})
    reader, _store = build(chain, [STATE_READ])

    rows = await reader.read_block(reader.config.reads[0], 90)

    assert [(row.address, row.function, row.success) for row in rows] == [
        (POOL_A, "slot0", True), (POOL_A, "liquidity", True), (POOL_B, "slot0", False), (POOL_B, "liquidity", False),
    ]
    assert {(row.block_number, row.block_hash, row.block_time) for row in rows} == {(90, chain.block_hash(90), 1_700_000_090)}
    assert {block for block, _target, _data in chain.calls_seen} == {90}


async def test_a_fresh_read_starts_at_the_confirmed_block_and_keeps_to_its_cadence():
    chain = MockStateChain(head=103, answers={})
    reader, store = build(chain, [STATE_READ])

    await reader.step()
    chain.head = 127
    await reader.step()

    assert sorted({block for block, _target, _data in chain.calls_seen}) == [90, 95, 100, 105, 110, 115]
    assert reader.cursors[read_cursor_key(reader.config.reads[0])].to_block == 115


async def test_after_long_downtime_a_read_skips_what_is_older_than_its_catch_up_limit():
    chain = MockStateChain(head=100, answers={})
    reader, store = build(chain, [{**STATE_READ, "every_blocks": 1, "max_catch_up_blocks": 3}])
    await reader.step()

    chain.head = 1000
    await reader.step()

    assert sorted({block for block, _target, _data in chain.calls_seen}) == [90, 987, 988, 989, 990]
    assert reader.metrics.reads_skipped_blocks.labels(read="state")._value.get() == 896


async def test_a_tick_map_reads_the_spacing_once_and_the_ticks_around_each_price():
    answers = {(POOL_A, SLOT0): slot0(2**96, -125), (POOL_A, TICK_SPACING): encode(["int24"], [50])}
    answers[(POOL_A, TICKS)] = encode(["uint128", "int128", "int128"], [10, -3, 0])
    chain = MockStateChain(head=100, answers=answers)
    reader, _store = build(chain, [{"name": "ticks", "contract": "pool", "tick_map": {"spacings": 1}}])
    read = reader.config.reads[0]

    rows = await reader.read_block(read, 90)
    await reader.read_block(read, 91)

    # The tick -125 sits in the spacing that starts at -150.
    assert [row.input for row in rows] == [TICKS + int_word(tick) for tick in (-200, -150, -100)]
    assert sum(1 for _block, target, data in chain.calls_seen if (target, data) == (POOL_A, TICK_SPACING)) == 1
    assert {row.address for row in rows} == {POOL_A}


async def test_reads_restarted_after_a_crash_store_nothing_twice():
    chain = MockStateChain(head=100, answers={})
    reader, store = build(chain, [STATE_READ])
    await reader.step()
    first = len(store.calls)
    reader.cursors.clear()

    await reader.step()

    assert len(store.calls) == first
    assert MULTICALL3 == "0xca11bde05977b3631167028862be2a173976ca11"


async def test_call_reads_due_at_the_same_block_share_one_request():
    chain = MockStateChain(head=100, answers={(POOL_A, SLOT0): slot0(2**96, 1)})
    second = {"name": "other", "contract": "pool", "every_blocks": 5, "calls": ["fee() returns (uint24 fee)"]}
    reader, store = build(chain, [STATE_READ, second])

    await reader.step()

    # One header per request: both reads at block 90 went out together.
    header_requests = [params for method, params in chain.requests if method == "eth_getBlockByNumber"]
    assert header_requests == [[hex(90), False]]
    assert {row.read for row in store.calls} == {"state", "other"}


class MockForgetfulChain(MockStateChain):
    """A chain whose node has dropped the state of every block below ``oldest``."""

    def __init__(self, head: int, oldest: int):
        super().__init__(head, answers={})
        self.oldest = oldest

    async def batch(self, calls):
        block = int(calls[0][1][0], 16)
        if block < self.oldest:
            return [None, *(RpcError(-32000, "historical state 0xabc is not available", "eth_call") for _call in calls[1:])]
        return await super().batch(calls)


async def test_a_read_skips_blocks_whose_state_the_node_dropped_and_moves_on():
    chain = MockForgetfulChain(head=100, oldest=0)
    reader, store = build(chain, [{**STATE_READ, "max_catch_up_blocks": 100}])
    await reader.step()

    chain.head, chain.oldest = 160, 140
    await reader.step()

    read = reader.config.reads[0]
    assert sorted({block for block, _target, _data in chain.calls_seen}) == [90, 140, 145, 150]
    assert reader.cursors[read_cursor_key(read)].to_block == 150
    assert reader.metrics.reads_skipped_blocks.labels(read="state")._value.get() == 45


async def test_a_call_is_stored_only_when_its_result_differs_from_the_last_stored_one():
    chain = MockStateChain(head=100, answers={(POOL_A, SLOT0): slot0(2**96, 1), (POOL_A, LIQUIDITY): encode(["uint128"], [7])})
    reader, store = build(chain, [STATE_READ])
    await reader.step()
    first = len(store.calls)

    chain.answers[(POOL_A, LIQUIDITY)] = encode(["uint128"], [8])
    chain.head = 105
    await reader.step()
    chain.head = 110
    await reader.step()

    assert first == 4
    assert [(row.block_number, row.address, row.function) for row in store.calls[first:]] == [(95, POOL_A, "liquidity")]


async def test_the_read_cursor_moves_when_nothing_changed_so_consumers_know_the_values_are_current():
    chain = MockStateChain(head=100, answers={(POOL_A, SLOT0): slot0(2**96, 1)})
    reader, store = build(chain, [STATE_READ])
    await reader.step()
    stored = len(store.calls)

    chain.head = 120
    await reader.step()

    assert len(store.calls) == stored
    assert store.cursor_rows[read_cursor_key(reader.config.reads[0])].to_block == 110


async def test_a_restarted_reader_compares_with_the_last_results_stored():
    chain = MockStateChain(head=100, answers={(POOL_A, SLOT0): slot0(2**96, 1)})
    reader, store = build(chain, [STATE_READ])
    await reader.step()
    stored = len(store.calls)

    restarted = Reader(reader.config, reader.registry, store, pool=chain, metrics=Metrics("test", isolated=True))
    await restarted.load()
    chain.head = 120
    await restarted.step()

    assert len(store.calls) == stored


async def test_a_tick_map_stores_a_tick_only_when_its_result_changes():
    answers = {(POOL_A, SLOT0): slot0(2**96, -125), (POOL_A, TICK_SPACING): encode(["int24"], [50])}
    answers[(POOL_A, TICKS)] = encode(["uint128", "int128", "int128"], [10, -3, 0])
    chain = MockStateChain(head=100, answers=answers)
    reader, store = build(chain, [{"name": "ticks", "contract": "pool", "every_blocks": 5, "tick_map": {"spacings": 1}}])
    await reader.step()
    first = len(store.calls)

    # One spacing up: -150 and -100 are read again unchanged, -50 is new.
    chain.answers[(POOL_A, SLOT0)] = slot0(2**96, -75)
    chain.head = 105
    await reader.step()

    assert first == 3
    assert [(row.block_number, row.input) for row in store.calls[first:]] == [(95, TICKS + int_word(-50))]


class MockFailingStore(MockStore):
    """A store whose first call insert fails, as a ClickHouse timeout would."""

    def __init__(self) -> None:
        super().__init__()
        self.failures = 1

    async def insert_calls(self, rows):
        if self.failures:
            self.failures -= 1
            raise TimeoutError("ClickHouse did not answer")
        return await super().insert_calls(rows)


async def test_changes_whose_insert_failed_are_stored_on_the_next_step():
    chain = MockStateChain(head=100, answers={(POOL_A, SLOT0): slot0(2**96, 1)})
    reader, _store = build(chain, [STATE_READ])
    reader.store = MockFailingStore()

    with pytest.raises(TimeoutError):
        await reader.step()
    await reader.step()

    assert [(row.block_number, row.address, row.function) for row in reader.store.calls] == [
        (90, POOL_A, "slot0"), (90, POOL_A, "liquidity"), (90, POOL_B, "slot0"), (90, POOL_B, "liquidity"),
    ]
