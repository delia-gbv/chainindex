"""Account kinds keep only the logs that name one of the chain's accounts, through the same streams as any kind."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from chainindex.abi import topic
from chainindex.config import account_word
from chainindex.fetch import LogFetcher
from chainindex.hub import Hub
from chainindex.ledger import Ledger
from chainindex.live import LiveChain, SocketFeed
from chainindex.metrics import Metrics
from chainindex.registry import Registry

from .doubles import MockChain, MockStore, address, chain_config, word

WALLET = address(0x77)
SOMEONE = address(0x55)
USDC = address(0xC)
AERO = address(0xD)
POOL = address(0xA)
TRANSFER = topic("Transfer(address,address,uint256)")
APPROVAL = topic("Approval(address,address,uint256)")
SYNC = topic("Sync(uint256,uint256)")
ACCOUNTS = [{"address": WALLET, "label": "books"}]
TOKENS = {
    "name": "token", "abi": "ERC20", "events": ["Transfer", "Approval"], "account_topics": [1, 2], "start_block": 100,
    "addresses": [{"address": USDC}, {"address": AERO}],
}
POOLS = {"name": "pool", "abi": "AerodromePool", "start_block": 100, "addresses": [{"address": POOL}]}


def transfer(chain: MockChain, token: str, block: int, sender: str, recipient: str, amount: int = 1) -> None:
    chain.add_log(token, block, [TRANSFER, word(sender), word(recipient)], word(amount))


def test_an_account_kind_asks_for_the_account_at_each_of_its_topic_positions():
    config = chain_config([TOKENS], accounts=ACCOUNTS)

    filters = config.topic_filters(config.contract("token"))

    assert filters == [
        [[TRANSFER, APPROVAL], [account_word(WALLET)]],
        [[TRANSFER, APPROVAL], None, [account_word(WALLET)]],
    ]


@pytest.mark.parametrize(
    "contract, accounts, complaint",
    [
        (TOKENS, [], "lists no accounts"),
        ({**TOKENS, "account_topics": [4]}, ACCOUNTS, "topic positions 1, 2 or 3"),
        ({**TOKENS, "fetch": "topics"}, ACCOUNTS, "listed kind fetched by address"),
        ({**TOKENS, "unfiltered_events": ["Approval"], "account_topics": []}, ACCOUNTS, "exceptions of a kind with account_topics"),
        ({**TOKENS, "unfiltered_events": ["MultiplierUpdated"]}, ACCOUNTS, "must be among its events"),
    ],
)
def test_an_account_kind_without_accounts_or_with_a_bad_position_is_refused(contract, accounts, complaint):
    with pytest.raises(ValidationError, match=complaint):
        chain_config([contract], accounts=accounts)


def test_account_streams_get_their_own_cursors_and_other_streams_keep_theirs():
    registry = Registry(chain_config([TOKENS, POOLS], accounts=ACCOUNTS))
    registry.reconcile([])
    other_accounts = Registry(chain_config([TOKENS, POOLS], accounts=[*ACCOUNTS, {"address": SOMEONE}]))
    other_accounts.reconcile([])

    keys = {stream.key for stream in registry.streams()}
    keys_with_another_account = {stream.key for stream in other_accounts.streams()}

    assert f"address:{POOL}" in keys
    assert any(key.startswith(f"address:{USDC}:accounts:") for key in keys)
    # A new account changes the question, so the account streams start over and the pool's cursor stays.
    assert keys & keys_with_another_account == {f"address:{POOL}"}


async def build_ledger(chain: MockChain, store: MockStore) -> Ledger:
    config = chain_config([TOKENS, POOLS], accounts=ACCOUNTS)
    registry = Registry(config)
    await store.record_contracts(registry.reconcile(await store.contracts()))
    ledger = Ledger(
        config, registry, store,
        tip_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        backfill_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        head_pool=chain, metrics=Metrics("test", isolated=True),
    )
    await ledger.load()
    return ledger


async def settle(ledger: Ledger) -> None:
    for _round in range(200):
        tip_behind = await ledger.tip_step()
        backfilled = await ledger.backfill_step()
        if not tip_behind and not backfilled:
            return
    raise AssertionError("The ledger never settled")


async def test_the_ledger_keeps_the_accounts_transfers_both_ways_and_nobody_elses():
    chain = MockChain(head=400)
    transfer(chain, USDC, 150, SOMEONE, WALLET)
    transfer(chain, AERO, 160, WALLET, SOMEONE)
    transfer(chain, USDC, 170, SOMEONE, address(0x66))
    chain.add_log(POOL, 180, [SYNC], word(5) + word(7)[2:])
    transfer(chain, USDC, 380, WALLET, POOL)
    store = MockStore()
    ledger = await build_ledger(chain, store)

    await settle(ledger)

    stored = sorted((log.block_number, log.address) for log in store.logs.values())
    assert stored == [(150, USDC), (160, AERO), (180, POOL), (380, USDC)]
    account_requests = [request for request in chain.get_logs_requests() if request.get("topics", [None])[0] == [TRANSFER, APPROVAL]]
    assert account_requests
    # Account streams never ride along in a pool's request, which would ask for everyone's logs.
    assert all(POOL not in request["address"] for request in account_requests)


def test_the_websockets_subscribe_to_each_account_position_separately():
    config = chain_config([TOKENS, POOLS], accounts=ACCOUNTS)
    registry = Registry(config)
    registry.reconcile([])
    chain = MockChain(head=400)
    metrics = Metrics("test", isolated=True)
    hub = Hub(ring_blocks=100, queue_size=1000, metrics=metrics)
    live = LiveChain(
        config, registry, hub, fetcher=LogFetcher(chain, addresses_per_request=8, max_blocks=100), pool=chain, metrics=metrics,
    )
    feed = SocketFeed("ws-0", "ws://node", live, metrics=metrics)

    filters = feed._log_filters()

    assert {"address": [POOL]} in filters
    assert {"address": [USDC, AERO], "topics": [[TRANSFER, APPROVAL], [account_word(WALLET)]]} in filters
    assert {"address": [USDC, AERO], "topics": [[TRANSFER, APPROVAL], None, [account_word(WALLET)]]} in filters


MULTIPLIER = topic("MultiplierUpdated(uint256)")


@pytest.fixture(autouse=True)
def rebasing_token_abi(monkeypatch):
    """Makes the test-only ABI of a token whose balances rebase by a multiplier loadable by name."""
    monkeypatch.setenv("CHAININDEX_ABIS_DIR", str(Path(__file__).parent / "fixtures" / "abis"))
REBASING_TOKENS = {**TOKENS, "abi": "RebasingToken", "events": ["Transfer", "Approval", "MultiplierUpdated"], "unfiltered_events": ["MultiplierUpdated"],
                "unfiltered_start_block": 50}


def test_unfiltered_events_get_a_whole_stream_per_address_from_their_own_start_and_leave_the_account_filter_alone():
    config = chain_config([REBASING_TOKENS], accounts=ACCOUNTS)
    registry = Registry(config)
    registry.reconcile([])

    whole = [stream for stream in registry.streams() if ":unfiltered:" in stream.key]

    assert config.topic_filters(config.contract("token")) == chain_config([TOKENS], accounts=ACCOUNTS).topic_filters(config.contract("token"))
    assert sorted(stream.addresses[0] for stream in whole) == sorted([USDC, AERO])
    assert all(stream.topics == [MULTIPLIER] and stream.filters is None and stream.start_block == 50 for stream in whole)


async def test_the_ledger_keeps_a_tokens_multiplier_change_that_names_nobody():
    chain = MockChain(head=400)
    chain.add_log(USDC, 60, [MULTIPLIER], word(10**18 + 5))
    transfer(chain, USDC, 170, SOMEONE, address(0x66))
    store = MockStore()
    config = chain_config([REBASING_TOKENS, POOLS], accounts=ACCOUNTS)
    registry = Registry(config)
    await store.record_contracts(registry.reconcile(await store.contracts()))
    ledger = Ledger(
        config, registry, store,
        tip_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        backfill_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        head_pool=chain, metrics=Metrics("test", isolated=True),
    )
    await ledger.load()

    await settle(ledger)

    assert [(log.block_number, log.topics[0]) for log in store.logs.values()] == [(60, MULTIPLIER)]
