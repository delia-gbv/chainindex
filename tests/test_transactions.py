"""The transactions lane stores every transaction of an account: named by logs, found by nonce, or found by balance."""

import pytest

from chainindex.abi import topic
from chainindex.config import account_word
from chainindex.fetch import LogFetcher
from chainindex.ledger import Ledger
from chainindex.metrics import Metrics
from chainindex.registry import Registry
from chainindex.transactions import UNWRAP, Transactions, fee_of

from .doubles import MockChain, MockStore, address, chain_config, word

WALLET = address(0x77)
SOMEONE = address(0x55)
ROUTER = address(0x99)
USDC = address(0xC)
WETH = address(0xE)
TRANSFER = topic("Transfer(address,address,uint256)")
TOKENS = {
    "name": "token", "abi": "WETH9", "account_topics": [1, 2], "start_block": 100,
    "addresses": [{"address": USDC}, {"address": WETH}],
}


async def build(chain: MockChain, store: MockStore) -> Transactions:
    config = chain_config([TOKENS], accounts=[{"address": WALLET}])
    registry = Registry(config)
    await store.record_contracts(registry.reconcile(await store.contracts()))
    metrics = Metrics("test", isolated=True)
    ledger = Ledger(
        config, registry, store,
        tip_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        backfill_fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100),
        head_pool=chain, metrics=metrics,
    )
    await ledger.load()
    for _round in range(100):
        tip_behind = await ledger.tip_step()
        backfilled = await ledger.backfill_step()
        if not tip_behind and not backfilled:
            break
    transactions = Transactions(config, registry, store, ledger, pool=chain)
    await transactions.load()
    return transactions


async def run(transactions: Transactions) -> None:
    while await transactions.step():
        pass


async def test_a_logged_transaction_a_reverted_one_and_a_plain_payment_in_are_all_stored():
    chain = MockChain(head=400)
    chain.start_balances[WALLET] = 10**18
    swap = chain.add_transaction(WALLET, ROUTER, 150)
    chain.add_log(USDC, 150, [TRANSFER, word(WALLET), word(ROUTER)], word(5), transaction_hash=swap)
    reverted = chain.add_transaction(WALLET, ROUTER, 160, status=0)
    funding = chain.add_transaction(SOMEONE, WALLET, 170, value=10**16)
    store = MockStore()
    transactions = await build(chain, store)

    await run(transactions)

    found_by = {row.transaction_hash: row.found_by for row in store.transaction_rows}
    assert found_by == {swap: "logs", reverted: "nonce", funding: "balance"}
    audits = [row for row in store.audit_rows if row.kind == "transactions"]
    assert all("unexplained_wei=0" in row.detail for row in audits)


async def test_an_unwrap_explains_the_coins_it_pays_without_a_search():
    chain = MockChain(head=400)
    unwrap = chain.add_transaction(WALLET, WETH, 150)
    chain.add_log(WETH, 150, [UNWRAP, account_word(WALLET)], word(3 * 10**15), transaction_hash=unwrap)
    chain.internal_payments.append((WALLET, 150, 3 * 10**15))
    chain.start_balances[WALLET] = 10**18
    store = MockStore()
    transactions = await build(chain, store)

    await run(transactions)

    assert [row.found_by for row in store.transaction_rows] == ["logs"]
    assert all("unexplained_wei=0" in row.detail for row in store.audit_rows if row.kind == "transactions")


async def test_coins_a_contract_pays_without_a_log_are_left_in_an_audit_row_and_the_lane_moves_on():
    chain = MockChain(head=400)
    chain.internal_payments.append((WALLET, 250, 7))
    store = MockStore()
    transactions = await build(chain, store)

    await run(transactions)

    audits = [row for row in store.audit_rows if row.kind == "transactions"]
    assert any("unexplained_wei=7" in row.detail for row in audits)
    assert transactions.cursors[f"transactions:{WALLET}"].to_block == 390


async def test_the_lane_never_reads_past_what_the_account_streams_have_stored():
    chain = MockChain(head=400)
    store = MockStore()
    transactions = await build(chain, store)
    covered = transactions.covered()

    await run(transactions)

    assert covered == (100, 390)
    assert transactions.cursors[f"transactions:{WALLET}"].to_block == 390


@pytest.mark.parametrize(
    "receipt, expected",
    [
        ({"gasUsed": "0x64", "effectiveGasPrice": "0xa", "l1Fee": "0x5"}, (1005, 5)),
        ({"gasUsed": "0x64", "effectiveGasPrice": "0xa"}, (1000, 0)),
    ],
    ids=["op-stack pays an L1 data fee", "arbitrum counts L1 inside gas used"],
)
def test_the_fee_is_gas_at_the_effective_price_plus_any_l1_data_fee(receipt, expected):
    assert fee_of(receipt) == expected
