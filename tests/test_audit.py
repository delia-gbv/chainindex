"""The auditor compares a window's stored logs with the RPC's and inserts what ClickHouse is missing."""

from chainindex.abi import topic
from chainindex.audit import Auditor
from chainindex.fetch import LogFetcher
from chainindex.ledger import Ledger
from chainindex.metrics import Metrics

from .doubles import MockChain, MockStore, address, word
from .test_ledger import build, settle

TRANSFER = topic("Transfer(address,address,uint256)")
TRANSFERS = {"name": "transfers", "abi": "ERC20", "track": "any", "fetch": "topics", "start_block": 100}


def auditor_of(ledger: Ledger, chain: MockChain, store: MockStore) -> Auditor:
    return Auditor(
        ledger.config, ledger.registry, store, ledger,
        fetcher=LogFetcher(chain, addresses_per_request=2, max_blocks=100), pool=chain,
        metrics=Metrics("test", isolated=True),
    )


async def test_an_any_kinds_stored_logs_count_as_stored_whoever_emitted_them():
    chain, store = MockChain(head=1000), MockStore()
    chain.add_log(address(0x99), 150, [TRANSFER, word(address(1)), word(address(2))], word(1))
    ledger = await build([TRANSFERS], chain, store)
    await settle(ledger)
    assert store.blocks_with_logs() == [150]

    row = await auditor_of(ledger, chain, store).audit("historic", 101, 200)

    assert row is not None
    assert (row.rpc_logs, row.stored_logs, row.missing_logs, row.extra_logs) == (1, 1, 0, 0)


async def test_an_any_kinds_log_clickhouse_lacks_is_still_found_and_inserted():
    chain, store = MockChain(head=1000), MockStore()
    ledger = await build([TRANSFERS], chain, store)
    await settle(ledger)
    chain.add_log(address(0x99), 150, [TRANSFER, word(address(1)), word(address(2))], word(1))

    row = await auditor_of(ledger, chain, store).audit("historic", 101, 200)

    assert row is not None
    assert (row.missing_logs, row.repaired_logs) == (1, 1)
    assert store.blocks_with_logs() == [150]
