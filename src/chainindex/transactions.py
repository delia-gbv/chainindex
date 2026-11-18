"""Every transaction of the chain's accounts, with its receipt and fee, found from the chain alone.

Most are found from logs: the transaction of every stored log that names an account at topic 1, 2 or 3 (the account
kinds, a gauge's stakes). The rest name the account in no log: a reverted transaction, or a call that logs nothing
about it. The nonce finds those. The transactions an account sent between two blocks number exactly its nonce's rise,
so a missing nonce is bisected to its block and read from the block's transactions. Native coins that arrive without
a log (a plain transfer to the account) are found the same way from the balance: it must move by exactly what the
stored transactions and unwraps explain. Whatever is still unexplained is written to ``audits``.

The lane trails the account kinds' streams, so every log that names an account in a window is stored before the
window is read. It needs an endpoint that keeps state history: the nonce and balance are read at past blocks.
"""

import asyncio
import json
from typing import Any

from chainindex.abi import topic
from chainindex.config import ChainConfig, account_word
from chainindex.fetch import fetch_headers
from chainindex.ledger import Ledger
from chainindex.logger import get_logger
from chainindex.registry import Registry
from chainindex.rpc import RpcError, RpcPool, RpcUnavailable
from chainindex.store import AuditRow, ChainStore, Cursor, FoundBy, TransactionRow

logger = get_logger(__name__)

WINDOW_BLOCKS = 10_000
CALLS_PER_BATCH = 20
ERROR_PAUSE_SECONDS = 5.0
# Unexplained balance changes searched for in one window before the rest is left to the audit row.
MAX_BALANCE_SEARCHES = 10
# WETH9 pays native coins out on unwrap and logs it as Withdrawal(src, wad), not as a transaction to the account.
UNWRAP = topic("Withdrawal(address,uint256)")


def cursor_key(account: str) -> str:
    """Names an account's transactions cursor."""
    return f"transactions:{account}"


def fee_of(receipt: dict[str, Any]) -> tuple[int, int]:
    """Returns ``(fee, l1_fee)`` in wei: gas used at the effective price, plus the L1 data fee where the chain charges one.

    OP-stack receipts (Base) carry ``l1Fee``; Arbitrum chains' count L1 gas inside ``gasUsed`` and have none.
    """
    l1_fee = int(receipt.get("l1Fee") or "0x0", 16)
    return int(receipt["gasUsed"], 16) * int(receipt["effectiveGasPrice"], 16) + l1_fee, l1_fee


class Transactions:
    """Stores every transaction of every account, window by window, behind the account kinds' streams."""

    def __init__(self, config: ChainConfig, registry: Registry, store: ChainStore, ledger: Ledger, *, pool: RpcPool):
        self.config = config
        self.registry = registry
        self.store = store
        self.ledger = ledger
        self.pool = pool
        self.cursors: dict[str, Cursor] = {}

    async def load(self) -> None:
        """Reads the stored cursors."""
        stored = await self.store.cursors()
        for account in self.config.accounts:
            key = cursor_key(account.address)
            if key in stored:
                self.cursors[key] = stored[key]

    def covered(self) -> tuple[int, int] | None:
        """Returns ``(first, last)``: the blocks every account stream has stored, or None without account streams."""
        streams = [stream for stream in self.registry.streams() if stream.filters is not None]
        if not streams:
            return None
        cursors = [self.ledger.cursor(stream) for stream in streams]
        return min(stream.start_block for stream in streams), min(cursor.to_block for cursor in cursors)

    def cursor(self, account: str, first: int) -> Cursor:
        """Returns an account's cursor, empty just before the account streams' first block."""
        key = cursor_key(account)
        if key not in self.cursors:
            self.cursors[key] = Cursor(stream=key, from_block=first, to_block=first - 1)
        return self.cursors[key]

    async def step(self) -> bool:
        """Stores the next window of every account and returns whether any account is still behind its streams."""
        covered = self.covered()
        if covered is None:
            return False
        first, last = covered
        behind = False
        for account in self.config.accounts:
            cursor = self.cursor(account.address, first)
            start = cursor.to_block + 1
            end = min(last, start + WINDOW_BLOCKS - 1)
            if end < start:
                continue
            await self.store_window(account.address, start, end)
            cursor.to_block = end
            await self.store.save_cursors([cursor])
            behind = behind or end < last
        return behind

    async def store_window(self, account: str, start: int, end: int) -> list[TransactionRow]:
        """Finds, checks and stores an account's transactions in ``[start, end]`` and returns them."""
        word = account_word(account)
        hashes = await self.store.account_log_transactions(word, start, end)
        found: dict[str, tuple[dict[str, Any], FoundBy]] = {}
        for transaction in await self._by_hash(hashes):
            if account in (transaction["from"].lower(), (transaction.get("to") or "").lower()):
                found[transaction["hash"].lower()] = (transaction, "logs")
        nonce_before, nonce_after, balance_before, balance_after = await self._batch(
            [
                ("eth_getTransactionCount", [account, hex(start - 1)]),
                ("eth_getTransactionCount", [account, hex(end)]),
                ("eth_getBalance", [account, hex(start - 1)]),
                ("eth_getBalance", [account, hex(end)]),
            ]
        )
        sent_nonces = {int(transaction["nonce"], 16) for transaction, _how in found.values() if transaction["from"].lower() == account}
        for nonce in range(int(nonce_before, 16), int(nonce_after, 16)):
            if nonce in sent_nonces:
                continue
            transaction = await self._sent_with_nonce(account, nonce, start, end)
            found[transaction["hash"].lower()] = (transaction, "nonce")
        rows = await self._rows(account, found)
        unwraps = await self.store.account_unwraps(word, UNWRAP, start, end)
        balance = Balance(account, int(balance_before, 16), rows, unwraps)
        unexplained = balance.unexplained(end, int(balance_after, 16))
        searches = 0
        while unexplained and searches < MAX_BALANCE_SEARCHES:
            searches += 1
            block = await self._first_unexplained_block(balance, start, end)
            arrivals = await self._arrivals(account, block, found)
            if not arrivals:
                # Native coins a contract paid out, with no log to name them: an audit row says where.
                break
            rows.extend(await self._rows(account, arrivals))
            for transaction_hash, entry in arrivals.items():
                found[transaction_hash] = entry
            balance = Balance(account, int(balance_before, 16), rows, unwraps)
            unexplained = balance.unexplained(end, int(balance_after, 16))
        inserted = await self.store.insert_transactions(sorted(rows, key=lambda row: (row.block_number, row.transaction_index)))
        by_nonce = sum(1 for row in rows if row.found_by != "logs")
        if unexplained:
            logger.warning(f"Account {account}: blocks {start}-{end} leave {unexplained} wei of its balance change unexplained")
        await self.store.record_audit(
            AuditRow(
                kind="transactions", from_block=start, to_block=end, streams=1, rpc_logs=len(rows),
                stored_logs=len(rows) - inserted, missing_logs=by_nonce, extra_logs=0, repaired_logs=inserted,
                detail=(
                    f"account={account} sent={int(nonce_after, 16) - int(nonce_before, 16)} found_without_logs={by_nonce} "
                    f"unexplained_wei={unexplained}"
                ),
            )
        )
        if rows:
            logger.info(f"Account {account}: blocks {start}-{end}, {len(rows)} transaction(s), {inserted} new, {by_nonce} found without logs")
        return rows

    async def _batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        answers = []
        for part in range(0, len(calls), CALLS_PER_BATCH):
            for answer in await self.pool.batch(calls[part : part + CALLS_PER_BATCH]):
                if isinstance(answer, RpcError):
                    raise answer
                answers.append(answer)
        return answers

    async def _by_hash(self, hashes: list[str]) -> list[dict[str, Any]]:
        transactions = await self._batch([("eth_getTransactionByHash", [transaction_hash]) for transaction_hash in hashes])
        missing = [transaction_hash for transaction_hash, transaction in zip(hashes, transactions) if transaction is None]
        if missing:
            raise RpcUnavailable(f"The {self.pool.lane} lane does not know transaction {missing[0]}")
        return transactions

    async def _count_at(self, account: str, block: int) -> int:
        return int(await self.pool.request("eth_getTransactionCount", [account, hex(block)]), 16)

    async def _sent_with_nonce(self, account: str, nonce: int, start: int, end: int) -> dict[str, Any]:
        """Returns the transaction the account sent with ``nonce``: the first block past it, then that block's list."""
        low, high = start, end
        while low < high:
            middle = (low + high) // 2
            if await self._count_at(account, middle) > nonce:
                high = middle
            else:
                low = middle + 1
        block = await self.pool.request("eth_getBlockByNumber", [hex(low), True])
        for transaction in block["transactions"]:
            if transaction["from"].lower() == account and int(transaction["nonce"], 16) == nonce:
                return transaction
        raise RpcUnavailable(f"Block {low} holds no transaction from {account} with nonce {nonce}")

    async def _first_unexplained_block(self, balance: "Balance", start: int, end: int) -> int:
        """Bisects for the first block whose balance the stored transactions do not explain."""
        low, high = start, end
        while low < high:
            middle = (low + high) // 2
            actual = int(await self.pool.request("eth_getBalance", [balance.account, hex(middle)]), 16)
            if balance.unexplained(middle, actual):
                high = middle
            else:
                low = middle + 1
        return low

    async def _arrivals(
        self, account: str, block: int, known: dict[str, tuple[dict[str, Any], FoundBy]]
    ) -> dict[str, tuple[dict[str, Any], FoundBy]]:
        """Returns the block's transactions that paid the account native coins and are not known yet."""
        payload = await self.pool.request("eth_getBlockByNumber", [hex(block), True])
        return {
            transaction["hash"].lower(): (transaction, "balance")
            for transaction in payload["transactions"]
            if (transaction.get("to") or "").lower() == account and int(transaction["value"], 16) > 0
            and transaction["hash"].lower() not in known
        }

    async def _rows(self, account: str, found: dict[str, tuple[dict[str, Any], FoundBy]]) -> list[TransactionRow]:
        hashes = sorted(found)
        receipts = await self._batch([("eth_getTransactionReceipt", [transaction_hash]) for transaction_hash in hashes])
        blocks = sorted({int(receipt["blockNumber"], 16) for receipt in receipts})
        times = {header.number: header.timestamp for header in await fetch_headers(self.pool, blocks)}
        rows = []
        for transaction_hash, receipt in zip(hashes, receipts):
            transaction, found_by = found[transaction_hash]
            fee, l1_fee = fee_of(receipt)
            block = int(receipt["blockNumber"], 16)
            rows.append(
                TransactionRow(
                    account=account, block_number=block, block_hash=receipt["blockHash"].lower(), block_time=times[block],
                    transaction_hash=transaction_hash, transaction_index=int(receipt["transactionIndex"], 16),
                    sender=transaction["from"].lower(), recipient=(transaction.get("to") or "").lower(),
                    nonce=int(transaction["nonce"], 16), value=int(transaction["value"], 16), status=int(receipt["status"], 16),
                    gas_used=int(receipt["gasUsed"], 16), effective_gas_price=int(receipt["effectiveGasPrice"], 16),
                    l1_fee=l1_fee, fee=fee, found_by=found_by,
                    raw_transaction=json.dumps(transaction, sort_keys=True), raw_receipt=json.dumps(receipt, sort_keys=True),
                )
            )
        return rows

    async def run(self) -> None:
        """Stores transactions forever."""
        await self.load()
        while True:
            try:
                behind = await self.step()
            except Exception as exc:
                logger.error(f"Transactions step failed, retrying in {ERROR_PAUSE_SECONDS:.0f} s: {exc!r}")
                await asyncio.sleep(ERROR_PAUSE_SECONDS)
                continue
            if not behind:
                await asyncio.sleep(self.config.ledger.poll_seconds)


class Balance:
    """An account's native balance as its stored transactions and unwraps explain it, from a known starting balance."""

    def __init__(self, account: str, start_balance: int, rows: list[TransactionRow], unwraps: list[tuple[int, int]]):
        self.account = account
        self.start_balance = start_balance
        self.rows = rows
        self.unwraps = unwraps

    def expected(self, block: int) -> int:
        """Returns the balance at the end of ``block``: value in and unwraps, less value out and fees paid."""
        balance = self.start_balance
        for row in self.rows:
            if row.block_number > block:
                continue
            succeeded = row.status == 1
            if row.recipient == self.account and succeeded:
                balance += row.value
            if row.sender == self.account:
                balance -= row.fee + (row.value if succeeded else 0)
        return balance + sum(wei for number, wei in self.unwraps if number <= block)

    def unexplained(self, block: int, actual: int) -> int:
        """Returns how far the actual balance at ``block`` is from the explained one, in wei."""
        return actual - self.expected(block)
