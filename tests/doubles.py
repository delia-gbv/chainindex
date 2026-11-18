"""Test doubles: a chain held in memory that answers JSON-RPC, and a ChainStore held in memory."""

from typing import Any

from chainindex.config import ChainConfig
from chainindex.models import Header, Log
from chainindex.rpc import RpcError
from chainindex.store import (
    AuditRow,
    CallRow,
    ContractRow,
    Cursor,
    EventTransactionRow,
    TransactionRow,
)


def word(value: int | str) -> str:
    """Encodes an int or address as a 32-byte topic."""
    if isinstance(value, str):
        return "0x" + value[2:].lower().rjust(64, "0")
    return "0x" + format(value % (1 << 256), "064x")


def address(number: int) -> str:
    """Returns a recognizable address for a small number."""
    return f"0x{number:040x}"


# Two kinds tracked as created: pools a factory announces, and gauges a voter announces.
CREATED_CONTRACTS: list[dict[str, Any]] = [
    {
        "name": "factory", "abi": "UniswapV3Factory", "addresses": [{"address": address(0xF), "start_block": 5}],
        "creates": {"event": "PoolCreated", "param": "pool", "contract": "pool"},
    },
    {
        "name": "voter", "abi": "AerodromeVoter", "events": ["GaugeCreated"],
        "addresses": [{"address": address(0xE), "start_block": 50}],
        "creates": {"event": "GaugeCreated", "param": "gauge", "contract": "gauge"},
    },
    {"name": "pool", "abi": "UniswapV3Pool", "track": "created", "fetch": "topics"},
    {"name": "gauge", "abi": "AerodromeCLGauge", "track": "created", "fetch": "topics"},
]


def chain_config(contracts: list[dict[str, Any]], **overrides: Any) -> ChainConfig:
    """Builds a small test chain with the given contract kinds."""
    document: dict[str, Any] = {
        "name": "testnet",
        "chain_id": 31337,
        "block_seconds": 1,
        "rpc": {"http": "http://node/{key}", "ws": "ws://node/{key}", "addresses_per_request": 2, "max_blocks_per_request": 100},
        "ledger": {"confirmations": 10, "poll_seconds": 0, "tip_window_blocks": 50, "backfill_window_blocks": 200},
        "live": {"ring_seconds": 100, "seal_seconds": 0},
        "contracts": contracts,
    }
    document.update(overrides)
    return ChainConfig.model_validate(document)


class MockChain:
    """A chain in memory: blocks have deterministic hashes unless forked, and logs are served by filter."""

    def __init__(self, head: int, *, lane: str = "ledger"):
        self.lane = lane
        self.head = head
        self.logs: list[dict[str, Any]] = []
        self.forks: dict[int, str] = {}
        self.requests: list[tuple[str, Any]] = []
        self.transactions: list[dict[str, Any]] = []
        # Native coins that arrive without a transaction to the account: (account, block, wei).
        self.internal_payments: list[tuple[str, int, int]] = []
        self.start_balances: dict[str, int] = {}

    def block_hash(self, number: int) -> str:
        """Returns the canonical hash of a block."""
        return self.forks.get(number, "0x" + format(number, "064x"))

    def header(self, number: int) -> Header:
        """Returns a canonical header."""
        return Header.from_rpc(self._header(number))

    def _header(self, number: int) -> dict[str, Any]:
        return {
            "number": hex(number),
            "hash": self.block_hash(number),
            "parentHash": self.block_hash(number - 1),
            "timestamp": hex(1_700_000_000 + number),
        }

    def add_log(
        self, contract_address: str, block: int, topics: list[str], data: str = "0x", *, transaction_hash: str | None = None
    ) -> Log:
        """Adds a log to a block and returns it as the service would parse it."""
        index = sum(1 for item in self.logs if int(item["blockNumber"], 16) == block)
        item = {
            "address": contract_address,
            "topics": topics,
            "data": data,
            "blockNumber": hex(block),
            "blockHash": self.block_hash(block),
            "blockTimestamp": hex(1_700_000_000 + block),
            "transactionHash": transaction_hash or "0x" + format(block * 1000 + index, "064x"),
            "transactionIndex": hex(index),
            "logIndex": hex(index),
            "removed": False,
        }
        self.logs.append(item)
        return Log.from_rpc(item)

    def add_transaction(
        self, sender: str, recipient: str, block: int, *, value: int = 0, status: int = 1, gas_used: int = 100_000,
        price: int = 10, l1_fee: int | None = 5, priority: int | None = 3,
    ) -> str:
        """Adds a transaction with the sender's next nonce and returns its hash; ``priority=None`` makes it legacy."""
        nonce = sum(1 for item in self.transactions if item["from"] == sender)
        transaction_hash = "0x" + format(0xABC000000 + len(self.transactions), "064x")
        index = sum(1 for item in self.transactions if int(item["blockNumber"], 16) == block)
        self.transactions.append(
            {
                "hash": transaction_hash, "from": sender, "to": recipient, "nonce": hex(nonce), "value": hex(value),
                "blockNumber": hex(block), "blockHash": self.block_hash(block), "transactionIndex": hex(index),
                "status": hex(status), "gasUsed": hex(gas_used), "effectiveGasPrice": hex(price), "l1Fee": l1_fee,
                "gasPrice": hex(price), "gas": hex(2 * gas_used), "input": "0x04e45aaf" + "00" * 32,
                "type": "0x2" if priority is not None else "0x0",
                "maxFeePerGas": hex(price + 1) if priority is not None else None,
                "maxPriorityFeePerGas": hex(priority) if priority is not None else None,
            }
        )
        return transaction_hash

    def balance(self, account: str, block: int) -> int:
        """Returns an account's native balance at the end of a block."""
        balance = self.start_balances.get(account, 0)
        for item in self.transactions:
            if int(item["blockNumber"], 16) > block:
                continue
            succeeded = item["status"] == "0x1"
            if item["to"] == account and succeeded:
                balance += int(item["value"], 16)
            if item["from"] == account:
                balance -= int(item["gasUsed"], 16) * int(item["effectiveGasPrice"], 16) + (item["l1Fee"] or 0)
                balance -= int(item["value"], 16) if succeeded else 0
        return balance + sum(wei for paid, number, wei in self.internal_payments if paid == account and number <= block)

    def _transaction(self, item: dict[str, Any]) -> dict[str, Any]:
        keys = (
            "hash", "from", "to", "nonce", "value", "blockNumber", "blockHash", "transactionIndex", "gasPrice", "gas", "input",
            "type", "maxFeePerGas", "maxPriorityFeePerGas",
        )
        return {key: item[key] for key in keys}

    def _receipt(self, item: dict[str, Any]) -> dict[str, Any]:
        receipt = {
            "transactionHash": item["hash"], "blockNumber": item["blockNumber"], "blockHash": item["blockHash"],
            "transactionIndex": item["transactionIndex"], "status": item["status"], "gasUsed": item["gasUsed"],
            "effectiveGasPrice": item["effectiveGasPrice"], "from": item["from"], "to": item["to"], "logs": [],
        }
        if item["l1Fee"] is not None:
            receipt["l1Fee"] = hex(item["l1Fee"])
        return receipt

    def fork(self, number: int, new_hash: str) -> None:
        """Replaces a block's hash, re-hashing its logs, as a reorg would."""
        self.forks[number] = new_hash
        for item in self.logs:
            if int(item["blockNumber"], 16) == number:
                item["blockHash"] = new_hash

    def _matching(self, log_filter: dict[str, Any]) -> list[dict[str, Any]]:
        start = int(log_filter["fromBlock"], 16)
        end = int(log_filter["toBlock"], 16)
        addresses = {item.lower() for item in log_filter.get("address") or []}
        positions = log_filter.get("topics") or []

        def topics_match(topics: list[str]) -> bool:
            # Each position accepts its alternatives, or anything when it is None.
            for index, accepted in enumerate(positions):
                if accepted is None:
                    continue
                alternatives = [accepted] if isinstance(accepted, str) else accepted
                if len(topics) <= index or topics[index] not in alternatives:
                    return False
            return True

        return [
            item for item in self.logs
            if start <= int(item["blockNumber"], 16) <= min(end, self.head)
            and (not addresses or item["address"].lower() in addresses)
            and topics_match(item["topics"])
        ]

    async def batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        """Answers JSON-RPC calls."""
        answers: list[Any] = []
        for method, params in calls:
            self.requests.append((method, params))
            if method == "eth_blockNumber":
                answers.append(hex(self.head))
            elif method == "eth_getLogs":
                answers.append(self._matching(params[0]))
            elif method == "eth_getBlockByNumber":
                tag = params[0]
                number = self.head if tag == "latest" else int(tag, 16)
                header = self._header(number) if number <= self.head else None
                if header is not None and len(params) > 1 and params[1]:
                    header["transactions"] = [
                        self._transaction(item) for item in self.transactions if int(item["blockNumber"], 16) == number
                    ]
                answers.append(header)
            elif method in ("eth_getTransactionByHash", "eth_getTransactionReceipt"):
                item = next((item for item in self.transactions if item["hash"] == params[0]), None)
                shape = self._transaction if method == "eth_getTransactionByHash" else self._receipt
                answers.append(shape(item) if item is not None else None)
            elif method == "eth_getTransactionCount":
                answers.append(hex(sum(
                    1 for item in self.transactions if item["from"] == params[0] and int(item["blockNumber"], 16) <= int(params[1], 16)
                )))
            elif method == "eth_getBalance":
                answers.append(hex(self.balance(params[0], int(params[1], 16))))
            else:
                answers.append(RpcError(-32601, f"{method} not served", method))
        return answers

    async def request(self, method: str, params: list[Any]) -> Any:
        """Answers one JSON-RPC call."""
        (answer,) = await self.batch([(method, params)])
        if isinstance(answer, RpcError):
            raise answer
        return answer

    async def block_number(self) -> int:
        """Returns the head."""
        return self.head

    def get_logs_requests(self) -> list[dict[str, Any]]:
        """Returns the filters of every eth_getLogs served so far."""
        return [params[0] for method, params in self.requests if method == "eth_getLogs"]


class MockStore:
    """The ChainStore interface, in memory."""

    def __init__(self) -> None:
        self.logs: dict[tuple[int, int, str], Log] = {}
        self.lanes: dict[tuple[int, int, str], str] = {}
        self.contract_rows: list[ContractRow] = []
        self.cursor_rows: dict[str, Cursor] = {}
        self.audit_rows: list[AuditRow] = []
        self.orphan_rows: list[tuple[int, str, str]] = []
        self.headers: list[Header] = []
        self.arrivals: dict[tuple[int, str], float] = {}
        self.calls: list[CallRow] = []
        self.transaction_rows: list[TransactionRow] = []
        self.event_transaction_rows: list[EventTransactionRow] = []

    def create(self) -> None:
        """Has nothing to create."""

    async def insert_logs(self, logs: list[Log], *, lane: str) -> tuple[int, int]:
        fresh = [log for log in logs if log.key not in self.logs]
        for log in fresh:
            self.logs[log.key] = log
            self.lanes[log.key] = lane
        return len(fresh), len(logs) - len(fresh)

    async def stored_logs(self, from_block: int, to_block: int) -> list[tuple[int, int, str, str, str]]:
        return [
            (log.block_number, log.log_index, log.block_hash, log.address, log.topics[0] if log.topics else "")
            for log in self.logs.values() if from_block <= log.block_number <= to_block
        ]

    async def stored_block_hashes(self, from_block: int, to_block: int) -> set[tuple[int, str]]:
        rows = [(log.block_number, log.block_hash) for log in self.logs.values()]
        rows += [(row.block_number, row.block_hash) for row in [*self.calls, *self.transaction_rows, *self.event_transaction_rows]]
        return {(number, block_hash) for number, block_hash in rows if from_block <= number <= to_block}

    async def record_contracts(self, rows: list[ContractRow]) -> None:
        self.contract_rows.extend(rows)

    async def contracts(self) -> list[ContractRow]:
        latest: dict[str, ContractRow] = {}
        for row in self.contract_rows:
            latest[row.address] = row
        return list(latest.values())

    async def cursors(self) -> dict[str, Cursor]:
        return {key: cursor.model_copy() for key, cursor in self.cursor_rows.items()}

    async def save_cursors(self, cursors: list[Cursor]) -> None:
        for cursor in cursors:
            self.cursor_rows[cursor.stream] = cursor.model_copy()

    async def record_audit(self, audit: AuditRow) -> None:
        self.audit_rows.append(audit)

    async def record_audits(self, audits: list[AuditRow]) -> None:
        self.audit_rows.extend(audits)

    async def orphans(self) -> set[tuple[int, str]]:
        return {(number, block_hash) for number, block_hash, _canonical in self.orphan_rows}

    async def record_orphans(self, orphans: list[tuple[int, str, str]], *, detected_by: str) -> None:
        self.orphan_rows.extend(orphans)

    async def insert_blocks(self, headers: list[Header], arrivals: dict[tuple[int, str], float] | None = None) -> int:
        stored = {(header.number, header.hash) for header in self.headers}
        headers = [header for header in headers if (header.number, header.hash) not in stored]
        self.headers.extend(headers)
        self.arrivals.update({key: value for key, value in (arrivals or {}).items() if key in {(h.number, h.hash) for h in headers}})
        return len(headers)

    async def max_block(self) -> int | None:
        return max((header.number for header in self.headers), default=None)

    async def insert_calls(self, rows: list[CallRow]) -> int:
        stored = {(row.read, row.block_number) for row in self.calls}
        fresh = [row for row in rows if (row.read, row.block_number) not in stored]
        self.calls.extend(fresh)
        return len(fresh)

    async def latest_calls(self, reads: list[str]) -> dict[tuple[str, str, str, str], tuple[bool, str]]:
        latest = {}
        for row in sorted(self.calls, key=lambda row: row.block_number):
            if row.read in reads:
                latest[(row.read, row.address, row.function, row.input)] = (row.success, row.output)
        return latest

    async def block_times(self, numbers: list[int]) -> dict[int, int]:
        stamped = {log.block_number: log.block_timestamp for log in self.logs.values() if log.block_timestamp}
        stamped |= {header.number: header.timestamp for header in self.headers}
        return {number: stamped[number] for number in numbers if number in stamped}

    async def account_log_transactions(self, account_word: str, from_block: int, to_block: int) -> list[str]:
        return sorted(
            {
                log.transaction_hash for log in self.logs.values()
                if from_block <= log.block_number <= to_block and account_word in log.topics[1:4]
            }
        )

    async def account_unwraps(self, account_word: str, unwrap_topic: str, from_block: int, to_block: int) -> list[tuple[int, int]]:
        return [
            (log.block_number, int(log.data, 16)) for log in self.logs.values()
            if from_block <= log.block_number <= to_block and log.topics[:2] == (unwrap_topic, account_word)
        ]

    async def insert_transactions(self, rows: list[TransactionRow]) -> int:
        stored = {row.transaction_hash for row in self.transaction_rows}
        fresh = [row for row in rows if row.transaction_hash not in stored]
        self.transaction_rows.extend(fresh)
        return len(fresh)

    async def event_log_transactions(
        self, topics: list[str], addresses: list[str] | None, from_block: int, to_block: int
    ) -> list[tuple[str, int, int]]:
        found: dict[str, tuple[int, int]] = {}
        for log in sorted(self.logs.values(), key=lambda log: log.block_number):
            listed = addresses is None or log.address in addresses
            if from_block <= log.block_number <= to_block and log.topic0 in topics and listed:
                found.setdefault(log.transaction_hash, (log.block_number, log.block_timestamp or 0))
        return sorted((transaction_hash, number, timestamp) for transaction_hash, (number, timestamp) in found.items())

    async def stored_event_transactions(self, from_block: int, to_block: int) -> set[str]:
        return {row.transaction_hash for row in self.event_transaction_rows if from_block <= row.block_number <= to_block}

    async def insert_event_transactions(self, rows: list[EventTransactionRow]) -> int:
        stored = {row.transaction_hash for row in self.event_transaction_rows}
        fresh = [row for row in rows if row.transaction_hash not in stored]
        self.event_transaction_rows.extend(fresh)
        return len(fresh)

    def addresses_with_logs(self) -> set[str]:
        """Returns every address that has a stored log."""
        return {log.address for log in self.logs.values()}

    def blocks_with_logs(self) -> list[int]:
        """Returns the block numbers of stored logs, in order."""
        return sorted(log.block_number for log in self.logs.values())
