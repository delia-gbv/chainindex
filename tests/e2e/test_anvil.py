"""Against a real anvil node and a real ClickHouse: confirmed logs are written once, a reorg deeper than the
confirmations is found and repaired by the verifier, and a restart writes nothing twice."""

import json
import os
import shutil
import socket
import subprocess
import time
import urllib.request
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from eth_abi import encode
from eth_utils import function_signature_to_4byte_selector

from chainindex.clickhouse import get_client
from chainindex.config import Settings
from chainindex.metrics import Metrics
from chainindex.models import Log
from chainindex.service import ChainService
from chainindex.store import Cursor

from ..test_ledger import settle
from ..test_verify import verify_all

pytestmark = pytest.mark.e2e

ABIS = Path(__file__).parent / "abis"
BYTECODE = json.loads((ABIS / "Emitter.json").read_text())["bytecode"]["object"]
PING_SELECTOR = "0x" + function_signature_to_4byte_selector("ping(uint256,uint256)").hex()
CONFIRMATIONS = 2
# With one-second blocks the verifier checks a block once it is this many blocks behind the ledger's tip.
DEPTH_BLOCKS = 3
# Anvil answers as fast as we ask, so no lane should pace the test.
UNPACED = {"requests_per_second": 1000, "max_concurrency": 4}


class Node:
    """A local anvil node, spoken to over plain JSON-RPC."""

    def __init__(self, url: str):
        self.url = url

    def call(self, method: str, *params: Any) -> Any:
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)}).encode()
        request = urllib.request.Request(self.url, body, {"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10) as response:
            answer = json.loads(response.read())
        if "error" in answer:
            raise RuntimeError(f"{method} failed: {answer['error']}")
        return answer["result"]

    @property
    def account(self) -> str:
        return self.call("eth_accounts")[0].lower()

    def send(self, transaction: dict[str, str]) -> dict[str, Any]:
        """Sends a transaction from the first dev account, which anvil mines into its own block, and returns the receipt."""
        transaction_hash = self.call("eth_sendTransaction", {"from": self.account, **transaction})
        # Anvil answers before the block holding it is sealed, so the receipt can lag the hash by a moment.
        for _attempt in range(100):
            receipt = self.call("eth_getTransactionReceipt", transaction_hash)
            if receipt is not None:
                return receipt
            time.sleep(0.02)
        raise RuntimeError(f"Transaction {transaction_hash} was never mined")

    def deploy(self) -> tuple[str, int]:
        """Deploys the Emitter and returns its address and block."""
        receipt = self.send({"data": BYTECODE})
        return receipt["contractAddress"].lower(), int(receipt["blockNumber"], 16)

    def ping(self, contract: str, round_number: int, value: int) -> int:
        """Emits one Ping and returns its block."""
        data = PING_SELECTOR + encode(["uint256", "uint256"], [round_number, value]).hex()
        return int(self.send({"to": contract, "data": data})["blockNumber"], 16)

    def mine(self, blocks: int) -> None:
        self.call("anvil_mine", hex(blocks))

    def block(self, number: int) -> dict[str, Any]:
        return self.call("eth_getBlockByNumber", hex(number), False)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def anvil_path() -> str | None:
    installed = Path.home() / ".foundry" / "bin" / "anvil"
    return shutil.which("anvil") or (str(installed) if installed.exists() else None)


@pytest.fixture(scope="session")
def clickhouse() -> None:
    host = os.environ.get("CLICKHOUSE_HOST")
    if not host:
        pytest.skip("CLICKHOUSE_HOST not set")
    if host not in ("localhost", "127.0.0.1"):
        pytest.skip("These tests drop their chain databases, so they only run against a local ClickHouse")
    port = os.environ.get("CLICKHOUSE_PORT", "8123")
    try:
        urllib.request.urlopen(f"http://{host}:{port}/ping", timeout=2).close()
    except OSError:
        pytest.skip(f"No ClickHouse answers on {host}:{port}")


@pytest.fixture
def node() -> Iterator[Node]:
    binary = anvil_path()
    if binary is None:
        pytest.skip("anvil is not installed (foundryup)")
    port = free_port()
    process = subprocess.Popen([binary, "--port", str(port), "--silent"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    node = Node(f"http://127.0.0.1:{port}")
    try:
        for _attempt in range(100):
            try:
                node.call("eth_chainId")
                break
            except OSError:
                time.sleep(0.1)
        else:
            raise RuntimeError("anvil did not start")
        yield node
    finally:
        process.terminate()
        process.wait(timeout=10)


@pytest.fixture
async def services(clickhouse, node, tmp_path, monkeypatch) -> AsyncIterator[list[ChainService]]:
    """Collects the services a test starts, so their HTTP sessions are closed after it."""
    monkeypatch.setattr("chainindex.service.Metrics", lambda name: Metrics(name, isolated=True))
    monkeypatch.setenv("CHAININDEX_CHAINS_DIR", str(tmp_path))
    monkeypatch.setenv("CHAININDEX_ABIS_DIR", str(ABIS))
    started: list[ChainService] = []
    yield started
    for service in started:
        for pool in service.pools.values():
            await pool.close()


def write_chain_file(folder: Path, name: str, node: Node, contract: str, start_block: int) -> None:
    chain = {
        "name": name,
        "chain_id": 31337,
        "block_seconds": 1,
        "rpc": {"http": node.url, "stream": UNPACED, "ledger": UNPACED, "backfill": UNPACED},
        "ledger": {"confirmations": CONFIRMATIONS, "poll_seconds": 0},
        "live": {"enabled": False},
        "verify": {"depth_seconds": DEPTH_BLOCKS, "history_pause_seconds": 0},
        "contracts": [{"name": "emitter", "abi": "Emitter", "addresses": [{"address": contract, "start_block": start_block}]}],
    }
    (folder / f"{name}.yaml").write_text(yaml.safe_dump(chain))


async def start(services: list[ChainService], name: str, *, fresh: bool = True) -> ChainService:
    """Builds the chain's service as ``chainindex run`` does, against the real node and ClickHouse, and prepares it."""
    settings = Settings(CHAIN=name, _env_file=None)
    if fresh:
        get_client(settings, database="default").command(f"DROP DATABASE IF EXISTS chain_{name}")
    service = ChainService(settings)
    services.append(service)
    await service.prepare()
    return service


def stored_logs(service: ChainService, *, canonical_only: bool = False) -> list[Log]:
    """Reads the stored logs back as Logs, leaving out orphaned blocks the way the dbt views do when asked."""
    database = service.store.database
    orphaned = (
        f"WHERE (block_number, block_hash) NOT IN (SELECT block_number, block_hash FROM {database}.orphaned_blocks)"
        if canonical_only else ""
    )
    result = service.store.client.query(
        "SELECT block_number, block_hash, toUnixTimestamp(block_time), transaction_hash, transaction_index, log_index, "
        f"address, topic0, topic1, topic2, topic3, data FROM {database}.logs {orphaned} ORDER BY block_number, log_index"
    )
    return [
        Log(
            block_number=number, block_hash=block_hash, block_timestamp=timestamp, transaction_hash=transaction_hash,
            transaction_index=transaction_index, log_index=log_index, address=address,
            topics=tuple(topic for topic in (topic0, topic1, topic2, topic3) if topic), data=data,
        )
        for number, block_hash, timestamp, transaction_hash, transaction_index, log_index, address, topic0, topic1, topic2, topic3, data
        in result.result_rows
    ]


def pings(service: ChainService, logs: list[Log]) -> list[dict[str, Any]]:
    """Decodes stored logs with the service's own decoder."""
    decoded = [service.registry.decoder.decode("emitter", log) for log in logs]
    return [{"block": item.log.block_number, "event": item.event, **item.args} for item in decoded if item is not None]


async def test_logs_are_written_only_once_confirmed_and_decode_to_what_the_contract_emitted(services, node, tmp_path):
    contract, deployed = node.deploy()
    blocks = [node.ping(contract, round_number, value) for round_number, value in ((1, 100), (2, 200), (3, 300))]
    write_chain_file(tmp_path, "e2e_written_once", node, contract, deployed)
    service = await start(services, "e2e_written_once")

    await settle(service.ledger)
    early = stored_logs(service)
    node.mine(CONFIRMATIONS)
    await settle(service.ledger)
    node.mine(10)
    await settle(service.ledger)
    stored = stored_logs(service)

    # The last two pings were not yet CONFIRMATIONS deep on the first pass.
    assert [log.block_number for log in early] == blocks[:1]
    assert pings(service, stored) == [
        {"block": block, "event": "Ping", "sender": node.account, "round": round_number, "value": value}
        for block, (round_number, value) in zip(blocks, ((1, 100), (2, 200), (3, 300)))
    ]
    assert [(log.block_hash, log.block_timestamp) for log in stored] == [
        (node.block(block)["hash"], int(node.block(block)["timestamp"], 16)) for block in blocks
    ]


async def test_a_reorg_deeper_than_the_confirmations_is_orphaned_and_only_the_canonical_events_stay_visible(services, node, tmp_path):
    contract, deployed = node.deploy()
    snapshot = node.call("evm_snapshot")
    replaced = [node.ping(contract, round_number, value) for round_number, value in ((1, 100), (2, 200), (3, 300))]
    node.mine(CONFIRMATIONS)
    write_chain_file(tmp_path, "e2e_reorg", node, contract, deployed)
    service = await start(services, "e2e_reorg")
    await settle(service.ledger)
    replaced_hashes = {block: node.block(block)["hash"] for block in replaced}

    # The chain drops every block since the deploy and grows a longer fork with other pings in the same heights.
    node.call("evm_revert", snapshot)
    canonical = [node.ping(contract, round_number, value) for round_number, value in ((1, 111), (2, 222))]
    node.mine(CONFIRMATIONS + DEPTH_BLOCKS + 5)
    await settle(service.ledger)
    before_verify = stored_logs(service)
    await service.verifier.load()
    await verify_all(service.verifier)

    orphans = service.store.client.query(
        "SELECT block_number, block_hash, canonical_hash FROM chain_e2e_reorg.orphaned_blocks ORDER BY block_number"
    ).result_rows
    audits = service.store.client.query("SELECT kind, repaired_logs FROM chain_e2e_reorg.audits").result_rows
    visible = pings(service, stored_logs(service, canonical_only=True))
    # The ledger had already passed these blocks, so only the verifier could see they changed.
    assert {(log.block_number, log.block_hash) for log in before_verify} == set(replaced_hashes.items())
    assert canonical == replaced[:2]
    assert orphans == [(block, replaced_hashes[block], node.block(block)["hash"]) for block in replaced]
    # The verifier had the auditor fetch the new fork's two pings.
    assert audits == [("verify", 2)]
    assert [(ping["block"], ping["round"], ping["value"]) for ping in visible] == [
        (canonical[0], 1, 111), (canonical[1], 2, 222)
    ]
    # Nothing is deleted: the replaced logs are still stored, beside the canonical ones.
    assert len(stored_logs(service)) == 5


async def test_a_restart_that_lost_its_cursors_refetches_the_range_and_writes_nothing_twice(services, node, tmp_path):
    contract, deployed = node.deploy()
    for round_number in range(1, 6):
        node.ping(contract, round_number, round_number * 10)
    node.mine(CONFIRMATIONS)
    write_chain_file(tmp_path, "e2e_restart", node, contract, deployed)
    first = await start(services, "e2e_restart")
    await settle(first.ledger)
    written = stored_logs(first)
    # As if the process died after inserting the logs but before its cursor reached ClickHouse.
    stream = first.registry.streams()[0]
    await first.store.save_cursors([Cursor(stream=stream.key, from_block=deployed, to_block=deployed - 1)])

    restarted = await start(services, "e2e_restart", fresh=False)
    await settle(restarted.ledger)

    # Whichever lane refetched the range (the tip lane takes along a stream just behind it), it wrote nothing again.
    already_stored = sum(
        restarted.metrics.registry.get_sample_value("chainindex_ledger_logs_already_stored_total", {"lane": lane}) or 0
        for lane in ("tip", "backfill")
    )
    assert len(written) == 5
    assert stored_logs(restarted) == written
    assert already_stored == 5
    assert (await restarted.store.cursors())[stream.key].to_block == restarted.ledger.tip_block
