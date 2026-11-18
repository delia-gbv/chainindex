"""Rebuilds base_logs.json: up to three real logs of each event the tests decode, from Aerodrome's WETH/USDC pools and
two of its factories on Base, taken from recent blocks of a public RPC (``uv run python tests/fixtures/make_base_logs.py``)."""

import json
import urllib.request
from pathlib import Path

RPC = "https://base-rpc.publicnode.com"
WANTED = {
    # Slipstream (concentrated liquidity) WETH/USDC, tick spacings 100 and 1: Swap, Mint, Burn, Collect.
    "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59": ["0xc42079f9", "0x7a53080b", "0x0c396cd9", "0x70935338"],
    "0x4e392fbfe4d0557c82d2f97f02ec39daa31516dd": ["0xc42079f9", "0x7a53080b", "0x0c396cd9", "0x70935338"],
    # The basic WETH/USDC pool: Swap, Sync, Fees.
    "0xcdac0d6c6c59727a65f871236188350531885c43": ["0xb3e27736", "0xcf2aa508", "0x112c2569"],
    # The newest Slipstream factory and the basic pool factory: PoolCreated.
    "0xf8f2eb4940cfe7d13603dddd87f123820fc061ef": ["0xab0d57f0"],
    "0x420dd381b31aef6683db6b902084cb0ffece40da": ["0x2128d88d"],
}
PER_EVENT = 3
WINDOW = 1000


def rpc(method: str, params: list) -> dict:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(RPC, body, {"Content-Type": "application/json", "User-Agent": "chainindex-fixture"})
    answer = json.load(urllib.request.urlopen(request, timeout=60))
    if "error" in answer:
        raise RuntimeError(f"{method} failed: {answer['error']}")
    return answer["result"]


def main() -> None:
    head = int(rpc("eth_blockNumber", []), 16) - 100
    found: dict[tuple[str, str], list[dict]] = {(address, topic): [] for address, topics in WANTED.items() for topic in topics}
    end = head
    while any(len(logs) < PER_EVENT for logs in found.values()) and head - end < 400_000:
        start = end - WINDOW + 1
        logs = rpc("eth_getLogs", [{"fromBlock": hex(start), "toBlock": hex(end), "address": list(WANTED)}])
        for log in logs:
            key = (log["address"].lower(), log["topics"][0][:10])
            if key in found and len(found[key]) < PER_EVENT:
                found[key].append(log)
        end = start - 1
    missing = sorted(key for key, logs in found.items() if not logs)
    if missing:
        print(f"No logs found for {missing}")
    logs = sorted((log for logs in found.values() for log in logs), key=lambda log: (int(log["blockNumber"], 16), int(log["logIndex"], 16)))
    Path(__file__).with_name("base_logs.json").write_text(json.dumps(logs, indent=1) + "\n")
    print(f"{len(logs)} logs from blocks {logs[0]['blockNumber']} to {logs[-1]['blockNumber']}")


if __name__ == "__main__":
    main()
