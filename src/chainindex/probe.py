"""Checks what an endpoint allows before a chain file relies on it."""

import asyncio
import json
from typing import Any

from websockets.asyncio.client import connect

from chainindex.config import ChainConfig, LaneSpec
from chainindex.metrics import Metrics
from chainindex.rpc import RpcError, RpcPool, RpcUnavailable, Throttle

ADDRESS_COUNTS = (8, 16, 32, 64, 128)


def _filler_addresses(count: int) -> list[str]:
    return [f"0x{index + 1:040x}" for index in range(count)]


async def _get_logs(pool: RpcPool, log_filter: dict[str, Any]) -> tuple[list[dict[str, Any]] | None, str]:
    try:
        return await pool.request("eth_getLogs", [log_filter]), ""
    except (RpcError, RpcUnavailable) as exc:
        return None, str(exc)


async def topic_only_allowed(pool: RpcPool, topic: str, head: int) -> tuple[bool, str]:
    """Tells whether the endpoint answers an ``eth_getLogs`` with topics but no address."""
    logs, error = await _get_logs(pool, {"fromBlock": hex(head - 20), "toBlock": hex(head - 10), "topics": [[topic]]})
    return logs is not None, error


async def probe(config: ChainConfig, http_url: str, ws_url: str | None, *, metrics: Metrics) -> dict[str, Any]:
    """Reports the head, topic-only support, timestamps on logs, the address cap and websocket heads."""
    pool = RpcPool(
        "stream", http_url, LaneSpec(requests_per_second=2, max_concurrency=1),
        throttle=Throttle(), metrics=metrics, max_attempts=2,
    )
    report: dict[str, Any] = {}
    try:
        head = await pool.block_number()
        report["head"] = head
        topics = [topic for contract in config.contracts for topic in contract.topics()]
        if topics:
            allowed, error = await topic_only_allowed(pool, topics[0], head)
            report["topic_only"] = allowed if allowed else error
        else:
            report["topic_only"] = "no topics to check: no kind has an abi"
        listed = [entry.address for contract in config.contracts for entry in contract.addresses]
        logs, error = await _get_logs(pool, {"fromBlock": hex(head - 2000), "toBlock": hex(head - 10), "address": listed[:8]})
        report["block_timestamp_on_logs"] = (
            all("blockTimestamp" in item for item in logs) if logs else error or "no logs to check"
        )
        accepted = 0
        for count in ADDRESS_COUNTS:
            logs, error = await _get_logs(
                pool, {"fromBlock": hex(head - 20), "toBlock": hex(head - 10), "address": _filler_addresses(count)}
            )
            if logs is None:
                break
            accepted = count
        report["addresses_per_request"] = accepted
    finally:
        await pool.close()
    if ws_url is None:
        report["websocket_heads"] = "no websocket configured"
        return report
    try:
        async with connect(ws_url, open_timeout=10) as socket:
            await socket.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
            await asyncio.wait_for(socket.recv(), 10)
            message = json.loads(await asyncio.wait_for(socket.recv(), max(10.0, 5 * config.block_seconds)))
            report["websocket_heads"] = message.get("method") == "eth_subscription"
    except Exception as exc:
        report["websocket_heads"] = f"{type(exc).__name__}: {exc}"
    return report
