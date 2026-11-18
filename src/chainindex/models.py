"""Raw chain records as the RPC returns them, normalized to lowercase hex and ints."""

from typing import Any

from pydantic import BaseModel, ConfigDict


def _int(value: Any) -> int:
    if isinstance(value, int):
        return value
    return int(value, 16)


class Log(BaseModel):
    """One event log."""

    model_config = ConfigDict(frozen=True)

    block_number: int
    block_hash: str
    block_timestamp: int | None
    transaction_hash: str
    transaction_index: int
    log_index: int
    address: str
    topics: tuple[str, ...]
    data: str
    removed: bool = False

    @classmethod
    def from_rpc(cls, item: dict[str, Any]) -> "Log":
        """Builds a log from an ``eth_getLogs`` or ``eth_subscribe`` result item."""
        timestamp = item.get("blockTimestamp")
        # Some official RPCs send "0x0" for every log: zero means the node did not say.
        if timestamp is not None and _int(timestamp) == 0:
            timestamp = None
        return cls(
            block_number=_int(item["blockNumber"]),
            block_hash=item["blockHash"].lower(),
            block_timestamp=_int(timestamp) if timestamp is not None else None,
            transaction_hash=item["transactionHash"].lower(),
            transaction_index=_int(item["transactionIndex"]),
            log_index=_int(item["logIndex"]),
            address=item["address"].lower(),
            topics=tuple(topic.lower() for topic in item["topics"]),
            data=item["data"].lower(),
            removed=bool(item.get("removed", False)),
        )

    @property
    def key(self) -> tuple[int, int, str]:
        """Identifies the log on one fork: position plus block hash."""
        return (self.block_number, self.log_index, self.block_hash)

    @property
    def topic0(self) -> str:
        """Returns the event signature topic, or an empty string for an anonymous log."""
        return self.topics[0] if self.topics else ""


class Header(BaseModel):
    """One block header."""

    model_config = ConfigDict(frozen=True)

    number: int
    hash: str
    parent_hash: str
    timestamp: int
    gas_used: int = 0
    gas_limit: int = 0
    base_fee_per_gas: int | None = None
    miner: str = ""

    @classmethod
    def from_rpc(cls, item: dict[str, Any]) -> "Header":
        """Builds a header from an ``eth_getBlockByNumber`` result or a ``newHeads`` notification."""
        base_fee = item.get("baseFeePerGas")
        return cls(
            number=_int(item["number"]),
            hash=item["hash"].lower(),
            parent_hash=item["parentHash"].lower(),
            timestamp=_int(item["timestamp"]),
            gas_used=_int(item.get("gasUsed", "0x0")),
            gas_limit=_int(item.get("gasLimit", "0x0")),
            base_fee_per_gas=_int(base_fee) if base_fee is not None else None,
            miner=(item.get("miner") or "").lower(),
        )
