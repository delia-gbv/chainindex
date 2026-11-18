"""What the live stream sends to bots, the same in-process and over the websocket."""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter, field_validator


class HeadEvent(BaseModel):
    """A new canonical head."""

    type: Literal["head"] = "head"
    block_number: int
    block_hash: str
    parent_hash: str
    timestamp: int


class LogEvent(BaseModel):
    """A decoded log. ``sealed`` is False when it came straight off a websocket, before its block was sealed."""

    type: Literal["log"] = "log"
    block_number: int
    block_hash: str
    block_timestamp: int | None
    transaction_hash: str
    transaction_index: int
    log_index: int
    address: str
    contract: str
    event: str
    args: dict[str, Any]
    topics: list[str]
    data: str
    sealed: bool

    @property
    def key(self) -> tuple[int, int, str]:
        """Identifies the log on one fork."""
        return (self.block_number, self.log_index, self.block_hash)


class SealedEvent(BaseModel):
    """Every log at or below ``block_number`` has been delivered."""

    type: Literal["sealed"] = "sealed"
    block_number: int
    block_hash: str


class ReorgEvent(BaseModel):
    """The tip forked: everything delivered above ``block_number`` is void."""

    type: Literal["reorg"] = "reorg"
    block_number: int


class ErrorEvent(BaseModel):
    """The stream cannot serve a subscription as asked, for example a resume point older than the ring."""

    type: Literal["error"] = "error"
    reason: str
    oldest_block: int | None = None


Event = Annotated[HeadEvent | LogEvent | SealedEvent | ReorgEvent | ErrorEvent, Field(discriminator="type")]
EVENT_ADAPTER: TypeAdapter[Event] = TypeAdapter(Event)


class Subscription(BaseModel):
    """What a subscriber wants. Empty lists mean everything; ``from_block`` resumes from the ring."""

    contracts: list[str] = []
    addresses: list[str] = []
    events: list[str] = []
    heads: bool = False
    from_block: int | None = None

    @field_validator("addresses")
    @classmethod
    def _lowercase(cls, value: list[str]) -> list[str]:
        return [address.lower() for address in value]

    def wants(self, event: LogEvent) -> bool:
        """Tells whether a log matches the subscription."""
        return (
            (not self.contracts or event.contract in self.contracts)
            and (not self.addresses or event.address in self.addresses)
            and (not self.events or event.event in self.events)
        )
