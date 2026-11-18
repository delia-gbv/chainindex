"""ABI events: their topics, column names, and Python decoding of raw logs for the live stream."""

import json
import os
import re
from functools import cache
from importlib import resources
from pathlib import Path
from typing import Any

from eth_abi import decode as abi_decode
from eth_abi.exceptions import DecodingError
from eth_utils import keccak
from pydantic import BaseModel

from chainindex.logger import get_logger
from chainindex.models import Log

logger = get_logger(__name__)

STATIC_TYPE = re.compile(r"^(address|bool|u?int\d*|bytes([1-9]|[12][0-9]|3[0-2]))$")


def topic(signature: str) -> str:
    """Returns an event's topic0."""
    return "0x" + keccak(text=signature).hex()


def snake_case(name: str) -> str:
    """Turns an ABI name (``sqrtPriceX96``, ``PoolCreated``) into a column or table name."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def canonical_type(abi_input: dict[str, Any]) -> str:
    """Spells an ABI input's type the way signatures do, expanding tuples."""
    abi_type = abi_input["type"]
    if not abi_type.startswith("tuple"):
        return abi_type
    members = ",".join(canonical_type(component) for component in abi_input["components"])
    return f"({members}){abi_type[len('tuple'):]}"


class EventInput(BaseModel):
    """One parameter of an event."""

    name: str
    abi_type: str
    indexed: bool

    @property
    def column(self) -> str:
        """Names the decoded column."""
        return snake_case(self.name)

    @property
    def is_static(self) -> bool:
        """Tells whether the value fits one 32-byte word."""
        return bool(STATIC_TYPE.match(self.abi_type))


class EventSpec(BaseModel):
    """One ABI event."""

    name: str
    signature: str
    topic0: str
    inputs: list[EventInput]

    @property
    def table(self) -> str:
        """Names the event's part of a decoded table name."""
        return snake_case(self.name)


@cache
def load_abi(name: str) -> tuple[dict[str, Any], ...]:
    """Reads ``<name>.json`` from ``CHAININDEX_ABIS_DIR`` or else the ABIs shipped with the package, accepting a bare
    list or a Foundry artifact."""
    configured = os.environ.get("CHAININDEX_ABIS_DIR")
    own = Path(configured) / f"{name}.json" if configured else None
    path = own if own is not None and own.exists() else Path(str(resources.files("chainindex") / "abis" / f"{name}.json"))
    content = json.loads(path.read_text())
    if isinstance(content, dict):
        content = content["abi"]
    return tuple(content)


@cache
def load_abi_events(name: str) -> list[EventSpec]:
    """Returns a packaged ABI's events, with unnamed inputs called ``arg<i>``."""
    events = []
    for entry in load_abi(name):
        if entry.get("type") != "event":
            continue
        inputs = [
            EventInput(name=item.get("name") or f"arg{index}", abi_type=canonical_type(item), indexed=item["indexed"])
            for index, item in enumerate(entry["inputs"])
        ]
        signature = f"{entry['name']}({','.join(item.abi_type for item in inputs)})"
        events.append(EventSpec(name=entry["name"], signature=signature, topic0=topic(signature), inputs=inputs))
    names = [event.name for event in events]
    overloaded = sorted({name for name in names if names.count(name) > 1})
    if overloaded:
        raise ValueError(f"{name}.json overloads events {overloaded}; give each a distinct name in the ABI")
    return events


class DecodedLog(BaseModel):
    """A raw log with its contract kind, event name and decoded arguments."""

    log: Log
    contract: str
    event: str
    args: dict[str, Any]


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return "0x" + value.hex()
    if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
        return value.lower()
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def decode_event(event: EventSpec, log: Log) -> dict[str, Any]:
    """Decodes one log's arguments by the event's ABI; indexed dynamic values stay as their topic hash."""
    topics = iter(log.topics[1:])
    data_inputs = [item for item in event.inputs if not item.indexed]
    data_values = abi_decode([item.abi_type for item in data_inputs], bytes.fromhex(log.data[2:]))
    by_data = dict(zip((item.name for item in data_inputs), data_values))
    args: dict[str, Any] = {}
    for item in event.inputs:
        if not item.indexed:
            args[item.column] = _json_value(by_data[item.name])
            continue
        topic = next(topics)
        if item.is_static:
            (value,) = abi_decode([item.abi_type], bytes.fromhex(topic[2:]))
            args[item.column] = _json_value(value)
        else:
            args[item.column] = topic
    return args


class Decoder:
    """Decodes logs of known contract kinds by topic0."""

    def __init__(self, events_by_contract: dict[str, list[EventSpec]]):
        self.events: dict[tuple[str, str], EventSpec] = {
            (contract, event.topic0): event
            for contract, events in events_by_contract.items()
            for event in events
        }

    def decode(self, contract: str, log: Log) -> DecodedLog | None:
        """Returns the decoded log, or None when its topic is not an indexed event of the contract."""
        if not log.topics:
            return None
        event = self.events.get((contract, log.topics[0]))
        if event is None:
            return None
        try:
            args = decode_event(event, log)
        except (DecodingError, StopIteration, ValueError) as exc:
            # A log can share a topic with our event yet not its layout (an indexed-ness mismatch).
            logger.warning(f"Could not decode {contract}.{event.name} at {log.block_number}/{log.log_index}: {exc}")
            return None
        return DecodedLog(log=log, contract=contract, event=event.name, args=args)
