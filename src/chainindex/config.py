"""Chain files (what to index and how) and the deployment settings (keys, ClickHouse)."""

import os
import re
from importlib import resources
from pathlib import Path
from typing import Literal

import yaml
from eth_utils import keccak
from pydantic import BaseModel, Field, field_validator, model_validator

from chainindex.abi import STATIC_TYPE, EventSpec, load_abi_events
from chainindex.settings import BaseSettings

NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
ADDRESS_PATTERN = re.compile(r"^0x[0-9a-f]{40}$")
Lane = Literal["stream", "ledger", "backfill", "headers", "reads", "addresses", "transactions"]
LANES: tuple[Lane, ...] = ("stream", "ledger", "backfill")


def normalize_address(value: str) -> str:
    """Lowercases an address and rejects anything that is not 20 bytes of hex."""
    address = value.strip().lower()
    if not ADDRESS_PATTERN.match(address):
        raise ValueError(f"Not an address: {value!r}")
    return address


class AddressSpec(BaseModel):
    """One deployed contract of a kind."""

    address: str
    start_block: int | None = None
    label: str = ""

    @field_validator("address")
    @classmethod
    def _lowercase(cls, value: str) -> str:
        return normalize_address(value)


class AccountSpec(BaseModel):
    """A wallet whose own activity is indexed: the logs that name it, in the kinds that declare ``account_topics``."""

    address: str
    label: str = ""

    @field_validator("address")
    @classmethod
    def _lowercase(cls, value: str) -> str:
        return normalize_address(value)


def account_word(address: str) -> str:
    """Returns an address as the 32-byte topic an indexed address parameter is logged as."""
    return "0x" + "0" * 24 + address[2:]


class CreatesSpec(BaseModel):
    """A factory event announcing a new contract of another kind."""

    event: str
    param: str
    contract: str


class TransactionsSpec(BaseModel):
    """The transactions behind a kind's stored events, fetched by hash and kept raw in ``event_transactions``.

    ``events`` names the events whose transactions are wanted (a pool's Swap: who sent it, through which router, at
    what priority fee). ``start_block`` starts later than the kind's logs when the history before it is not worth the
    requests; unset, the lane starts where the kind's streams start. ``window_blocks`` bounds the transactions held at
    once: a 0.1 s chain packs a Base window's swaps into a fiftieth of its blocks.
    """

    events: list[str]
    start_block: int | None = None
    window_blocks: int = 5_000


class ContractSpec(BaseModel):
    """A contract kind: one ABI (or none), any number of addresses, and how they are found and fetched.

    ``track`` decides which addresses are indexed: ``listed`` the configured and runtime-added ones,
    ``all`` every child its factories announced, each fetched by address from its creation (or by topic, where the
    key allows chain-wide queries), ``matching`` children whose creation event matches ``match``,
    ``created`` every child too, without a registry: every ``created`` kind shares one chain-wide scan of their
    topics that stores each log whoever emitted it, and dbt keeps a log only when its address is a child a
    configured factory announced. Use it for factories with too many children to hold in memory.
    ``any`` keeps every log of the kind's topics from whoever emitted it, in a scan of its own from ``start_block``,
    with no factory: for an event a standard defines and many contracts share, such as ERC-8056's
    ``UIMultiplierUpdated``, whose consumers choose the emitters they trust.
    ``fetch`` decides how: ``addresses`` asks for logs by address in chunks, ``topics`` asks chain-wide by event
    topic and keeps the logs of known addresses, which costs one request however many addresses there are.
    ``events: []`` makes a registry-only kind: its addresses are tracked for reads and none of its logs are fetched.
    ``account_topics`` keeps only the logs that name one of the chain's ``accounts`` at those topic positions, such
    as a token's transfers from (1) or to (2) your wallet: the token is listed, everyone else's transfers are not
    fetched. ``unfiltered_events`` are the kind's events that name no account and are kept whole, such as a token's
    multiplier changes, in a stream of their own per address, from ``unfiltered_start_block`` when the account
    history starts later than theirs.
    ``refetch`` (a number, bumped by hand) fetches an address-fetched kind's history again under fresh cursors, after
    ``events`` gained an event: existing cursors only move forward, and the writer skips the logs it already has.
    ``transactions`` names the events whose transactions are fetched too, by hash, behind the kind's streams: of its
    members' logs, or of every stored log of the events for a ``created`` kind, whose factory check is dbt's.
    A kind without an ``abi`` stores every log its addresses emit, undecoded: an ABI added later decodes all of it.
    """

    name: str
    abi: str | None = None
    events: list[str] | None = None
    start_block: int | None = None
    addresses: list[AddressSpec] = []
    track: Literal["listed", "all", "matching", "created", "any"] = "listed"
    match: dict[str, list[str]] = {}
    # A creation parameter that must be an indexed address of another kind, e.g. a gauge's pool.
    match_contract: dict[str, str] = {}
    fetch: Literal["addresses", "topics"] = "addresses"
    creates: CreatesSpec | None = None
    account_topics: list[int] = []
    unfiltered_events: list[str] = []
    unfiltered_start_block: int | None = None
    refetch: int = 0
    transactions: TransactionsSpec | None = None

    @field_validator("name")
    @classmethod
    def _plain_name(cls, value: str) -> str:
        if not NAME_PATTERN.match(value):
            raise ValueError(f"Contract names are lowercase snake_case: {value!r}")
        return value

    @field_validator("match")
    @classmethod
    def _lowercase_match_values(cls, value: dict[str, list[str]]) -> dict[str, list[str]]:
        return {param: [item.lower() for item in accepted] for param, accepted in value.items()}

    @model_validator(mode="after")
    def _consistent(self) -> "ContractSpec":
        if self.track in ("created", "any") and self.fetch != "topics":
            raise ValueError(f"{self.name}: track: {self.track} needs fetch: topics, since its addresses are unbounded")
        if self.track == "any" and (self.start_block is None or self.addresses):
            raise ValueError(f"{self.name}: track: any needs a start_block and lists no addresses")
        if self.unfiltered_events and not self.account_topics:
            raise ValueError(f"{self.name}: unfiltered_events are the exceptions of a kind with account_topics")
        if any(name not in (self.events or []) for name in self.unfiltered_events):
            raise ValueError(f"{self.name}: unfiltered_events must be among its events")
        if self.track == "matching" and not (self.match or self.match_contract):
            raise ValueError(f"{self.name}: track: matching needs a match or match_contract rule")
        for entry in self.addresses:
            if entry.start_block is None and self.start_block is None:
                raise ValueError(f"{self.name}: {entry.address} has no start_block and the contract has no default")
        if self.account_topics and (self.track != "listed" or self.fetch != "addresses"):
            raise ValueError(f"{self.name}: account_topics needs a listed kind fetched by address")
        if any(position not in (1, 2, 3) for position in self.account_topics):
            raise ValueError(f"{self.name}: account_topics are topic positions 1, 2 or 3, not {self.account_topics}")
        if self.abi is None and self.events:
            raise ValueError(f"{self.name}: events name ABI events, but the kind has no abi")
        if self.fetch == "topics" and self.events != [] and not self.topics():
            raise ValueError(f"{self.name}: fetch: topics needs topics to ask for, from an abi with events")
        if self.transactions is not None:
            if self.track == "any":
                raise ValueError(f"{self.name}: transactions need a registry or factories of the kind's addresses, which track: any has neither")
            indexed = {event.name for event in self.event_specs()}
            unknown = [name for name in self.transactions.events if name not in indexed]
            if not self.transactions.events or unknown:
                raise ValueError(f"{self.name}: transactions.events must be among the kind's indexed events, not {unknown}")
        return self

    def transaction_topics(self) -> list[str]:
        """Returns the topic0 of every event whose transactions are fetched."""
        wanted = set(self.transactions.events) if self.transactions is not None else set()
        return [event.topic0 for event in self.event_specs() if event.name in wanted]

    def topic_filters(self, accounts: list[str]) -> list[list[list[str] | None]] | None:
        """Returns the positional ``eth_getLogs`` topic filters that keep logs naming an account, or None.

        One filter per position, since a filter can only offer alternatives within a position: the logs of the kind's
        events with an account at topic 1, then those with one at topic 2.
        """
        if not self.account_topics:
            return None
        unfiltered = set(self.unfiltered_topics())
        topic0 = [topic for topic in self.topics() if topic not in unfiltered] if self.events is not None else None
        words = [account_word(address) for address in sorted(accounts)]
        return [[topic0, *([None] * (position - 1)), words] for position in sorted(self.account_topics)]

    def event_specs(self) -> list[EventSpec]:
        """Returns the ABI events this contract kind indexes, none without an ABI."""
        if self.abi is None:
            return []
        events = load_abi_events(self.abi)
        if self.events is None:
            return events
        by_name = {event.name: event for event in events}
        unknown = [name for name in self.events if name not in by_name]
        if unknown:
            raise ValueError(f"{self.name}: {self.abi} has no events {unknown}")
        return [by_name[name] for name in self.events]

    def topics(self) -> list[str]:
        """Returns the topic0 of every indexed event."""
        return [event.topic0 for event in self.event_specs()]

    def unfiltered_topics(self) -> list[str]:
        """Returns the topic0 of the events kept whole, without the account filter."""
        return [event.topic0 for event in self.event_specs() if event.name in self.unfiltered_events]


SIGNATURE_PATTERN = re.compile(r"^\s*(\w+)\(([^)]*)\)\s+returns\s+\((.*)\)\s*$")


class FunctionSpec(BaseModel):
    """A view function written as ``name(argument types) returns (type name, ...)``."""

    name: str
    arguments: list[str]
    outputs: list[tuple[str, str]]

    @classmethod
    def parse(cls, signature: str) -> "FunctionSpec":
        """Reads a signature such as ``slot0() returns (uint160 sqrt_price_x96, int24 tick)``."""
        match = SIGNATURE_PATTERN.match(signature)
        if match is None:
            raise ValueError(f"Not a function signature with returns: {signature!r}")
        name, arguments, outputs = match.groups()
        parsed_outputs = []
        for output in outputs.split(","):
            parts = output.split()
            if len(parts) != 2:
                raise ValueError(f"{name}: name every output as `type name`, not {output.strip()!r}")
            abi_type, column = parts
            if not STATIC_TYPE.match(abi_type):
                raise ValueError(f"{name}: output {column} is {abi_type}; reads decode one-word types only")
            parsed_outputs.append((abi_type, column))
        return cls(name=name, arguments=[item.strip() for item in arguments.split(",") if item.strip()], outputs=parsed_outputs)

    @property
    def selector(self) -> str:
        """Returns the 4-byte selector as ``0x`` hex."""
        return "0x" + keccak(text=f"{self.name}({','.join(self.arguments)})").hex()[:8]


class TickMapSpec(BaseModel):
    """Initialized ticks within ``spacings`` tick spacings of a concentrated-liquidity pool's price."""

    spacings: int = 10
    ticks: str = "ticks(int24) returns (uint128 liquidity_gross, int128 liquidity_net, int128 staked_liquidity_net)"
    price: str = "slot0() returns (uint160 sqrt_price_x96, int24 tick)"
    spacing: str = "tickSpacing() returns (int24 tick_spacing)"


class ReadSpec(BaseModel):
    """State read every ``every_blocks`` confirmed blocks from every address of a contract kind, in one multicall.

    ``calls`` are view functions without arguments. ``tick_map`` reads the tick map around each pool's price instead.
    After downtime, blocks older than ``max_catch_up_blocks`` are skipped, since state history is costly to read.
    """

    name: str
    contract: str
    every_blocks: int = 1
    calls: list[str] = []
    tick_map: TickMapSpec | None = None
    max_catch_up_blocks: int = 1000

    @field_validator("name")
    @classmethod
    def _plain_name(cls, value: str) -> str:
        if not NAME_PATTERN.match(value):
            raise ValueError(f"Read names are lowercase snake_case: {value!r}")
        return value

    @model_validator(mode="after")
    def _one_kind(self) -> "ReadSpec":
        if bool(self.calls) == (self.tick_map is not None):
            raise ValueError(f"Read {self.name}: give either calls or a tick_map")
        for function in self.functions():
            if function.arguments and self.tick_map is None:
                raise ValueError(f"Read {self.name}: {function.name} takes arguments; reads call views without any")
        return self

    def functions(self) -> list[FunctionSpec]:
        """Returns the calls as parsed signatures."""
        return [FunctionSpec.parse(call) for call in self.calls]


class LaneSpec(BaseModel):
    """The request budget of one RPC lane, and optionally its own endpoint and address cap."""

    requests_per_second: float
    max_concurrency: int = 4
    # Another HTTP endpoint for this lane, such as a chain's official RPC that takes many addresses per eth_getLogs.
    http: str | None = None
    addresses_per_request: int | None = None


class RpcSpec(BaseModel):
    """Endpoint templates (``{key}`` is replaced by an API key, or dropped without one) and per-lane budgets."""

    http: str
    # None where the provider pushes no heads: the live seal then polls HTTP for the head.
    ws: str | None = None
    # PublicNode answers 9 addresses per eth_getLogs and refuses 10 or more with HTTP 403.
    addresses_per_request: int = 9
    ws_addresses_per_subscription: int = 9
    max_blocks_per_request: int = 2000
    stream: LaneSpec = LaneSpec(requests_per_second=8, max_concurrency=4)
    ledger: LaneSpec = LaneSpec(requests_per_second=4, max_concurrency=2)
    backfill: LaneSpec = LaneSpec(requests_per_second=6, max_concurrency=4)
    # Where block headers come from when logs arrive without a block time, if not the lane that fetched the logs:
    # another provider can answer far bigger header batches than a node that throttles each batch item.
    headers: LaneSpec | None = None
    headers_per_request: int = 50
    # Where state reads go, if not the ledger lane: an archive endpoint can answer a read however far behind it is.
    reads: LaneSpec | None = None
    # Where address streams' history comes from, if not the backfill lane. Topic scans stay on the backfill lane,
    # since an endpoint that serves address filters may refuse chain-wide topic queries.
    addresses: LaneSpec | None = None
    # The budget of the lane that fetches the transactions behind stored events, if not the backfill lane's: its own
    # pacing, so a long backfill of swappers never slows the ledger. On the default endpoint it shares the throttle.
    transactions: LaneSpec | None = None

    def lane(self, lane: Lane) -> LaneSpec:
        """Returns one lane's budget by name."""
        budgets = {
            "stream": self.stream, "ledger": self.ledger, "backfill": self.backfill, "headers": self.headers, "reads": self.reads,
            "addresses": self.addresses, "transactions": self.transactions,
        }
        budget = budgets[lane]
        if budget is None:
            raise ValueError(f"The chain file sets no {lane} lane")
        return budget

    def lane_http(self, lane: Lane) -> str:
        """Returns the HTTP endpoint template a lane uses."""
        return self.lane(lane).http or self.http

    def lane_addresses_per_request(self, lane: Lane) -> int:
        """Returns how many addresses a lane puts in one eth_getLogs."""
        return self.lane(lane).addresses_per_request or self.addresses_per_request


class LedgerSpec(BaseModel):
    """How the ClickHouse ledger trails the tip."""

    confirmations: int
    poll_seconds: float = 10.0
    tip_window_blocks: int = 2000
    backfill_window_blocks: int = 10000
    # The backfill window shrinks until a window holds about this many logs, so a dense range never floods memory.
    backfill_window_logs: int = 25000
    # Backfill workers, each on different streams: a second one lets a dense address stream and a topic scan, which
    # may use different endpoints, advance at the same time.
    backfill_workers: int = 1
    # Consecutive windows of one topic scan fetched at once by different workers. The cursor only joins them gaplessly.
    scan_windows_in_flight: int = 1


class LiveSpec(BaseModel):
    """The in-memory stream for bots."""

    enabled: bool = True
    # False runs the head sockets and the block recorder alone: every block's arrival time, no log stream for bots.
    logs: bool = True
    port: int = 8547
    ring_seconds: float = 600.0
    seal_seconds: float = 2.0
    subscriber_queue: int = 20000
    websockets: int = 2
    # Memory caps on top of ring_seconds: the ring evicts its oldest blocks past this many logs, and unsealed tip
    # logs (which pile up only while sealing is failing) are forgotten oldest first.
    ring_max_logs: int = 200000
    pending_max_logs: int = 50000


class AuditSpec(BaseModel):
    """How often stored ranges are re-fetched and compared."""

    every_seconds: float = 120.0
    recent_blocks: int = 20000
    window_blocks: int = 500
    historic_share: float = 0.25


class VerifySpec(BaseModel):
    """How every stored block's hash is checked against the canonical chain, in order, once it is deep enough.

    The auditor samples windows; the verifier walks every block that holds a stored row exactly once, so a fork that
    slipped past the confirmations is found however fast the chain is. A block is checked once it is ``depth_seconds``
    deep, past the chain's own reorg horizon, and history below the first check is walked backwards when the tip is done.
    """

    enabled: bool = True
    depth_seconds: float = 900.0
    window_blocks: int = 20000
    every_seconds: float = 30.0
    history: bool = True
    # A pause between history windows, so walking a whole table never starves the database's other readers.
    history_pause_seconds: float = 5.0


class ChainConfig(BaseModel):
    """Everything one chain's service indexes."""

    name: str
    chain_id: int
    block_seconds: float
    database: str = ""
    rpc: RpcSpec
    ledger: LedgerSpec
    live: LiveSpec = LiveSpec()
    audit: AuditSpec = AuditSpec()
    verify: VerifySpec = VerifySpec()
    contracts: list[ContractSpec] = Field(default_factory=list)
    reads: list[ReadSpec] = Field(default_factory=list)
    accounts: list[AccountSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def _consistent(self) -> "ChainConfig":
        if not NAME_PATTERN.match(self.name):
            raise ValueError(f"Chain names are lowercase snake_case: {self.name!r}")
        if not self.database:
            self.database = f"chain_{self.name}"
        names = [contract.name for contract in self.contracts]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"Contract names appear twice: {duplicates}")
        owners: dict[str, str] = {}
        for contract in self.contracts:
            for entry in contract.addresses:
                if entry.address in owners:
                    raise ValueError(f"{entry.address} is listed under {owners[entry.address]} and {contract.name}")
                owners[entry.address] = contract.name
        for contract in self.contracts:
            contract.event_specs()
            if contract.creates is not None:
                self._check_creates(contract, contract.creates)
        for contract in self.contracts:
            if contract.track not in ("listed", "any") and not self.factories_of(contract.name):
                raise ValueError(f"{contract.name}: track: {contract.track} needs a factory that creates it")
            for kind in contract.match_contract.values():
                self._require(kind, f"{contract.name}.match_contract")
        if any(contract.account_topics for contract in self.contracts) and not self.accounts:
            raise ValueError("A kind keeps logs by account_topics, but the chain file lists no accounts")
        read_names = [read.name for read in self.reads]
        if len(set(read_names)) != len(read_names):
            raise ValueError(f"Read names appear twice: {read_names}")
        for read in self.reads:
            self._require(read.contract, f"read {read.name}")
        return self

    def _require(self, name: str, used_by: str) -> None:
        """Refuses a reference to a contract kind the chain file does not define."""
        if name not in {contract.name for contract in self.contracts}:
            raise ValueError(f"{used_by} names {name!r}, but there is no contract named {name!r} on {self.name}")

    def _check_creates(self, factory: ContractSpec, creates: CreatesSpec) -> None:
        self._require(creates.contract, f"{factory.name}.creates")
        child = self.contract(creates.contract)
        events = {event.name: event for event in factory.event_specs()}
        if creates.event not in events:
            raise ValueError(f"{factory.name}: creates.event {creates.event} is not one of its indexed events")
        params = {item.name: item.abi_type for item in events[creates.event].inputs}
        if params.get(creates.param) != "address":
            raise ValueError(f"{factory.name}: {creates.event}.{creates.param} is not an address parameter")
        for param in [*child.match, *child.match_contract]:
            if param not in params:
                raise ValueError(f"{child.name}: match.{param} is not a parameter of {factory.name}.{creates.event}")

    def topic_filters(self, contract: ContractSpec) -> list[list[list[str] | None]] | None:
        """Returns a kind's account filters over this chain's accounts, or None for a kind that keeps every log."""
        return contract.topic_filters([account.address for account in self.accounts])

    def contract(self, name: str) -> ContractSpec:
        """Returns a contract kind by name."""
        for contract in self.contracts:
            if contract.name == name:
                return contract
        raise KeyError(f"No contract named {name!r} on {self.name}")

    def factories_of(self, name: str) -> list[ContractSpec]:
        """Returns the contract kinds whose creation events announce ``name``."""
        return [
            contract
            for contract in self.contracts
            if contract.creates is not None and contract.creates.contract == name
        ]

    @property
    def decoded_schema(self) -> str:
        """Names the dbt schema that holds this chain's decoded events."""
        return f"events_{self.name}"


def chains_folder() -> Path:
    """Returns the folder chain files are named from: ``CHAININDEX_CHAINS_DIR``, or the examples shipped with the
    package."""
    configured = os.environ.get("CHAININDEX_CHAINS_DIR")
    return Path(configured) if configured else Path(str(resources.files("chainindex") / "chains"))


def packaged_chains() -> list[str]:
    """Lists the chains in the chain folder."""
    return sorted(path.stem for path in chains_folder().glob("*.yaml"))


def load_chain(name_or_path: str) -> ChainConfig:
    """Loads a chain from the chain folder by name, or a chain file by path."""
    path = Path(name_or_path)
    if path.suffix not in (".yaml", ".yml"):
        path = chains_folder() / f"{name_or_path}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"No chain file {path} (chains in {chains_folder()}: {', '.join(packaged_chains())})")
    return ChainConfig.model_validate(yaml.safe_load(path.read_text()))


def endpoint_url(template: str, key: str) -> str:
    """Fills an endpoint template with an API key, or drops the key segment for the keyless endpoint."""
    if key:
        return template.replace("{key}", key)
    return template.replace("/{key}", "").replace("{key}", "")


class Settings(BaseSettings):
    """Deployment settings: which chain, the API key every lane uses, and ClickHouse."""

    # A chain's name in the chain folder (CHAININDEX_CHAINS_DIR, or the shipped examples), or a chain file's path.
    CHAIN: str = "base"
    # Without a key the keyless endpoint is used.
    RPC_API_KEY: str = ""
    REGISTRY_REFRESH_SECONDS: float = 30.0
    # Deployment overrides of the chain file: run the ledger alone, or slow the backfill down while other services
    # share the provider's budget.
    LIVE_ENABLED: bool | None = None
    LIVE_LOGS: bool | None = None
    READS_ENABLED: bool | None = None
    EVENT_TRANSACTIONS_ENABLED: bool | None = None
    BACKFILL_REQUESTS_PER_SECOND: float | None = None

    def chain_config(self) -> ChainConfig:
        """Loads the configured chain with this deployment's overrides applied."""
        config = load_chain(self.CHAIN)
        if self.LIVE_ENABLED is not None:
            config.live.enabled = self.LIVE_ENABLED
        if self.LIVE_LOGS is not None:
            config.live.logs = self.LIVE_LOGS
        if self.READS_ENABLED is False:
            config.reads = []
        if self.EVENT_TRANSACTIONS_ENABLED is False:
            for contract in config.contracts:
                contract.transactions = None
        if self.BACKFILL_REQUESTS_PER_SECOND is not None:
            config.rpc.backfill.requests_per_second = self.BACKFILL_REQUESTS_PER_SECOND
        return config
