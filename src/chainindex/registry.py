"""Which addresses are indexed: the chain file, runtime additions (``contracts`` table) and factory discoveries."""

import hashlib
import json

from pydantic import BaseModel

from chainindex.abi import Decoder, snake_case
from chainindex.config import ChainConfig, ContractSpec
from chainindex.fetch import TopicFilter
from chainindex.logger import get_logger
from chainindex.models import Log
from chainindex.store import ContractRow

logger = get_logger(__name__)


class Stream(BaseModel):
    """A unit of indexing with its own cursor: one address, or one contract kind fetched by topic."""

    key: str
    contract: str
    start_block: int
    # The query: addresses to ask for (None asks chain-wide) and topic0 alternatives (None asks for every event).
    addresses: list[str] | None
    topics: list[str] | None
    # Positional filters that replace ``topics`` for a kind that keeps only the logs naming an account.
    filters: list[TopicFilter] | None = None

    @property
    def by_topic(self) -> bool:
        """Tells whether the stream scans chain-wide by topic and filters to its members."""
        return self.addresses is None


def address_stream_key(address: str) -> str:
    """Names the cursor of one address."""
    return f"address:{address}"


def filtered_stream_key(address: str, filters: list[TopicFilter]) -> str:
    """Names the cursor of one address's account logs after the filters, so a new account starts over."""
    digest = hashlib.sha256(json.dumps(filters, sort_keys=True).encode()).hexdigest()[:12]
    return f"address:{address}:accounts:{digest}"


def unfiltered_stream_key(address: str, topics: list[str]) -> str:
    """Names the cursor of one address's events kept whole, after their topics, so a changed set starts over."""
    digest = hashlib.sha256(",".join(sorted(topics)).encode()).hexdigest()[:12]
    return f"address:{address}:unfiltered:{digest}"


def topic_stream_key(contract: str) -> str:
    """Names the cursor of a contract kind fetched by topic."""
    return f"topics:{contract}"


# The stream every ``track: created`` kind shares. Not a valid kind name, so no kind's members or factories match it.
CREATED = "*created"


def created_stream_key(topics: list[str]) -> str:
    """Names the shared scan's cursor after its topics, so a changed topic set scans from the start again."""
    digest = hashlib.sha256(",".join(sorted(topics)).encode()).hexdigest()[:12]
    return f"topics:created:{digest}"


class Registry:
    """Holds every known address and turns them into streams."""

    def __init__(self, config: ChainConfig):
        self.config = config
        self.rows: dict[str, ContractRow] = {}
        # Children the live seal found at the tip, known in memory until the ledger records them.
        self.unrecorded: set[str] = set()
        self.version = 0
        self.decoder = Decoder({contract.name: contract.event_specs() for contract in config.contracts})

    def config_rows(self) -> list[ContractRow]:
        """Returns a registry row for every address in the chain file."""
        rows = []
        for contract in self.config.contracts:
            for entry in contract.addresses:
                start_block = entry.start_block if entry.start_block is not None else contract.start_block
                rows.append(
                    ContractRow(
                        address=entry.address, contract=contract.name, start_block=start_block or 0,
                        source="config", label=entry.label,
                    )
                )
        return rows

    def reconcile(self, stored: list[ContractRow]) -> list[ContractRow]:
        """Adopts the stored rows and returns the changes the chain file implies, for the caller to record.

        A chain-file address that is new or changed gets a fresh row, also over a retired row or a factory's child of
        a created kind (such as a gauge the file lists under another kind). A stored config row the file no longer
        lists is deactivated, and so is a factory's child of a created kind, which keeps no children. Other rows
        added by the CLI or a factory are left alone.
        """
        self.load(stored)
        wanted = {row.address: row for row in self.config_rows()}
        created = {contract.name for contract in self.config.contracts if contract.track == "created"}
        changes = []
        for address, row in wanted.items():
            current = self.rows.get(address)
            same = (
                current is not None and current.active and current.contract == row.contract
                and current.start_block == row.start_block and current.label == row.label
            )
            replaceable = current is None or current.source == "config" or not current.active or current.contract in created
            if not same and replaceable:
                changes.append(row)
        replaced = {row.address for row in changes}
        for address, current in self.rows.items():
            retired = current.source == "config" and address not in wanted
            childless = current.source == "factory" and current.contract in created and address not in replaced
            if current.active and (retired or childless):
                changes.append(current.model_copy(update={"active": False}))
        self.apply(changes)
        return changes

    def load(self, rows: list[ContractRow]) -> None:
        """Replaces the known rows with stored ``rows``, keeping tip discoveries not yet recorded."""
        known = {contract.name for contract in self.config.contracts}
        fresh = {row.address: row for row in rows if row.contract in known}
        for address in sorted(self.unrecorded):
            if address in fresh:
                self.unrecorded.discard(address)
            else:
                fresh[address] = self.rows[address]
        if fresh != self.rows:
            self.rows = fresh
            self.version += 1

    def apply(self, rows: list[ContractRow], *, recorded: bool = True) -> None:
        """Adds or replaces rows; ``recorded=False`` marks them as not yet in ClickHouse."""
        changed = False
        for row in rows:
            if recorded:
                self.unrecorded.discard(row.address)
            else:
                self.unrecorded.add(row.address)
            if self.rows.get(row.address) != row:
                self.rows[row.address] = row
                changed = True
        if changed:
            self.version += 1

    def active(self, contract: str | None = None) -> list[ContractRow]:
        """Returns the indexed rows, optionally of one kind."""
        return [
            row for row in self.rows.values()
            if row.active and (contract is None or row.contract == contract)
        ]

    def contract_of(self, address: str) -> str | None:
        """Returns the kind of an indexed address."""
        row = self.rows.get(address)
        return row.contract if row is not None and row.active else None

    def members(self, contract: str) -> dict[str, int]:
        """Returns each indexed address of a kind with the block its logs count from."""
        return {
            row.address: max(row.start_block, row.created_block or 0)
            for row in self.active(contract)
        }

    def addresses_of(self, streams: list[Stream]) -> set[str]:
        """Returns the addresses whose logs some streams hold: their own, or every member of a topic-fetched kind."""
        addresses: set[str] = set()
        for stream in streams:
            addresses.update(stream.addresses if stream.addresses is not None else self.members(stream.contract))
        return addresses

    def _query_topics(self, contract: ContractSpec) -> list[str] | None:
        # Without an events list every log of the address is kept, so a later ABI can decode it.
        return contract.topics() if contract.events is not None else None

    def created_kinds(self) -> list[ContractSpec]:
        """Returns the kinds the shared scan fetches."""
        return [contract for contract in self.config.contracts if contract.track == "created" and contract.events != []]

    def created_stream(self) -> Stream | None:
        """Returns the one chain-wide scan of every created kind's topics, from its earliest factory's start."""
        kinds = self.created_kinds()
        if not kinds:
            return None
        topics = sorted({topic for contract in kinds for topic in contract.topics()})
        starts = [
            start for contract in kinds for factory in self.config.factories_of(contract.name)
            for start in self.members(factory.name).values()
        ]
        if not starts:
            return None
        return Stream(key=created_stream_key(topics), contract=CREATED, start_block=min(starts), addresses=None, topics=topics)

    def streams(self) -> list[Stream]:
        """Returns every stream: one per address of address-fetched kinds, one per topic-fetched kind, one shared
        by the created kinds."""
        streams = []
        created = self.created_stream()
        if created is not None:
            streams.append(created)
        for contract in self.config.contracts:
            if contract.events == [] or contract.track == "created":
                # A registry-only kind: its addresses are known for reads, its logs are not fetched. A created kind
                # is in the shared scan.
                continue
            if contract.fetch == "topics":
                members = self.members(contract.name) if contract.track != "any" else {}
                starts = list(members.values())
                if contract.start_block is not None:
                    starts.append(contract.start_block)
                if not starts:
                    continue
                streams.append(
                    Stream(
                        key=topic_stream_key(contract.name), contract=contract.name, start_block=min(starts),
                        addresses=None, topics=contract.topics(),
                    )
                )
                continue
            filters = self.config.topic_filters(contract)
            for address, start_block in sorted(self.members(contract.name).items()):
                streams.append(
                    Stream(
                        key=self.address_key(contract, address), contract=contract.name, start_block=start_block,
                        addresses=[address], topics=self._query_topics(contract), filters=filters,
                    )
                )
                if contract.unfiltered_events:
                    topics = contract.unfiltered_topics()
                    streams.append(
                        Stream(
                            key=unfiltered_stream_key(address, topics), contract=contract.name,
                            start_block=contract.unfiltered_start_block or start_block,
                            addresses=[address], topics=topics,
                        )
                    )
        return streams

    def address_key(self, contract: ContractSpec, address: str) -> str:
        """Returns the stream key of one address of an address-fetched kind, as its cursor is stored."""
        filters = self.config.topic_filters(contract)
        key = address_stream_key(address) if filters is None else filtered_stream_key(address, filters)
        return f"{key}:refetch{contract.refetch}" if contract.refetch else key

    def factory_streams(self, contract: str) -> list[str]:
        """Returns the stream keys of the factories that create a kind, which its topic stream may not pass.

        The shared scan waits for every created kind's factories, so dbt has a child's creation before its logs.
        """
        kinds = [kind.name for kind in self.created_kinds()] if contract == CREATED else [contract]
        factories = {factory.name: factory for kind in kinds for factory in self.config.factories_of(kind)}
        keys = []
        for factory in factories.values():
            if factory.fetch == "topics":
                keys.append(topic_stream_key(factory.name))
            else:
                keys.extend(self.address_key(factory, row.address) for row in self.active(factory.name))
        return keys

    def keep(self, stream: Stream, logs: list[Log]) -> list[Log]:
        """Filters a topic stream's chain-wide logs to its members, from each member's first block."""
        if not stream.by_topic or stream.contract == CREATED:
            # dbt checks a created kind's logs against its factories' creation events.
            return logs
        if self.config.contract(stream.contract).track == "any":
            # Every emitter's: the kind's consumers choose the ones they trust.
            return logs
        members = self.members(stream.contract)
        return [log for log in logs if log.block_number >= members.get(log.address, 1 << 62)]

    def discover(self, logs: list[Log]) -> list[ContractRow]:
        """Returns rows for children the factories' logs announce, the config wants, and ClickHouse lacks."""
        discovered = []
        for log in logs:
            factory = self.contract_of(log.address)
            if factory is None:
                continue
            spec = self.config.contract(factory)
            if spec.creates is None:
                continue
            decoded = self.decoder.decode(factory, log)
            if decoded is None or decoded.event != spec.creates.event:
                continue
            child = self.config.contract(spec.creates.contract)
            # Decoded arguments are keyed by column, so a camelCase parameter (votingRewardsManager) is found too.
            address = str(decoded.args.get(snake_case(spec.creates.param), "")).lower()
            already_recorded = address in self.rows and address not in self.unrecorded
            if not address or already_recorded or not self._wanted(child, decoded.args):
                continue
            row = ContractRow(
                address=address, contract=child.name, start_block=log.block_number, source="factory",
                created_block=log.block_number, creation_args=decoded.args,
                label=f"{factory} {log.address[:10]}",
            )
            discovered.append(row)
        if discovered:
            logger.info(f"Factories announced {len(discovered)} new contract(s) to index")
        return discovered

    def _wanted(self, child: ContractSpec, args: dict[str, object]) -> bool:
        if child.track == "all":
            return True
        if child.track == "created":
            # The scan needs no addresses, and a factory's children can number thousands,
            # too many to read. Reads use the kind's listed addresses.
            return False
        if child.track == "listed":
            return False
        by_value = any(str(args.get(snake_case(param), "")).lower() in accepted for param, accepted in child.match.items())
        by_kind = any(
            self.contract_of(str(args.get(snake_case(param), "")).lower()) == kind for param, kind in child.match_contract.items()
        )
        return by_value or by_kind
