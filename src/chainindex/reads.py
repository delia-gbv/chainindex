"""The reads lane: contract state at confirmed blocks, one Multicall3 call per block, stored raw in ``calls``.

A read names a contract kind and view calls (or a tick map). Every ``every_blocks`` confirmed blocks, the reader
calls each active address of the kind in one ``aggregate3`` pinned to that block, batched with the block's header so
every row carries the block's hash and time. A call's row is stored only when its result differs from the last one
stored, so ``calls`` holds each value once, from the block it was first seen. The read's cursor moves every step and
says how far the values are known. dbt decodes the raw return data.
"""

import asyncio
from typing import Any

from eth_abi import decode, encode

from chainindex.config import ChainConfig, FunctionSpec, ReadSpec, TickMapSpec
from chainindex.logger import get_logger
from chainindex.metrics import Metrics
from chainindex.models import Header
from chainindex.registry import Registry
from chainindex.rpc import RpcError, RpcPool, RpcUnavailable
from chainindex.store import CallRow, ChainStore, Cursor

logger = get_logger(__name__)

MULTICALL3 = "0xca11bde05977b3631167028862be2a173976ca11"
AGGREGATE3 = "0x82ad56cb"
CALLS_PER_MULTICALL = 400
BLOCKS_PER_STEP = 30
ERROR_PAUSE_SECONDS = 5.0
# How nodes say they no longer keep a block's state (some keep only 10-15 minutes of it).
STATE_GONE_WORDS = ("historical state", "missing trie node", "state is not available", "state not available")


def is_state_gone(error: RpcError) -> bool:
    """Tells whether a node no longer keeps the state at a block, so reading it again cannot succeed."""
    message = error.message.lower()
    return any(word in message for word in STATE_GONE_WORDS)


def read_cursor_key(read: ReadSpec) -> str:
    """Names a read's cursor."""
    return f"read:{read.name}"


def encode_aggregate3(calls: list[tuple[str, str]]) -> str:
    """Encodes ``aggregate3`` over ``(address, calldata)`` pairs, letting each call fail on its own."""
    triples = [(address, True, bytes.fromhex(data[2:])) for address, data in calls]
    return AGGREGATE3 + encode(["(address,bool,bytes)[]"], [triples]).hex()


def decode_aggregate3(output: str) -> list[tuple[bool, str]]:
    """Decodes ``aggregate3``'s ``(success, returnData)`` results."""
    (results,) = decode(["(bool,bytes)[]"], bytes.fromhex(output[2:]))
    return [(bool(success), "0x" + data.hex()) for success, data in results]


def int_word(value: int) -> str:
    """Encodes an int as one 32-byte argument word, two's complement for negatives."""
    return format(value % (1 << 256), "064x")


def signed_word(output: str, index: int) -> int | None:
    """Returns the ``index``-th word of return data as a signed int, or None when the data is too short."""
    start = 2 + 64 * index
    if len(output) < start + 64:
        return None
    value = int(output[start : start + 64], 16)
    return value - (1 << 256) if value >= 1 << 255 else value


class Reader:
    """Runs every read of the chain file, each with its own cursor."""

    def __init__(self, config: ChainConfig, registry: Registry, store: ChainStore, *, pool: RpcPool, metrics: Metrics):
        self.config = config
        self.registry = registry
        self.store = store
        self.pool = pool
        self.metrics = metrics
        self.cursors: dict[str, Cursor] = {}
        # Tick spacing never changes for a pool, so it is read once.
        self.tick_spacings: dict[str, int] = {}
        # The last stored ``(success, output)`` of every ``(read, address, function, input)``.
        self.latest: dict[tuple[str, str, str, str], tuple[bool, str]] = {}

    async def load(self) -> None:
        """Reads the stored read cursors and the last stored result of every call."""
        stored = await self.store.cursors()
        self.cursors = {read_cursor_key(read): stored[read_cursor_key(read)] for read in self.config.reads if read_cursor_key(read) in stored}
        self.latest = await self.store.latest_calls([read.name for read in self.config.reads])

    def changes(self, rows: list[CallRow]) -> tuple[list[CallRow], dict[tuple[str, str, str, str], tuple[bool, str]]]:
        """Returns the rows, in block order, whose result differs from the call's previous one, and the new results."""
        changed = []
        results: dict[tuple[str, str, str, str], tuple[bool, str]] = {}
        for row in rows:
            call = (row.read, row.address, row.function, row.input)
            result = (row.success, row.output)
            if results.get(call, self.latest.get(call)) != result:
                results[call] = result
                changed.append(row)
        return changed, results

    def due_blocks(self, read: ReadSpec, confirmed: int) -> list[int]:
        """Returns the next blocks this read owes, skipping what is older than its catch-up limit."""
        cursor = self.cursors.get(read_cursor_key(read))
        if cursor is None:
            # A fresh read starts at the newest due block, so it begins on the first step.
            start = confirmed // read.every_blocks * read.every_blocks
        else:
            start = cursor.to_block + 1
            oldest = confirmed - read.max_catch_up_blocks
            if start < oldest:
                self.metrics.reads_skipped_blocks.labels(read=read.name).inc(oldest - start)
                logger.warning(f"Read {read.name} skips blocks {start}-{oldest - 1}: older than its catch-up limit")
                start = oldest
        first = -(-start // read.every_blocks) * read.every_blocks
        return list(range(first, confirmed + 1, read.every_blocks))[:BLOCKS_PER_STEP]

    async def call_at(self, block: int, calls: list[tuple[str, str]]) -> tuple[Header, list[tuple[bool, str]]]:
        """Runs calls at one block through Multicall3 and returns the block's header with the results."""
        parts = [calls[start : start + CALLS_PER_MULTICALL] for start in range(0, len(calls), CALLS_PER_MULTICALL)]
        batch: list[tuple[str, list[Any]]] = [("eth_getBlockByNumber", [hex(block), False])]
        batch += [("eth_call", [{"to": MULTICALL3, "data": encode_aggregate3(part)}, hex(block)]) for part in parts]
        answers = await self.pool.batch(batch)
        for answer in answers:
            if isinstance(answer, RpcError):
                raise answer
        if answers[0] is None:
            raise RpcUnavailable(f"The {self.pool.lane} lane has no header for block {block}")
        results = [result for output in answers[1:] for result in decode_aggregate3(output)]
        return Header.from_rpc(answers[0]), results

    def _addresses(self, read: ReadSpec, block: int) -> list[str]:
        return sorted(address for address, start in self.registry.members(read.contract).items() if start <= block)

    def _rows(
        self, read: ReadSpec, header: Header, calls: list[tuple[str, str, str]], results: list[tuple[bool, str]]
    ) -> list[CallRow]:
        return [
            CallRow(
                read=read.name, block_number=header.number, block_hash=header.hash, block_time=header.timestamp,
                address=address, function=function, input=data, success=success, output=output,
            )
            for (address, function, data), (success, output) in zip(calls, results)
        ]

    async def read_block(self, read: ReadSpec, block: int) -> list[CallRow]:
        """Reads one block for one read and returns its raw rows."""
        if read.tick_map is None:
            return await self.read_together([read], block)
        addresses = self._addresses(read, block)
        if not addresses:
            return []
        return await self._read_tick_map(read, read.tick_map, block, addresses)

    async def read_together(self, reads: list[ReadSpec], block: int) -> list[CallRow]:
        """Reads several call reads at one block in a single request and returns every read's rows."""
        planned: list[tuple[ReadSpec, list[tuple[str, str, str]]]] = []
        for read in reads:
            calls = [
                (address, function.name, function.selector)
                for address in self._addresses(read, block) for function in read.functions()
            ]
            if calls:
                planned.append((read, calls))
        if not planned:
            return []
        header, results = await self.call_at(block, [(address, data) for _read, calls in planned for address, _name, data in calls])
        rows: list[CallRow] = []
        offset = 0
        for read, calls in planned:
            rows.extend(self._rows(read, header, calls, results[offset : offset + len(calls)]))
            offset += len(calls)
        return rows

    async def _read_tick_map(self, read: ReadSpec, tick_map: TickMapSpec, block: int, addresses: list[str]) -> list[CallRow]:
        price = FunctionSpec.parse(tick_map.price)
        spacing = FunctionSpec.parse(tick_map.spacing)
        ticks = FunctionSpec.parse(tick_map.ticks)
        unknown = [address for address in addresses if address not in self.tick_spacings]
        first_calls = [(address, price.selector) for address in addresses] + [(address, spacing.selector) for address in unknown]
        header, results = await self.call_at(block, first_calls)
        for address, (success, output) in zip(unknown, results[len(addresses) :]):
            value = signed_word(output, 0) if success else None
            if value:
                self.tick_spacings[address] = value
        calls = []
        for address, (success, output) in zip(addresses, results[: len(addresses)]):
            # The price call's second output is the current tick.
            current = signed_word(output, 1) if success else None
            tick_spacing = self.tick_spacings.get(address)
            if current is None or tick_spacing is None:
                continue
            base = current // tick_spacing * tick_spacing
            for step in range(-tick_map.spacings, tick_map.spacings + 1):
                calls.append((address, ticks.name, ticks.selector + int_word(base + step * tick_spacing)))
        if not calls:
            return []
        tick_header, tick_results = await self.call_at(block, [(address, data) for address, _name, data in calls])
        return self._rows(read, tick_header, calls, tick_results)

    async def step(self) -> bool:
        """Reads the next due blocks of every read and returns whether any read is still behind."""
        head = await self.pool.block_number()
        confirmed = head - self.config.ledger.confirmations
        behind = False
        due = {read.name: self.due_blocks(read, confirmed) for read in self.config.reads}
        rows_by_read: dict[str, list[CallRow]] = {read.name: [] for read in self.config.reads}
        # Call reads due at the same block share one request: one multicall, one header.
        together: dict[int, list[ReadSpec]] = {}
        # Blocks whose state the node has dropped are skipped, so a lane that fell behind cannot retry them forever.
        gone: set[int] = set()
        for read in self.config.reads:
            for block in due[read.name]:
                if read.tick_map is None:
                    together.setdefault(block, []).append(read)
                    continue
                try:
                    rows_by_read[read.name].extend(await self.read_block(read, block))
                except RpcError as error:
                    if not is_state_gone(error):
                        raise
                    gone.add(block)
        for block, reads in sorted(together.items()):
            try:
                rows = await self.read_together(reads, block)
            except RpcError as error:
                if not is_state_gone(error):
                    raise
                gone.add(block)
                continue
            for row in rows:
                rows_by_read[row.read].append(row)
        if gone:
            logger.warning(f"Reads skip {len(gone)} block(s) from {min(gone)}: the node no longer keeps their state")
        for read in self.config.reads:
            blocks = due[read.name]
            if not blocks:
                continue
            skipped = sum(1 for block in blocks if block in gone)
            if skipped:
                self.metrics.reads_skipped_blocks.labels(read=read.name).inc(skipped * read.every_blocks)
            rows = rows_by_read[read.name]
            changed, results = self.changes(rows)
            inserted = await self.store.insert_calls(changed)
            # Only once stored, so a failed insert is retried with the same changes.
            self.latest.update(results)
            key = read_cursor_key(read)
            cursor = self.cursors.get(key) or Cursor(stream=key, from_block=blocks[0], to_block=blocks[0] - 1)
            cursor.to_block = blocks[-1]
            self.cursors[key] = cursor
            await self.store.save_cursors([cursor])
            failed = sum(1 for row in rows if not row.success)
            self.metrics.reads_block.labels(read=read.name).set(blocks[-1])
            self.metrics.reads_rows.labels(read=read.name).inc(inserted)
            self.metrics.reads_failed_calls.labels(read=read.name).inc(failed)
            behind = behind or blocks[-1] + read.every_blocks <= confirmed
        return behind

    async def run(self) -> None:
        """Reads forever."""
        await self.load()
        while True:
            try:
                behind = await self.step()
            except Exception as exc:
                logger.error(f"Reads step failed, retrying in {ERROR_PAUSE_SECONDS:.0f} s: {exc!r}")
                await asyncio.sleep(ERROR_PAUSE_SECONDS)
                continue
            if not behind:
                await asyncio.sleep(max(self.config.block_seconds, 1.0))
