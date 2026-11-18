"""One window of logs for a set of streams, as both the ledger and the live seal need it."""

import asyncio
import json
from collections.abc import Awaitable, Callable

from chainindex.fetch import LogFetcher, LogQuery
from chainindex.models import Log
from chainindex.registry import Registry, Stream


def address_groups(streams: list[Stream]) -> list[tuple[Stream, list[str]]]:
    """Groups the address streams that ask the same question (same topics, same account filters) with their addresses.

    Each group's first stream carries the question, so the group shares requests.
    """
    grouped: dict[str, tuple[Stream, list[str]]] = {}
    for stream in streams:
        if stream.addresses is not None:
            _first, addresses = grouped.setdefault(json.dumps([stream.topics, stream.filters]), (stream, []))
            addresses.extend(stream.addresses)
    return list(grouped.values())


async def collect(
    registry: Registry,
    streams: list[Stream],
    start: int,
    end: int,
    fetcher: LogFetcher,
    *,
    on_children: Callable[[list[Log]], Awaitable[None]],
) -> list[Log]:
    """Fetches address streams, lets ``on_children`` register what factories announced, then scans topic streams,
    factories' scans first, registering what each announced too.

    The order matters: a child created inside the window is a member by the time its own logs are filtered.
    """
    address_parts = await asyncio.gather(
        *(
            fetcher.fetch(LogQuery(addresses=addresses, topics=first.topics, filters=first.filters), start, end)
            for first, addresses in address_groups(streams)
        )
    )
    logs = [log for part in address_parts for log in part]
    await on_children(logs)
    factories = {contract.name for contract in registry.config.contracts if contract.creates is not None}
    scans = [stream for stream in streams if stream.by_topic]
    for stream in sorted(scans, key=lambda stream: stream.contract not in factories):
        scanned = registry.keep(stream, await fetcher.fetch(LogQuery(addresses=None, topics=stream.topics), start, end))
        await on_children(scanned)
        logs.extend(scanned)
    unique = {log.key: log for log in logs}
    return sorted(unique.values(), key=lambda log: (log.block_number, log.log_index))
