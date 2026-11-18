"""``chainindex``: run a chain's service, manage its contracts, rebuild its tables, generate dbt models, probe RPC keys
and tail the stream."""

import argparse
import asyncio
import json
import signal
import sys
from pathlib import Path

from chainindex import api, dbtgen
from chainindex.clickhouse import get_client
from chainindex.client import StreamClient
from chainindex.config import (
    Settings,
    endpoint_url,
    load_chain,
    normalize_address,
    packaged_chains,
)
from chainindex.events import LogEvent, SealedEvent
from chainindex.logger import configure_logging, get_logger
from chainindex.metrics import Metrics
from chainindex.probe import probe
from chainindex.rebuild import Rebuilder, StillWriting
from chainindex.registry import Registry
from chainindex.service import ChainService
from chainindex.store import ChainStore, ContractRow

logger = get_logger("chainindex")

# A dbt project's models folder. Generated views go under its chains/<chain>/.
DEFAULT_MODELS_DIR = Path("models")
# One partition copy of a large table runs for many minutes in one HTTP request.
REBUILD_TIMEOUT_SECONDS = 6 * 3600


def _store(settings: Settings) -> ChainStore:
    config = settings.chain_config()
    return ChainStore(get_client(settings, database="default", autogenerate_session_id=False), config.database)


async def _run(settings: Settings) -> None:
    service = ChainService(settings)
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    if task is not None:
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
    await service.run()


async def _init(settings: Settings) -> None:
    store = _store(settings)
    await asyncio.to_thread(store.create)
    registry = Registry(settings.chain_config())
    changes = registry.reconcile(await store.contracts())
    await store.record_contracts(changes)
    print(f"{store.database}: tables ready, {len(changes)} registry change(s) recorded")


async def _contracts(settings: Settings, args: argparse.Namespace) -> None:
    config = settings.chain_config()
    store = _store(settings)
    if args.action == "list":
        rows = sorted(await store.contracts(), key=lambda row: (row.contract, row.address))
        for row in rows:
            if row.active or args.all:
                state = "" if row.active else "  (removed)"
                print(f"{row.contract:28} {row.address}  from {row.start_block:>10}  {row.source:8} {row.label}{state}")
        return
    address = normalize_address(args.address)
    if args.action == "add":
        contract = config.contract(args.contract)
        start_block = args.from_block if args.from_block is not None else contract.start_block
        if start_block is None:
            raise SystemExit(f"{contract.name} has no default start block: pass --from-block")
        row = ContractRow(address=address, contract=contract.name, start_block=start_block, source="cli", label=args.label)
    else:
        current = {row.address: row for row in await store.contracts()}.get(address)
        if current is None:
            raise SystemExit(f"{address} is not in the registry")
        row = current.model_copy(update={"active": False, "source": "cli"})
    await store.record_contracts([row])
    verb = "added" if row.active else "removed"
    print(f"{verb} {row.contract} {row.address}; the running service picks it up within {settings.REGISTRY_REFRESH_SECONDS:.0f} s")


async def _status(settings: Settings) -> None:
    store = _store(settings)
    cursors = await store.cursors()
    for key, cursor in sorted(cursors.items(), key=lambda item: item[1].to_block):
        if cursor.until_block is not None and cursor.done:
            continue
        span = "nothing yet" if cursor.to_block < cursor.from_block else f"{cursor.from_block}-{cursor.to_block}"
        until = f" (catch-up to {cursor.until_block})" if cursor.until_block is not None else ""
        print(f"{key:52} {span}{until}")


def _rebuild(settings: Settings) -> None:
    config = settings.chain_config()
    client = get_client(
        settings, database="default", autogenerate_session_id=False, send_receive_timeout=REBUILD_TIMEOUT_SECONDS
    )
    try:
        rebuilt = Rebuilder(ChainStore(client, config.database)).run()
    except StillWriting as exc:
        raise SystemExit(str(exc)) from exc
    print(f"{config.database}: {', '.join(rebuilt) or 'no table'} rebuilt; every table has the current schema")


def _dbt(args: argparse.Namespace) -> None:
    chains = [load_chain(name) for name in packaged_chains()]
    if args.check:
        problems = dbtgen.stale(chains, args.models_dir)
        if problems:
            print("Generated dbt models are stale; run `chainindex dbt`:")
            print("\n".join(f"  {problem}" for problem in problems))
            raise SystemExit(1)
        print("Generated dbt models are up to date")
        return
    changed = dbtgen.write(chains, args.models_dir)
    print(f"{len(changed)} file(s) changed under {args.models_dir / 'chains'}")


async def _probe(settings: Settings) -> None:
    config = settings.chain_config()
    http_url = endpoint_url(config.rpc.http, settings.RPC_API_KEY)
    ws_url = endpoint_url(config.rpc.ws, settings.RPC_API_KEY) if config.rpc.ws else None
    print(json.dumps(await probe(config, http_url, ws_url, metrics=Metrics("chainindex_probe", isolated=True))))


async def _tail(args: argparse.Namespace) -> None:
    async with StreamClient(args.url) as client:
        async for event in client.subscribe(contracts=args.contract or [], events=args.event or []):
            if isinstance(event, LogEvent):
                state = "sealed" if event.sealed else "fresh"
                print(f"{event.block_number} {event.contract}.{event.event} {event.address} {state} {json.dumps(event.args)}")
            elif isinstance(event, SealedEvent):
                if args.sealed:
                    print(f"{event.block_number} sealed")
            else:
                print(event.model_dump_json())


def _parse(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="chainindex", description=__doc__)
    parser.add_argument("--chain", help="A chain name from the chain folder, or a chain file (default: CHAIN)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="Run the chain's service")
    commands.add_parser("init", help="Create the chain database and record the chain file's contracts")
    contracts = commands.add_parser("contracts", help="List, add or remove indexed contracts at runtime")
    contracts_actions = contracts.add_subparsers(dest="action", required=True)
    listing = contracts_actions.add_parser("list")
    listing.add_argument("--all", action="store_true", help="Include removed addresses")
    add = contracts_actions.add_parser("add")
    add.add_argument("contract", help="Contract kind from the chain file, such as aerodrome_cl_pool")
    add.add_argument("address")
    add.add_argument("--from-block", type=int, default=None)
    add.add_argument("--label", default="")
    remove = contracts_actions.add_parser("remove")
    remove.add_argument("address")
    commands.add_parser("status", help="Print every stream's cursor")
    commands.add_parser(
        "rebuild", help="Copy tables whose schema changed (such as a sorting key) into the current one; service stopped"
    )
    dbt = commands.add_parser("dbt", help="Generate the decoding dbt models of every chain in the chain folder")
    dbt.add_argument("--check", action="store_true", help="Fail when the generated files are stale")
    dbt.add_argument("--models-dir", type=Path, default=DEFAULT_MODELS_DIR)
    commands.add_parser("probe", help="Check what the key allows (timestamps, topic-only logs, websocket)")
    commands.add_parser("api", help="Serve the public read-only API over API_CHAINS")
    tail = commands.add_parser("tail", help="Print a running service's stream")
    tail.add_argument("url", help="Such as ws://localhost:8547/stream")
    tail.add_argument("--contract", action="append")
    tail.add_argument("--event", action="append")
    tail.add_argument("--sealed", action="store_true", help="Also print seal marks")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Runs one CLI command."""
    args = _parse(argv)
    settings = Settings()
    if args.chain:
        settings = settings.model_copy(update={"CHAIN": args.chain})
    configure_logging(settings.ENV, level=settings.LOG_LEVEL, redact=settings.redactable())
    try:
        if args.command == "run":
            asyncio.run(_run(settings))
        elif args.command == "init":
            asyncio.run(_init(settings))
        elif args.command == "contracts":
            asyncio.run(_contracts(settings, args))
        elif args.command == "status":
            asyncio.run(_status(settings))
        elif args.command == "rebuild":
            _rebuild(settings)
        elif args.command == "dbt":
            _dbt(args)
        elif args.command == "probe":
            asyncio.run(_probe(settings))
        elif args.command == "api":
            asyncio.run(api.serve(api.ApiSettings()))
        elif args.command == "tail":
            asyncio.run(_tail(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Stopped")
        sys.exit(0)


if __name__ == "__main__":
    main()
