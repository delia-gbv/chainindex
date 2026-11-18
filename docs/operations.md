# Operations

## Tables

Each chain has its own ClickHouse database, `chain_<chain>`. Every table is append-only except `cursors`.

| Table | What it holds |
|---|---|
| `logs` | Every indexed log, raw. Sorted by `(topic0, address, block_number)`, so one event of one contract reads only its own rows. |
| `contracts` | Which address belongs to which kind. The latest row per address wins. |
| `blocks` | Headers of confirmed blocks, with when each arrived over the websocket. |
| `calls` | Results of `reads`, a row only when a value changed. |
| `transactions` | Your accounts' transactions and receipts. |
| `event_transactions` | Transactions behind indexed events. |
| `orphaned_blocks` | Stored blocks that turned out not to be on the chain. The views skip them. |
| `audits` | Every comparison of stored data with the node. |
| `cursors` | How far each contract, read and check has got. |
| `schema_migrations` | Which schema changes have been applied. |

The decoded views are in `events_<chain>`. Build them with dbt:

```bash
cd dbt && uv run dbt build --profiles-dir profiles --select tag:chain_base
```

They are views, so a contract added later, or an ABI added later, is decoded as soon as its logs are stored.

## Reorgs and verification

A log is written only once its block has `ledger.confirmations` blocks after it, so short reorgs never reach the
database. For the rare deeper one there are three checks:

1. **At write time.** If the live chain knows a different hash for a block, the window is fetched again, up to three
   times. If they still disagree, it is written and sent to the auditor.
2. **The verifier.** Every block that holds a stored row is checked once, after `verify.depth_seconds` (15 minutes):
   its stored hash is compared with the node's header. A block that changed is recorded in `orphaned_blocks`, and the
   block's correct logs are fetched and stored. The verifier follows the chain head, then works backwards through
   history until it reaches the oldest stored log, or a block the node no longer has. Its progress is the cursor
   `verify:blocks`.
3. **The auditor.** Every two minutes it fetches a random 500-block range again and compares it with the database,
   mostly recent ranges and sometimes old ones. It inserts logs the database is missing and explains extra ones.

Nothing is ever deleted: the views leave out rows from orphaned blocks.

## Schema changes

A change to a table that already exists is a migration in `MIGRATIONS` (`src/chainindex/store.py`). Each runs once,
in order, and is recorded in `schema_migrations` with a checksum. If a migration's SQL is edited after it ran, the
service refuses to start; add a new migration instead.

Changing a table's sort order needs a copy. Stop the service, then run `chainindex --chain <chain> rebuild`: it copies
the table into the new schema one partition at a time, checks the row counts, swaps the tables and drops the old one.
It can be stopped and rerun.

## Live stream

The service serves decoded events on `ws://<host>:8547/stream`:

```python
from chainindex.client import StreamClient

async with StreamClient("ws://localhost:8547/stream") as client:
    async for event in client.subscribe(contracts=["aerodrome_cl_pool"], events=["Swap"]):
        ...
```

- A `LogEvent` arrives as soon as a websocket sees it, already decoded.
- A `SealedEvent(n)` means every log up to block `n` has been sent. chainindex checks each block over HTTP and sends
  anything the websockets missed before sealing it.
- A `ReorgEvent(n)` means: drop what you hold above block `n`.
- A client that reconnects resumes from its last sealed block, without duplicates.
- A client that falls too far behind is disconnected and resumes when it reconnects.

`LIVE_LOGS=false` keeps only the block recorder, which stores every header with its arrival time. A chain file
without `rpc.ws` runs no websockets and polls for new blocks.

Memory is capped: the replay buffer keeps `live.ring_seconds` of blocks up to `live.ring_max_logs` logs, and
history windows shrink until they hold about `ledger.backfill_window_logs` logs.

## Settings

All settings are environment variables (a `.env` file works too).

| Setting | Meaning |
|---|---|
| `CHAIN` | A chain's name in the chain folder, or a chain file's path. |
| `CHAININDEX_CHAINS_DIR` | The chain folder. Default: the examples in `src/chainindex/chains/`. |
| `CHAININDEX_ABIS_DIR` | Extra ABIs, used before the shipped ones. |
| `RPC_API_KEY` | Replaces `{key}` in endpoint URLs. Without it, keyless endpoints are used. |
| `CLICKHOUSE_HOST`, `_PORT`, `_USER`, `_PASSWORD`, `_SECURE` | Where ClickHouse is. |
| `METRICS_PORT` | Serve Prometheus metrics on this port. Unset: no metrics. |
| `ENV` | `local` logs readable text; anything else logs JSON lines. |
| `LOG_LEVEL` | Default `INFO`. |
| `REGISTRY_REFRESH_SECONDS` | How often the running service rereads `contracts`, so `chainindex contracts add` and `remove` need no restart. Default `30`. |
| `LIVE_ENABLED`, `LIVE_LOGS`, `READS_ENABLED`, `EVENT_TRANSACTIONS_ENABLED`, `BACKFILL_REQUESTS_PER_SECOND` | Override the chain file. |

### Public API

| Setting | Default | Meaning |
|---|---|---|
| `API_CHAINS` | `base` | Chains to serve, comma-separated. |
| `API_HIDDEN_CONTRACTS` | empty | Contract kinds not to serve, such as your own wallet's. Deny them in ClickHouse too. |
| `API_PORT` | `8080` | |
| `API_MAX_ROWS`, `API_DEFAULT_ROWS` | `1000`, `100` | Rows per response. |
| `API_MAX_RANGE_SECONDS` | `3600` | The widest block range, in chain time, for a request without an address. |
| `API_QUERY_SECONDS` | `10` | Time limit per query. |
| `API_QUERY_MEMORY_BYTES` | 1 GiB | Memory limit per query. |
| `API_QUERY_ROWS_READ` | 100 million | Rows one query may read. |
| `API_CONCURRENT_QUERIES` | `4` | Queries running at once; more wait briefly, then get 503. |
| `API_REQUESTS_PER_MINUTE` | `60` | Each client's budget; past it, 429. A client also gets one request at a time. IPv6 clients are counted per /64. |
| `API_CLIENT_IP_HEADER` | empty | Behind a proxy, the header holding the client's address (Cloudflare: `CF-Connecting-IP`). |
| `API_TRUSTED_PROXIES` | empty | The proxies' addresses, as comma-separated CIDRs. The header is believed only from these. |

Give the API its own ClickHouse user with `readonly = 2`, `SELECT` on the `events_*` databases and the status tables,
and a quota.

## Metrics

Prometheus metrics are named `chainindex_*`. The rules in [deploy/prometheus/alerts.yml](../deploy/prometheus/alerts.yml)
alert on these:

| Metric | Alert when | Rule |
|---|---|---|
| `ledger_last_write_timestamp` | The head has not been written for 10 minutes. | `ChainindexLedgerStalled` |
| `up` | Prometheus cannot scrape a pod for 10 minutes. | `ChainindexMetricsMissing` |
| `ledger_last_write_timestamp` (absent) | A chain that exported it in the last day has no pod being scraped. | `ChainindexAbsent` |
| `ledger_confirmed_block` − `ledger_tip_block` | The tip lane is more than 5 minutes of chain time behind the confirmed head. | `ChainindexLedgerBehindHead` |
| `verify_last_check_timestamp` | The verifier has not checked a block for an hour. | `ChainindexVerifierStalled` |
| `live_last_head_timestamp` | No new block over the websocket for 5 minutes. | `ChainindexHeadsStale` |
| `audits_total{result="repaired"}`, `orphaned_blocks_total` | Data was repaired (informational). | `ChainindexDataRepaired` |

`ledger_lag_blocks` and `verify_lag_blocks` are on the dashboard but have no rule. `ledger_unresolved_forks_total`
(windows written while their hashes were disputed, which the audit checks again) has neither.

## Deploying

[deploy/](../deploy/README.md) has Kubernetes manifests for one chain with the optional API, Prometheus alerting rules
for the metrics above, and a Grafana dashboard.
