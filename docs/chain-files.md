# Chain files

A chain file is a YAML file that says which chain to follow, which RPC endpoints to use, and which contracts to
index. The examples are in `src/chainindex/chains/`. Set `CHAININDEX_CHAINS_DIR` to use your own folder, and
`CHAININDEX_ABIS_DIR` for ABIs that are not shipped in `src/chainindex/abis/`.

```yaml
name: base
chain_id: 8453
block_seconds: 0.2
rpc:
  http: https://base-rpc.publicnode.com/{key}     # {key} is replaced by RPC_API_KEY
  ws: wss://base-rpc.publicnode.com/{key}
ledger:
  confirmations: 30                                # write a log once its block is this deep
contracts:
  - ...
```

## Contract kinds

Each entry under `contracts` is a kind: a name, an ABI, and how its addresses are found.

| `track` | Which addresses | Notes |
|---|---|---|
| `listed` (default) | The `addresses` listed, plus any added with `chainindex contracts add`. | |
| `all` | Every contract the kind's factories create. | Each is fetched by address from its creation, or with `fetch: topics` by one scan of the kind's events where the provider allows chain-wide queries. |
| `matching` | Created contracts whose creation event matches `match: {param: [values]}`. | Or `match_contract`, below. |
| `created` | Every created contract, without keeping a list of them. | For factories with too many children to list (Uniswap v3). Needs `fetch: topics`. |
| `any` | Every contract that emits the kind's events. | For a standard event many contracts share. Consumers pick the addresses they trust. Needs `fetch: topics`. |

A factory names what it creates:

```yaml
- name: uniswap_v3_factory
  abi: UniswapV3Factory
  addresses: [{address: "0x1F7d...", start_block: 9000}]
  creates: {event: PoolCreated, param: pool, contract: uniswap_v3_pool}  # param as the ABI names it, camelCase or not
```

Other options:

- `events: [Swap, Mint]` indexes only those events. Without it, every event in the ABI is indexed.
- `fetch: topics` asks the node for an event across the whole chain and keeps the known addresses. That is one
  request however many addresses there are, but some providers refuse it; `chainindex probe` tells you.
- No `abi`: every log the addresses emit is stored, and dbt gets a `<kind>__logs` view of the raw topics and data.
  Adding an ABI later decodes all of it.
- `match_contract: {pool: aerodrome_cl_pool}` keeps created contracts whose `pool` parameter is an address of another
  indexed kind, such as the gauges of the pools you index.
- `refetch: 1` fetches a kind's history again, for example after adding an event to its ABI. Raise the number to do
  it again. Logs already stored are skipped.

### `created` kinds

All `created` kinds of a chain share one chain-wide scan that stores every log with their events, whoever emitted it.
Because anyone can emit those events, the dbt views keep a log only if its address was created by a configured
factory. The scan never gets ahead of its factories, so a contract's creation is always stored before its logs.
Changing the events of a `created` kind starts the scan again from the beginning.

### Adding contracts at runtime

```bash
chainindex --chain base contracts add aerodrome_cl_pool 0x... --from-block 50300000 --label "USDC/XYZ"
chainindex --chain base contracts remove 0x...
```

The running service picks the change up within 30 seconds, fetches the new contract's history, then follows it live.

## Contract state (`reads`)

`reads` records the result of view functions at confirmed blocks, in one Multicall3 request per block. A result is
stored only when it changed, so a value holds until its next row.

```yaml
reads:
  - name: cl_pool_state
    contract: aerodrome_cl_pool
    every_blocks: 1
    calls: ["slot0() returns (uint160 sqrt_price_x96, int24 tick)", "liquidity() returns (uint128 liquidity)"]
  - name: cl_pool_ticks
    contract: aerodrome_cl_pool
    every_blocks: 15
    tick_map: {spacings: 10}     # the ticks around each pool's price
```

dbt decodes them into `events_<chain>.read__<name>`. A kind with `events: []` fetches no logs and exists only for its
reads. `READS_ENABLED=false` turns reads off.

## Your own wallets (`accounts`)

To index a wallet's own activity without everyone else's, list it under `accounts` and give a kind
`account_topics`: only logs that name an account in those topic positions are fetched.

```yaml
accounts:
  - {address: "0x...", label: "my wallet"}
contracts:
  - name: account_token
    abi: AccountToken
    account_topics: [1, 2]        # Transfer's from (topic 1) and to (topic 2)
    start_block: 51750000
    addresses: [{address: "0x8335...", label: "USDC"}]
```

`unfiltered_events` lists a kind's events that name no account and should be kept in full, such as a token's
multiplier changes.

With accounts listed, chainindex also stores each account's transactions with their receipts and fees (including the
L1 data fee on OP-stack chains) in `transactions`. It finds them three ways:

- from the stored logs that name the account;
- by the account's nonce, for transactions that log nothing about it (a nonce that should be in a range but is not
  is searched for block by block);
- by the account's balance, for native coins received without a log.

Anything it cannot explain is recorded in `audits`. This needs an endpoint that answers calls at past blocks (set
`rpc.reads`).

## Transactions behind events

To know who sent each swap, through which router and at what priority fee:

```yaml
rpc:
  transactions: {requests_per_second: 2, max_concurrency: 2}
contracts:
  - name: aerodrome_cl_pool
    transactions: {events: [Swap], start_block: 52011727}
```

chainindex fetches the transaction of every stored `Swap` by hash, 100 per request, and stores it once in
`event_transactions`. It skips receipts: a reverted transaction emits no logs, so these all succeeded, and a mined
transaction's `gasPrice` is what it paid. dbt gets `<kind>__swap_transactions`, each swap next to its transaction.
`EVENT_TRANSACTIONS_ENABLED=false` turns this off.

## RPC lanes

Each kind of work has a lane with its own rate and concurrency, and optionally its own endpoint:

| Lane | Used for |
|---|---|
| `stream` | The live stream's HTTP checks. |
| `ledger` | Confirmed logs at the chain head. |
| `backfill` | History, and the auditor. |
| `headers` | Block headers (timestamps, the verifier). Optional. |
| `reads` | `eth_call` at past blocks. Optional. |
| `addresses` | History of address-fetched kinds, when topic scans use another endpoint. Optional. |
| `transactions` | Transactions behind events. Optional. |

```yaml
rpc:
  http: https://archive-provider.example/{key}
  addresses_per_request: 9          # the provider's limit per eth_getLogs
  backfill: {http: "https://rpc.your-chain.example", requests_per_second: 2, max_concurrency: 3}
  headers: {requests_per_second: 12, max_concurrency: 6}
  headers_per_request: 500
```

When a provider refuses a request as too large, chainindex halves the block range or the number of addresses. The
block range doubles back after each answered request. After 100 answered requests at a shrunk address count, one
request tries twice as many addresses, never more than `addresses_per_request`. If it is answered, the larger count
stays. If it is refused, that one request is the only cost. The log lines are
`<lane> address chunk shrinks to <n> after: <error>` and `<lane> address chunk grows back to <n>`. When it throttles,
every background lane pauses. Changes to these settings never lose data, only speed.

When an endpoint fails, for example with a timeout, an HTTP 5xx or a refused connection, its lane rests 2 s. Each
further failure doubles the rest, up to 60 s, and a success resets it. Requests that fail together count as one
failure. If the node dropped a kept-alive connection, the request goes out again at once on a new connection, without
a rest (once per request). A request gives up after 8 attempts. Each rest logs
`The <lane> lane failed <method> (<reason>); resting it <n> s`.

When a tip window fails at the provider, the tip lane tries its streams one at a time. A stream that still fails is
left behind and the backfill lane takes it over (`Tip window <start>-<end> failed for <stream>; the backfill lane
takes it over`). When ClickHouse fails instead (an error or no connection), the whole window is retried after 5 s
(`Tip lane step failed, retrying in 5 s`): its logs were already fetched, and every stream would fail alike.
