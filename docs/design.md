# Design

Why chainindex is built the way it is. For how to use it, see the [README](../README.md).

## Decisions

**Write only confirmed data, fetched over HTTP.** The database is filled from `eth_getLogs` once blocks are
confirmed, never from websockets. Websockets feed the live stream only, which is fast but can miss or repeat events.

**Never update or delete.** Every raw table is append-only. A block found later to be off the chain is recorded in
`orphaned_blocks`, and the views skip it. Readers never need `FINAL`, and ClickHouse never runs a mutation, which
matters on slow disks.

**Check every block once, and sample the rest.** Waiting for confirmations makes a late reorg rare, not impossible.
Random audits miss most blocks of a fast chain: a chain with 0.1 s blocks makes 10 a second, and the auditor checks
about 3. So the verifier checks the hash of every block that holds data once the block is 15 minutes old
(`verify.depth_seconds`), then walks back through history. That costs one header per such block. The auditor keeps
looking for missing logs.

**One cursor per contract.** Each address, or each kind fetched by topic, tracks its own progress. Adding a contract
fetches only that contract's history, while everything else keeps running.

**Store raw, decode when reading.** Logs and call results are stored once, as returned. The decoded tables are dbt
views. On one chain, decoded copies of Uniswap v4's swaps alone would have taken 22 GiB.
This works because `logs` is sorted by event and address: decoding one event's full history (3.6M swaps) took about
625 s when the table was sorted by block, and reads only that event's rows now. Compressing the hex columns with ZSTD
also made the table about 40% smaller. Call results repeated their previous value 99% of the time, so only changes
are stored.

**Fetch other people's transactions by hash.** To learn who sent each swap and at what fee, chainindex fetches
transactions by the hashes in stored swaps, 100 per request. Fetching whole blocks would cost about 50 times the
bytes, and receipts would double the requests for fields nobody needed.

**Out of scope.** Sending transactions, quoting against the latest state, and off-chain data.

## Compared with other indexers

| | chainindex | [tidx](https://github.com/tempoxyz/tidx) (Tempo) | Subgraphs, Ponder, rindexer | [cryo](https://github.com/paradigmxyz/cryo) |
|---|---|---|---|---|
| What it stores | The contracts you list | Every block, transaction and receipt | Tables your handlers write | Files you extract |
| Decoding | When reading (dbt views) | When reading (SQL from an event signature) | When indexing | When extracting |
| Reorgs | Wait, then mark orphaned blocks | Delete and rewrite | Roll back handler writes | Not tracked |
| Checks | Every block verified, random audits | Parent hash on every batch | Varies | None |
| Storage | ClickHouse | PostgreSQL and ClickHouse | PostgreSQL mostly | Parquet |
| Fits | Selected contracts on several chains with a small RPC budget | One chain in full, for an explorer | App backends | One-off research |

The verifier came from this comparison: tidx checks every batch's parent hash before writing, and chainindex only
had random audits. tidx stores whole blocks, which works for a young chain but would need far more RPC and disk on
Base.
