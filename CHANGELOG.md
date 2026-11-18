# Changelog

All notable changes to chainindex. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] - 2026-11-14

### Fixed
- A backfill window ClickHouse refuses is retried whole, as a tip window is. It was split into its streams, each
  fetched again alone, and each one that failed was rested, for up to ten minutes after a long outage: after twelve
  refused steps, 60 address streams took 72 steps and 1,248 requests to pass the window instead of 2 and 120.
- A registry refresh ClickHouse refuses is tried again on the next refresh, as every other loop retries its own step.
  It was the only component that crashed instead: every `component_restarts_total` in 18 days (14 to 16 a chain) was
  the registry meeting a ClickHouse blip, each followed by a restart backoff of up to 60 s.
- Writes skip ClickHouse's async-insert buffer. A server that turns async inserts on (26.x does by default) held each
  write about 55 ms (cursors and audits, median, on a chain with sub-second blocks) behind the store's write lock, though one process's
  writes come one at a time and the buffer never had another to merge them with; a write now takes a few ms.
- Event transactions kinds stepping the same window read its stored logs and transactions once a step, not once a
  kind. On one chain four kinds (three sharing Swap) sent seven reads a window; they now send three.
- A tip window ClickHouse refuses is retried whole, not stream by stream. Its logs were already fetched and ClickHouse
  fails every stream alike, so going one by one only fetched each stream's logs again (1,725 requests on a Base with
  1,725 streams, three minutes of its lane's budget) and marked every stream as failing.
- An address chunk shrunk by a refusal grows back. One transient error (`internal filter error`) shrank it for the
  life of the process, so a lane went on sending one request per address: once 100 requests are answered at the
  shrunk size, one request tries twice as many, and an answer keeps the larger chunk.
- A JSON-RPC batch the provider refuses for its size is split, not treated as an outage that rests the lane. The
  verifier uses the chain's `headers_per_request` with or without a headers lane.
- A websocket that dropped after subscribing reconnects after 1 s again: the backoff never started over, so after a
  few drops every reconnect waited 30 s.
- A factory fetched by topics registers the children its creation events announce. Only address-fetched factories'
  logs were looked at, so a topic-scanned factory's children were never found and none of their logs were fetched.
- A kept-alive connection the node dropped as a request went out on it is redialed at once instead of resting the lane
  2 s, as if the endpoint were down. The tip lane's first call after a poll met it most.
- Requests that fail together rest their lane once: each one in flight doubled the rest, so a single blip under four
  concurrent requests rested the lane 16 s instead of 2 s.
- The API gives each IPv4 client behind a dual-stack proxy its own request budget. Written as IPv6
  (`::ffff:a.b.c.d`), every such client fell in the one bucket `::/64`, so a single client could use up the budget of
  all of them; and a trusted proxy reaching the API at such an address is now recognized.
- The auditor counts a `track: any` kind's stored logs as stored. It compared them by member address, and such a kind
  has no members, so every one of its logs in a window was reported missing and the window "repaired", with nothing
  inserted.
- The example alerts fire for a chain that is not running at all (`ChainindexAbsent`): a pod that is gone leaves no
  `up` series at 0 and no ledger series, so no rule fired. `ChainindexVerifierStalled` and `ChainindexHeadsStale`
  give a process that has not checked or seen a head since it started the same threshold as one that stopped, and
  `ChainindexHeadsStale` no longer fires for chains with live disabled. `deploy/prometheus/alerts_test.yml` tests every
  rule with `promtool test rules`, and `tests/test_alerts.py` runs it when promtool is on PATH.

### Added
- End-to-end tests against a real anvil node and ClickHouse: logs written once after confirmations, a reorg deeper
  than the confirmations repaired by the verifier, and a restart that writes nothing twice.
- Kubernetes manifests, Prometheus alerts and a Grafana dashboard in `deploy/`.
- Example chains for Base (Aerodrome's WETH/USDC pools and gauges) and Ethereum (USDC and Uniswap v3), and standard
  `ERC20` and `WETH9` ABIs.
- `scripts/ci.sh` runs every check CI runs.
- Strict type checking with mypy in CI, and coverage reports.
- Releases publish to PyPI, to `ghcr.io/delia-gbv/chainindex` (amd64 and arm64), and to GitHub releases from a tag.

### Changed
- chainindex lives at `github.com/delia-gbv/chainindex`, and Delia maintains it. Old `Tburm/chainindex` links redirect.
- `track: all` no longer needs `fetch: topics`: without it each child is fetched by address from its creation, for
  providers that refuse chain-wide topic queries.
- A factory's `creates.param` and a child's `match` keys may be camelCase, as the ABI names them.
- Tested on Python 3.12 through 3.15 (3.15 by default), uv 0.13.6, and GitHub Actions pinned to commit SHAs on their
  Node 24 releases. The image builds in two stages and is about a third smaller.
- The tip lane takes along address streams just behind it, so many streams no longer trail the tip forever.
- The event transactions lane writes all its kinds' audits in one insert and their cursors in another each step,
  instead of two single-row inserts per kind.

### Removed
- Example chains and ABIs that served one deployment only. Keep your own chain files and ABIs in folders named by
  `CHAININDEX_CHAINS_DIR` and `CHAININDEX_ABIS_DIR`.

## [0.1.1] - 2026-10-27

### Fixed
- The verifier waits for streams near the chain head, so blocks a catching-up or resting stream writes late are still
  checked.
- A repair that fails is retried: orphaned blocks are recorded only after the correct logs are stored.
- Only a header the node no longer has stops the history walk; a passing RPC failure does not.
- A factory with `refetch` gates its children by the cursor it actually writes.
- The API takes one request at a time per client, believes the client address header only from trusted proxies,
  counts IPv6 clients per /64, caps rows read per query, and caches only successful answers.

## [0.1.0] - 2026-10-27

First release, extracted from an indexer running in production.

### Added
- Confirmed raw logs written once to ClickHouse, one cursor per contract, factories and their children.
- The verifier: every stored block's hash checked once against the chain, forks recorded and repaired.
- The auditor, contract state reads, account transactions and the transactions behind events.
- dbt views generated from ABIs that decode logs on read.
- A live websocket stream of decoded events with reorg notices.
- A read-only HTTP API of fixed queries.

[Unreleased]: https://github.com/delia-gbv/chainindex/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/delia-gbv/chainindex/releases/tag/v0.2.0
