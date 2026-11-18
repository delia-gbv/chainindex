# Security

Please report a vulnerability privately through GitHub's
[security advisories](https://github.com/delia-gbv/chainindex/security/advisories/new), not in a public issue. You will
get an answer within a few days.

The parts that face untrusted input are the read-only API (`chainindex api`) and the live stream (`/stream`). The API
accepts no SQL: contracts and events come from the chain files and every value is a bound parameter. Run it with a
read-only ClickHouse user, as described in [docs/operations.md](docs/operations.md#public-api).
