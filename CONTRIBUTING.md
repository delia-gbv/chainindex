# Contributing

Thanks for helping. Bug reports, chain presets, ABIs and fixes are all welcome.

## Set up

```bash
git clone https://github.com/delia-gbv/chainindex && cd chainindex
uv sync
uv run python -m pytest -q -m "not clickhouse and not e2e"
```

The ClickHouse and end-to-end tests need a local ClickHouse; the end-to-end tests also need
[anvil](https://book.getfoundry.sh/getting-started/installation):

```bash
docker compose up -d clickhouse
CLICKHOUSE_HOST=localhost uv run python -m pytest -q -m "clickhouse or e2e"
```

## Before you open a pull request

`scripts/ci.sh` runs every check CI runs, in the same order. It needs the local ClickHouse and anvil above:

```bash
CLICKHOUSE_HOST=localhost scripts/ci.sh
```

Without them, run the checks that need neither:

```bash
uv run ruff check src tests
uv run mypy
uv run chainindex dbt --check --models-dir dbt/models   # stale after a chain file or ABI change; drop `--check` to regenerate
```

- Add a test for every behavior you change. Test names are sentences about behavior, such as
  `test_a_repair_that_fails_is_tried_again_before_its_block_is_recorded`.
- Keep the raw tables append-only. A change to an existing table is a new entry in `MIGRATIONS` (`src/chainindex/store.py`), never an edit of an old one.
- Add a line under `Unreleased` in `CHANGELOG.md`.
- Write code a new reader understands at first read: plain names, one-sentence docstrings, comments that say why.

## Releasing

Bump `version` in `pyproject.toml`, move the `Unreleased` notes under the new version in `CHANGELOG.md`, merge, then
push a tag `vX.Y.Z`. The release workflow publishes to PyPI, to `ghcr.io/delia-gbv/chainindex` and to GitHub releases.
