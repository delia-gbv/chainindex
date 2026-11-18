#!/usr/bin/env bash
# Runs every check CI runs, in the same order, so a green run here is a green run there. Needs a ClickHouse on
# CLICKHOUSE_HOST (`docker compose up -d clickhouse`) and anvil on PATH for the end-to-end tests.
set -euo pipefail

uv sync --frozen
uv run ruff check src tests
uv run mypy
uv run chainindex dbt --check --models-dir dbt/models
uv run python -m pytest -q -m "not clickhouse and not e2e" -n 2 --cov
uv run python -m pytest -q -m "clickhouse or e2e" --cov --cov-append --cov-report=term --cov-report=markdown:coverage.md
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then cat coverage.md >> "$GITHUB_STEP_SUMMARY"; fi
