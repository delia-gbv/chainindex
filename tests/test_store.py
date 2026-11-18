"""The store's writes, against a client that records them."""

import json
from pathlib import Path
from typing import Any

from chainindex.models import Log
from chainindex.store import AuditRow, ChainStore, Cursor

FIXTURE = Path(__file__).parent / "fixtures" / "base_logs.json"


class RecordingClient:
    """A ClickHouse client that keeps every insert's table and settings, and answers every query with no rows."""

    def __init__(self) -> None:
        self.inserts: list[tuple[str, dict[str, Any]]] = []

    def insert(self, table: str, rows: list[list[Any]], *, column_names: list[str], settings: dict[str, Any]) -> None:
        self.inserts.append((table, settings))

    def query(self, sql: str, parameters: dict[str, Any] | None = None) -> Any:
        return type("Result", (), {"result_rows": []})()


async def test_every_write_skips_the_async_insert_buffer_and_keeps_its_own_settings():
    client = RecordingClient()
    store = ChainStore(client, "chain_test")
    await store.save_cursors([Cursor(stream="address:0xa", from_block=1, to_block=2)])
    await store.record_audits(
        [AuditRow(kind="recent", from_block=1, to_block=2, streams=1, rpc_logs=0, stored_logs=0, missing_logs=0, extra_logs=0, repaired_logs=0)]
    )
    await store.record_orphans([(1, "0xb", "0xc")], detected_by="verify")

    assert [table for table, _settings in client.inserts] == ["chain_test.cursors", "chain_test.audits", "chain_test.orphaned_blocks"]
    assert all(settings == {"async_insert": 0} for _table, settings in client.inserts)


async def test_a_log_insert_keeps_its_deduplication_token_without_the_async_insert_buffer():
    client = RecordingClient()
    store = ChainStore(client, "chain_test")
    logs = [Log.from_rpc(item) for item in json.loads(FIXTURE.read_text())[:3]]
    assert await store.insert_logs(logs, lane="tip") == (3, 0)

    [(table, settings)] = client.inserts
    assert table == "chain_test.logs"
    assert settings["async_insert"] == 0
    assert settings["insert_deduplication_token"].startswith("logs:")
