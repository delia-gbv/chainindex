"""Prometheus metrics of the chain service (``chainindex_*``)."""

from chainindex.prometheus import MetricsRegistry, counter, gauge, histogram


class Metrics(MetricsRegistry):
    """Every metric the service exports, labelled by chain through the scrape job."""

    rpc_requests = counter(
        "rpc_requests_total", "RPC requests by lane, method and outcome", labels=["lane", "method", "outcome"],
    )
    rpc_seconds = histogram(
        "rpc_seconds", "RPC round trip", labels=["lane", "method"],
        buckets=[0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30],
    )

    ledger_confirmed_block = gauge("ledger_confirmed_block", "Newest block the ledger may write")
    ledger_tip_block = gauge("ledger_tip_block", "Newest block the tip lane has written")
    ledger_lag_blocks = gauge("ledger_lag_blocks", "Blocks between the chain head and the tip lane")
    ledger_streams = gauge("ledger_streams", "Indexed streams by lane", labels=["lane"])
    ledger_behind_blocks = gauge("ledger_behind_blocks", "Blocks the backfill lane still has to fetch")
    ledger_logs = counter("ledger_logs_total", "Logs inserted", labels=["lane"])
    ledger_logs_already_stored = counter("ledger_logs_already_stored_total", "Fetched logs already in ClickHouse", labels=["lane"])
    ledger_windows = counter("ledger_windows_total", "Windows fetched and written", labels=["lane"])
    ledger_last_write = gauge("ledger_last_write_timestamp", "Unix time of the tip lane's last completed window")
    ledger_hash_mismatches = counter("ledger_hash_mismatches_total", "Windows refetched because a log's block hash was not canonical")

    live_head_block = gauge("live_head_block", "Newest head seen by the live feed")
    live_sealed_block = gauge("live_sealed_block", "Newest block whose logs are complete on the stream")
    live_last_head = gauge("live_last_head_timestamp", "Unix time the newest head arrived")
    live_ws_connected = gauge("live_ws_connected", "Whether each websocket is subscribed", labels=["socket"])
    live_ws_reconnects = counter("live_ws_reconnects_total", "Websocket reconnects", labels=["socket"])
    live_ws_first = counter("live_ws_first_total", "Logs a websocket delivered before any other source", labels=["socket"])
    live_sealed_late = counter("live_sealed_late_logs_total", "Logs the HTTP seal found that no websocket delivered")
    live_reorgs = counter("live_reorgs_total", "Tip reorgs")
    live_subscribers = gauge("live_subscribers", "Connected stream subscribers")
    live_subscriber_drops = counter("live_subscriber_drops_total", "Subscribers dropped for falling behind")
    live_head_arrival_seconds = histogram(
        "live_head_arrival_seconds", "From a block's whole-second stamp to its head's first arrival over a websocket",
        buckets=[0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 1.5, 2, 4],
    )
    live_head_latency_seconds = histogram(
        "live_head_latency_seconds",
        "From a block's estimated time (its stamp plus its place among the blocks sharing that second) to its head's arrival",
        buckets=[-0.25, -0.1, 0, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1, 2],
    )
    live_publish_seconds = histogram(
        "live_publish_seconds", "Delay from a log's block timestamp to its publication",
        buckets=[0.1, 0.25, 0.5, 1, 2, 4, 8, 16],
    )

    audits = counter("audits_total", "Audit checks by kind and result", labels=["kind", "result"])
    audit_missing_logs = counter("audit_missing_logs_total", "Logs the RPC had and ClickHouse did not, now inserted")
    audit_extra_logs = counter("audit_extra_logs_total", "Logs ClickHouse had and the RPC did not")
    orphaned_blocks = counter("orphaned_blocks_total", "Stored blocks found off the canonical chain")
    cross_check_mismatches = counter("cross_check_mismatches_total", "Ledger windows whose logs differ from what the stream sealed")
    ledger_unresolved_forks = counter(
        "ledger_unresolved_forks_total", "Windows written although the node's block hashes still disagreed with the live chain",
    )

    verify_block = gauge("verify_block", "Newest block whose stored hashes are checked against the canonical chain")
    verify_floor_block = gauge("verify_floor_block", "Oldest block of the verified range")
    verify_lag_blocks = gauge("verify_lag_blocks", "Blocks deep enough to verify that the verifier has not reached yet")
    verify_last_check = gauge("verify_last_check_timestamp", "Unix time of the verifier's last completed window")
    verify_blocks = counter("verify_blocks_total", "Stored blocks whose hash was checked, by tip or history", labels=["kind"])
    verify_unrepaired = counter("verify_unrepaired_total", "Off-chain block ranges no single stream covered, so not re-fetched")

    reads_block = gauge("reads_block", "Newest block each read has stored", labels=["read"])
    reads_rows = counter("reads_rows_total", "Call results stored", labels=["read"])
    reads_failed_calls = counter("reads_failed_calls_total", "Calls that reverted inside the multicall", labels=["read"])
    reads_skipped_blocks = counter("reads_skipped_blocks_total", "Blocks skipped after downtime past the catch-up limit", labels=["read"])

    event_transactions_block = gauge(
        "event_transactions_block", "Newest block whose events' transactions are stored", labels=["contract"],
    )
    event_transactions_rows = counter("event_transactions_rows_total", "Event transactions stored", labels=["contract"])
    event_transactions_unknown = counter(
        "event_transactions_unknown_total", "Event transactions the node did not know when asked", labels=["contract"],
    )

    component_restarts = counter("component_restarts_total", "Restarts of a supervised component", labels=["component"])
