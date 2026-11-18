# Deploying chainindex

Examples for running chainindex in production. They are a starting point: copy them and change the names, the
ClickHouse address and the chains to fit your setup.

## kubernetes/

Kustomize manifests for one chain (Base) in the namespace `chainindex`.

| File | What it is |
|---|---|
| `kustomization.yaml` | The settings (as ConfigMaps) and the image. Start here. |
| `namespace.yaml` | The namespace. |
| `indexer.yaml` | The indexer's Deployment and the Service for its live stream on :8547. |
| `secret.example.yaml` | The two Secrets it expects, with placeholder values. Not applied. |
| `api.yaml` | Optional: the read-only API on :8080. |
| `jobs.yaml` | Optional: `chainindex init` and `dbt build` as suspended CronJobs, started by hand. |
| `netpol.yaml` | Optional: NetworkPolicies that deny everything else. |

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl -n chainindex create secret generic chainindex-env \
  --from-literal=CLICKHOUSE_USER=chainindex --from-literal=CLICKHOUSE_PASSWORD=... --from-literal=RPC_API_KEY=...
kubectl -n chainindex create secret generic chainindex-api-env \
  --from-literal=CLICKHOUSE_USER=chainindex_api --from-literal=CLICKHOUSE_PASSWORD=...
kubectl apply -k deploy/kubernetes
kubectl -n chainindex create job --from=cronjob/chainindex-dbt chainindex-dbt-$(date +%s)   # the decoded views
```

Run the dbt job again only after a chain file or ABI changes: the views read the raw tables live. The indexer creates
its database when it starts, so the init job is needed only to prepare the database before that.

The indexer runs one replica with the `Recreate` strategy, since each chain's database has one writer. It runs as a
non-root user on a read-only filesystem. It is ready once its live stream can serve reconnecting clients.

For a second chain, copy `indexer.yaml`, replace `base` with the chain's name, and add the file to
`kustomization.yaml`. To index your own contracts, put your chain files in a ConfigMap and point
`CHAININDEX_CHAINS_DIR` at it. The commented lines in `kustomization.yaml` and `indexer.yaml` show how.

## prometheus/

- `alerts.yml`: alerting rules for a stalled ledger, a ledger behind the head, a stalled verifier, stale live heads,
  missing metrics, a chain that is not running at all, and repaired data. Check them with
  `promtool check rules deploy/prometheus/alerts.yml`.
- `alerts_test.yml`: unit tests for every rule, healthy and failing, run with
  `promtool test rules deploy/prometheus/alerts_test.yml` (promtool 3.9 or newer). `tests/test_alerts.py` runs both
  commands when `promtool` is on PATH and is skipped otherwise.
- `prometheus.yml`: a scrape job that finds the indexer pods and names each job after its chain's Deployment.

The rules and the dashboard tell chains apart by the `job` label, so give each chain its own job.

## grafana/

`chainindex.json` is a dashboard for one chain at a time: the ledger, RPC lanes, verifier, live heads, reads and
audits. Import it in Grafana (Dashboards, New, Import), pick your Prometheus data source, then pick the chain at the
top. It uses only `chainindex_*` metrics and the process metrics served next to them.
