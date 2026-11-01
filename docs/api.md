# REST API

Start: `cicd serve --addr 127.0.0.1:8080 --workspace . --workers 4 --data data`.
Set `CICD_API_TOKEN` to require `Authorization: Bearer TOKEN` for every route except
`/health`. Non-loopback binding requires a token. TLS is provided externally.

| Method | Path | Response |
| --- | --- | --- |
| GET | `/health` | Process liveness JSON |
| GET | `/ready` | 200 when accepting work and data is writable; 503 otherwise |
| GET | `/metrics` | Prometheus text counters/gauges |
| POST | `/api/v1/runs` | Raw version-1 YAML body; 202 with `{ "id": "..." }` |
| GET | `/api/v1/runs` | Most recent 100 run snapshots |
| GET | `/api/v1/runs/{id}` | Run with job/step state, timestamps, attempts, errors |
| GET | `/api/v1/runs/{id}/jobs` | Job ID -> result mapping |
| POST | `/api/v1/runs/{id}/cancel` | Idempotent cancellation request; terminal runs unchanged |
| GET | `/api/v1/runs/{id}/logs` | JSONL, capped at first 8 MiB |
| GET | `/api/v1/runs/{id}/logs/stream` | Future events as SSE; no replay |
| GET | `/api/v1/runs/{id}/artifacts` | Artifact metadata list |
| GET | `/api/v1/runs/{id}/artifacts/{name}` | Streamed tar.gz; `X-Checksum-SHA256` header |

Submission has a 1 MiB limit. The engine's fixed `--workspace` is the source for
all runs; API clients cannot choose host paths. Rejected workflows return 400,
over-size bodies 413, unavailable/capacity-rejected submissions 429, missing
resources 404, authentication failures 401. Server errors omit internal details.

```json
{"error":{"code":"WORKFLOW_INVALID","message":"version must be 1"}}
```

```sh
curl --data-binary @examples/parallel.yaml -H 'Content-Type: application/yaml' http://127.0.0.1:8080/api/v1/runs
curl http://127.0.0.1:8080/api/v1/runs
curl -N http://127.0.0.1:8080/api/v1/runs/RUN_ID/logs/stream
curl -X POST http://127.0.0.1:8080/api/v1/runs/RUN_ID/cancel
curl -o app.tar.gz http://127.0.0.1:8080/api/v1/runs/RUN_ID/artifacts/app
```

SSE starts with a connected comment, sends `id: SEQUENCE` and JSON `data:` events,
and emits heartbeats every five seconds. IDs are per-run sequence numbers.
`Last-Event-ID` replay is not implemented. A full 128-event subscriber buffer
closes that subscription; use persisted JSONL to recover missing output. When a
run finishes its stream closes. Disconnecting does not cancel the workflow.

Metrics: `cicd_runs_total`, `cicd_runs_active`, `cicd_jobs_total`,
`cicd_jobs_active`, `cicd_worker_slots_used`, and `cicd_worker_slots_total`.
Counters reset on engine restart. Active job execution includes preparation and
runner finalization. Metrics are operational snapshots, not billing or audit data.
