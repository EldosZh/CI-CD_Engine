# Verification record

Verified on 2026-11-02 (Asia/Qyzylorda), Windows amd64, Go 1.26.3.
Runtime files, test fixtures, downloads, and the portable compiler used for race
checks were kept in temporary directories. No repository commits or pushes were
created. The workspace initially contained no files.

## Commands and outcomes

| Check | Outcome |
| --- | --- |
| `go version` | `go1.26.3 windows/amd64` |
| `go mod tidy` | Passed |
| `gofmt -w cmd internal` | Completed |
| `go vet ./...` | Passed |
| `go test ./...` | Passed |
| `go test -race ./...` | Passed, with CGO and portable GCC |
| `go build -o cicd ./cmd/cicd` | Passed |
| `go build -o cicd.exe ./cmd/cicd` | Passed |
| `cicd --help`, `cicd version` | Passed; version 0.1.0 |
| Valid parallel workflow | Accepted, deterministic topological output |
| Invalid cycle workflow | Rejected: a -> b -> c -> a |
| Invalid artifact traversal workflow | Rejected before run creation |

Go's default cache directories were not writable in the sandbox. Verification
used temporary GOPATH/GOCACHE/GOMODCACHE paths, without global Go configuration
changes. A portable w64devkit GCC was downloaded to the temporary directory to
satisfy Windows race-detector requirements; it is not a project dependency.

## Execution and safety evidence

- Real CLI sequential workflow: SUCCESS, dependency preserved.
- Real CLI parallel DAG: SUCCESS; the three root job time intervals overlapped;
  build began after all three finished, and package waited for build.
- Controlled scheduler tests: at most two active workers with capacity two;
  dependency barriers prevent downstream execution until every prerequisite ends.
- Shared-slot tests: two simultaneous schedulers still respect the global bound.
- Real CLI failure workflow: FAILED; build/package SKIPPED, independent SUCCESS,
  failure-conditioned reporting SUCCESS.
- Real CLI timeout: slow job TIMED_OUT, downstream SKIPPED; helper-process tests
  verify direct process cancellation for step/job/workflow deadlines.
- Cancellation and graceful engine-close integration tests: CANCELLED, no dependent
  execution, completion within the tests' bounded waiting period.
- Real CLI retry: SUCCESS on exactly attempt three; tests also check capped backoff.
- Scoped environment tests: step overrides job/workflow; engine identifiers present.
- Real CLI secret fixture: raw value absent from every persisted log; unit tests
  verify masking across writes, concurrent writers, long lines, and live delivery.
- Real CLI artifact upload/list/download: archive exists; downloaded SHA-256 matches
  metadata. Unit tests check archive content, sizes, file limits, and raw checksums.
- Artifact traversal inputs rejected. Ordinary symlink creation was denied by this
  Windows account, so the symlink-specific test skipped. A Windows junction test
  passed: both direct and nested escape paths and snapshot copying were rejected.
- Filesystem restart tests preserve successful run/job/step and artifact metadata.
  Crash-state tests mark orphan run/job/step states INTERRUPTED.
- A real API daemon passed health/readiness, submission/status, streamed artifact
  download/checksum, CLI cancellation, and history-after-restart smoke tests.
  Killing and restarting that daemon changed an orphan RUNNING run to INTERRUPTED.
- HTTP integration tests check errors, authentication, submission body limit,
  jobs/logs/artifacts routes, cancellation, SSE event delivery/disconnect, and a
  multi-megabyte raw artifact. Downloads use `http.ServeFile`, not whole-file reads.
- Slow subscribers are disconnected after 128 buffered events without blocking
  workflow logging. Subscriber cleanup and concurrent writes are race tested.
- Artifact retention tests preserve active/new runs and remove expired terminal
  artifacts. The daemon runs the retention pass once per minute.
- A fixed-seed 1,000-job stress test completes every eligible job once and preserves
  dependency ordering and the configured worker bound.

## Actual scheduler-only measurements

One CLI invocation on this machine (not a performance guarantee):

```text
jobs=10000 workers=32
elapsed=31.2316ms
throughput=320189 jobs/s
dispatch-to-observed-completion p50=0s p95=511.5us
```

The zero median reflects the clock resolution/very short no-op work; it does not
mean zero execution cost. This is not shell, Docker, or durable-engine throughput.

`go test ./internal/scheduler -run '^$' -bench BenchmarkScheduler -benchmem -benchtime=1s`:

```text
BenchmarkScheduler-8  304  4288271 ns/op  986598 B/op  11139 allocs/op
```

Each benchmark operation schedules 1,000 no-op jobs with 16 workers. CPU reported:
11th Gen Intel Core i5-1155G7 @ 2.50GHz. Results vary by load and environment.

## Unverified or unavailable

Docker CLI 29.4.3 was present, but its daemon pipe did not exist. Docker lifecycle,
Docker timeout/cleanup, Docker image build, and Compose execution were therefore
not verified. Docker tests skip with an explicit reason. The Docker test requires
an already cached Alpine image to stay offline. Unix runtime behavior was not
executed on this Windows host. Ordinary symlink creation requires Windows privilege
or Developer Mode; junction rejection was verified instead. Cache, matrix, remote
workers, and dashboard are intentionally absent, so there are no checks to claim.

This is ready for source review and local-runner use. Docker and target Unix runtime
checks should run on those environments before treating their paths as verified.

## Additional checks

The serial coverage run (`go test -p 1 ./... -cover`) passed: API 78.1%, DAG 94.1%,
engine 73.4%, logs 77.9%, runner 42.5%, scheduler 86.5%, storage 72.4%, workflow
85.1%. CLI code has no automated statement coverage; its commands were smoke-tested
as described above. Runner coverage excludes unavailable Docker execution.

A three-second parser fuzz campaign passed with 10,470 executions and 34 new
interesting inputs (36 total corpus entries). This is a short smoke campaign,
not a comprehensive fuzzing claim. An initial parallel coverage/fuzz attempt ran
out of disk space; after temporary-tool cleanup, the serial coverage and fuzz
reruns passed. The final offline Docker-test guard was then checked again with
the temporary C compiler before cleanup.

`--job build --workers 1` was also smoke-tested: the selected build plus exactly
its transitive lint/security/test dependencies succeeded; package was excluded.
