# Implementation report

1. **Summary:** A working single-node Go workflow engine with its own parser,
   graph, scheduler, execution lifecycle, CLI, API, and durable local output.
2. **Structure:** `cmd/cicd`; `internal/{workflow,dag,scheduler,runner,engine,logs,storage,api}`;
   `examples`; `docs`; `scripts`; Docker/Compose recipes; MIT license.
3. **YAML:** Version/name/on/env/timeout/jobs; needs/runs-on/container/if/steps;
   run/command/shell/uses/artifact/env/timeout/retry/continue_on_error. Full field
   reference: [workflow format](workflow-format.md).
4. **Parser:** Typed `yaml.v3` decoding, known-fields checking, exactly one document,
   input bound, then validation. No YAML-to-monolithic-shell translation.
5. **Validation:** Names/version, IDs/duplicate keys, missing/self/duplicate needs,
   cycles, job/step bounds, actions, timeouts, retries, environment keys, runner/image,
   conditions, artifact names/paths. Multiple semantic errors are collected.
6. **DAG:** Explicit Needs and Children adjacency maps.
7. **Cycles:** DFS colors plus active-stack path reporting before execution.
8. **Topological order:** Kahn indegree counters with a lexical min-heap.
9. **Concurrency:** One coordinator owns scheduling state; worker completions use
   message passing. Engine progress persistence uses a short mutex.
10. **Workers:** Fixed goroutines per run, bounded completion channel, and shared
    engine-wide job semaphore. Active runs are separately bounded.
11. **Job states:** PENDING -> READY -> RUNNING -> SUCCESS/FAILED/CANCELLED/TIMED_OUT;
    unresolved nodes may SKIP or cancel; restart marks nonterminal nodes INTERRUPTED.
12. **Step states:** Similar lifecycle; retries retain RUNNING. Each result records
    timestamps, exit code, attempt count, and masked error; later ordinary steps skip
    after non-optional failure. Terminal transitions cannot restart execution.
13. **Local runner:** `os/exec`, direct argv or configured shell, separate streams,
    job directory, explicit environment, bounded pipe waiting, context cancellation.
14. **Shell/security:** Unix sh and Windows PowerShell defaults, explicit supported
    shell selection; scripts are intentional arbitrary code. Only trusted workflows.
15. **Docker:** Docker CLI create/start/exec/rm; one labeled Linux container per job,
    /workspace bind, explicit environment, capability/resource restrictions, cleanup.
16. **Isolation:** Docker is not a perfect sandbox; no privileged mode/socket mount.
    No remote/untrusted build-service security guarantee is made.
17. **Workspaces:** Bounded source snapshot and independent workspace copy per job;
    runtime data and .git excluded. checkout acknowledges prepared local source.
18. **Environment:** Step > job > workflow > defaults > inherited local environment.
    Docker receives defaults/explicit scopes only. Engine run/job/step/workspace/
    attempt variables are provided; no invented Git metadata.
19. **Secrets:** CICD_SECRET_NAME resolves env-only references. Source variables are
    removed from local inheritance. Streaming masking precedes JSONL/SSE; values
    are not included in run metadata. Transformed values/files are outside masking.
20. **Retries:** Failed step only; capped exponential backoff, up to ten attempts,
    cancellation-aware delay. Defaults: 100 ms, multiplier two, cap ten seconds.
21. **Timeouts:** Nested context deadlines; default workflow cap one hour. A step
    deadline covers retries/backoff and stops its process best-effort.
22. **Cancellation:** Running contexts propagate to processes/containers; undispatched
    jobs cancel; no cleanup dispatch after global cancellation. Cancel is idempotent.
23. **Failure:** Independent jobs continue; ineligible descendants iteratively SKIP.
24. **Conditions:** Literal success(), failure(), always(), cancelled(); no eval or
    arbitrary expressions. Conditions are evaluated after all needs are terminal.
25. **Matrix:** Not implemented; rejected unknown strategy fields, future work.
26. **Artifacts:** tar.gz files plus name/job/size/count/timestamp/SHA-256 metadata;
    checksum reread, bounded raw size/file count/artifact count, terminal retention.
27. **Path security:** Relative safe paths, rooted opens, rejected symlink prefixes/
    members and special files, ordinary archive permission bits, streamed downloads.
28. **Cache:** Not implemented; future work, no fake cache hit/miss behavior.
29. **Log persistence:** Sequenced UTC JSONL per run, bounded chunks and disk limit;
    separate stdout/stderr; disk errors surface to execution.
30. **Live logs:** Bounded 128-event subscriptions, overflow disconnect, SSE future
    events/heartbeats/write deadlines, CLI persisted-log following.
31. **Metadata:** Synced atomic schema-versioned JSON snapshots with OS ownership
    lock; Windows share-delete readers and bounded rename retries. No SQLite.
32. **Recovery:** Orphan nonterminal runs/jobs/steps become INTERRUPTED; no shell
    resume. Owner-labeled stale Docker cleanup when the daemon is available.
33. **REST:** Health/ready; create/list/get/cancel runs; jobs; logs/SSE; artifacts.
    Fixed source root, bounded bodies, consistent JSON errors, optional bearer auth,
    non-loopback auth requirement. [API reference](api.md).
34. **Metrics:** Run/job totals and active counts; used/total global slots, exposed
    in Prometheus text format. Counters reset on restart.
35. **Dashboard:** Not implemented; future work. No frontend or mocked UI data.
36. **Dependencies:** yaml.v3 for syntax decoding; otherwise standard library and
    optional installed Docker CLI. check.v1 appears in upstream-test checksums only.
37. **Vet:** Passed `go vet ./...`.
38. **Tests:** Passed `go test ./...`.
39. **Race:** Passed `go test -race ./...` with CGO and temporary portable GCC.
40. **Build:** Passed `go build -o cicd ./cmd/cicd` and Windows `.exe` build.
41. **Sequential:** Real CLI two-job workflow SUCCESS.
42. **Parallel:** Real CLI three roots overlap; all downstream jobs SUCCESS.
43. **Worker bound:** Controlled single-run and simultaneous-scheduler tests enforce
    capacity two; stress test checks its bound of sixteen.
44. **Dependency order:** Blocking scheduler tests plus real CLI timestamps confirm
    build waits for every prerequisite and package waits for build.
45. **Propagation:** Real CLI failed test, skipped build/package, successful independent
    job and failure-conditioned report; unit tests cover transitive skipping.
46. **Timeout:** Real CLI TIMED_OUT/skipped descendants; process helper tests cover
    workflow/job/step deadlines and actual process completion.
47. **Cancel:** Engine/API/CLI daemon checks yield CANCELLED; graceful engine-close
    tests cancel active work and prevent dependent execution.
48. **Retry:** Real CLI and subprocess integration reach exactly attempt three;
    backoff cap tests pass.
49. **Masking:** Real secret fixture absent from persisted logs; split-write,
    concurrent-write, long-output, and short-live-line tests pass.
50. **Checksum:** CLI/API downloads match persisted SHA-256; tar content verified.
51. **Traversal:** Invalid input rejected. Ordinary Windows symlink test skipped for
    missing privilege; Windows directory-junction direct/nested/snapshot escape
    tests passed. Ordinary symlink behavior still needs target-platform execution.
52. **Persistence:** Storage/engine restarts preserve successful states and metadata;
    real daemon restart preserves completed history.
53. **Crash:** Unit tests and killing/restarting a real daemon produce INTERRUPTED
    for orphaned active work.
54. **Docker verification:** Daemon unavailable. Docker-specific tests explicitly
    skip; cached Alpine image required if the daemon is present, keeping tests offline.
55. **Docker cleanup:** Implemented exact-ID finalization and owner-label recovery;
    execution verification unavailable without a daemon. No unrelated cleanup performed.
56. **Cache verification:** Not applicable, feature absent.
57. **Matrix verification:** Not applicable, feature absent.
58. **API verification:** httptest and real daemon checks pass health/readiness,
    submit/status/cancel/history/artifacts; tests cover body/auth/error handling.
59. **Live-log verification:** SSE delivery/disconnect tests and bounded subscriber
    concurrency tests pass. Persisted logs remain the replay source.
60. **Frontend verification:** Not applicable, feature absent.
61. **Measured benchmark:** 10,000 no-op jobs/32 workers: 31.2316 ms, 320,189 jobs/s;
    observed-completion P50 0 / P95 511.5 us on this Windows clock. Go benchmark:
    1,000-job operation 4,288,271 ns/op, 986,598 B/op, 11,139 allocs/op. These are
    scheduler-only measurements, excluding real processes and persistence.
62. **Build commands:** `go mod download`; `go build -o cicd ./cmd/cicd`;
    Windows: `go build -o cicd.exe ./cmd/cicd`.
63. **Run commands:** `./cicd validate examples/parallel.yaml`;
    `./cicd run examples/parallel.yaml --workers 3 --workspace .`;
    `./cicd runs list`; `./cicd logs RUN_ID`; Windows uses `./cicd.exe`.
64. **Example:** See [parallel workflow](../examples/parallel.yaml),
    [artifact workflow](../examples/artifacts.yaml), and the version-1 YAML sample
    in [README](../README.md). POSIX variants accompany platform-specific demos.
65. **Server:** `./cicd serve --addr 127.0.0.1:8080 --workspace . --workers 4`.
    `CICD_API_TOKEN` enables auth; `--artifact-retention 168h` sets retention.
66. **Suggested commits:** Schema/validation; DAG/states; scheduler/workers; runners;
    execution/reliability; secrets/logs; history/recovery; artifacts/security;
    REST/SSE/metrics; tests/stress; examples/docs/deployment recipe. No commits created.
67. **Limitations:** Trusted single-node model; no platform compatibility or
    exactly-once guarantee; limited expressions/secrets; filesystem rather than
    transactional metadata; no automatic workspace/log quotas or retention;
    Docker/Unix runtime paths unverified here; synchronous I/O can delay shutdown.
68. **Future work:** Cache, bounded matrix expansion, cross-job downloads, dashboard,
    remote workers, fairness, RBAC/OIDC, vault, S3 artifacts, telemetry, HA metadata.

Detailed evidence, test exclusions, coverage, and environment notes:
[verification record](verification.md). No unverified Docker or compatibility
claims are made.
