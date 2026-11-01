# Architecture

The executable is `cmd/cicd`. Runtime packages use only the standard library except
`yaml.v3` for decoding. The workflow format is independent of other CI products.

## Parsing and validation

`workflow.Parse` decodes one strict typed YAML document and validates every job and
step. YAML duplicate keys and unknown fields fail. Validation collects schema,
reference, timeout, environment, retry, runner, artifact, and condition errors.
DFS colors identify a concrete cycle before any workflow is submitted. Parsed
workflow objects never serve as run metadata: scripts, environment values, and
secrets are excluded from history. Submit owns a deep snapshot of caller inputs.

## Graph and execution

`dag.Graph` holds `Needs` (reverse edges) and `Children` (forward edges).
The scheduler owns mutable graph counters and job states. Fixed workers receive
job IDs and return results; jobs execute steps sequentially. A global semaphore
limits active jobs across runs. See [scheduler](scheduler.md) for the exact loop.

`engine` prepares a bounded source snapshot, copies it into one workspace per job,
builds child contexts, resolves environments, and invokes a runner session. Built-in
checkout is an acknowledgement of that prepared source; artifact steps archive the
current job workspace. A dependency supplies an ordering relationship, not a shared
filesystem. Retry affects only the current step and retains RUNNING until final
resolution. Every worker result is terminal before propagation.

## Runners

Local commands use argv directly, or a selected shell for `run`. Platform files
configure Unix process groups or Windows process tree termination. Docker sessions
create a labeled container, bind only the job workspace, and exec each step inside
it. Finalization removes the specific container ID, including on failure.
Owner-scoped restart cleanup cannot select containers from a different data root.
Unavailable Docker does not disable local workflows.

## Logs

Per-stream writers retain bounded raw tails for secret matching, then publish
bounded masked chunks. A mutex serializes per-run sequence assignment and JSONL
writes. Subscriber channels contain 128 events; overflow closes and removes the
subscriber. Disk log writes are synchronous and can exert disk backpressure;
network subscribers never do. Disk errors fail steps, and logging is capped around
64 MiB per run. SSE writes have deadlines and release subscriptions on disconnect.

## Artifacts and metadata

Artifacts use workspace-relative paths and rooted opens. Prefix symlinks, member
symlinks, and special files are rejected. Tar headers retain ordinary permissions,
not ownership or privileged bits. Raw bytes and member counts are limited. Archives
are written to a temporary file, closed, renamed, reread to verify SHA-256, and
published by atomic JSON metadata. Downloads use filesystem streaming.

Run snapshots use schema version 1, `fsync`, and rename-over. A short metadata mutex
protects readers/replacements within the process; Windows readers also permit
share-delete, with bounded rename retries for indexers. This is filesystem
persistence, not SQLite or a transactional multi-file database. A crash between
archive rename and metadata publication may leave an unpublished archive.

An OS lock grants one scheduler ownership of the data root. Startup marks orphan
nonterminal run/job/step states INTERRUPTED. Recovery does not resume processes.
Log/artifact files are separate. A once-per-minute daemon pass prunes only artifacts
of terminal runs past their configured retention. History/log/snapshot retention is
manual. See [security](security.md) for crash and filesystem limits.

## API and operations

The API uses `net/http`, a fixed source directory, 1 MiB submission limits, bounded
active runs, streaming downloads, optional bearer authentication, health/readiness,
and Prometheus text metrics. Request lifetime does not cancel accepted runs. Shutdown
stops submissions and cancels engine-owned contexts before draining HTTP connections.
API history returns the most recent 100 runs; local history lists all runs.

## Data layout

```text
data/
  engine.lock
  runs/ID/
    run.json
    logs.jsonl
    source/
    workspace/JOB/
  artifacts/ID/
    NAME.tar.gz
    NAME.json
```

The data root is trusted engine-owned state. A malicious local workflow shares the
engine's OS authority and can attack any accessible data; path helpers cannot
contain it. Do not share the data root with untrusted filesystem writers.
