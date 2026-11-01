# Security model

CI workflows execute arbitrary code. Run only trusted workflows, with trusted
source trees, on trusted machines. Anyone with API run-submission access can execute
code as the engine user. A bearer token is access control, not tenant isolation.

## Processes and Docker

The local runner has the engine process's full host authority. Workspace copies
prevent accidental source edits but cannot constrain malicious code. Direct argv
avoids engine-introduced shell interpolation; `run` deliberately invokes a shell.
The workflow author must quote values and handle shell-specific exit behavior.
The engine does not enforce network restrictions. Use an external OS/network
boundary if workflows require one.

Docker is not a perfect security sandbox. Containers share a kernel and may expose
runtime/kernel vulnerabilities. This runner has no privileged mode or Docker
socket mount, drops capabilities, sets no-new-privileges, and applies CPU/memory/PID
limits. The engine itself needs authority to use Docker; that is often host-root
level access. Images need `sh` and `sleep`, should be trusted, and should be pinned
to reviewed digests for reproducibility. Pulling/cache policy is Docker's behavior.
Only explicit workflow variables and engine defaults enter containers.

Unix process groups and Windows taskkill provide best-effort tree cleanup; escaped
or daemonized descendants can survive. A hard engine crash cannot clean arbitrary
local processes. Recovery marks metadata INTERRUPTED and does not resume commands.
Docker restart cleanup uses both engine and data-root owner labels. Reusing the
same data root gives that engine permission to remove its stale containers.
If Docker is unavailable at startup, stale cleanup must wait for a later restart.

## Secrets

Secrets come from `CICD_SECRET_NAME`, held in memory, and expanded only in explicit
environment values. Raw secret source variables are removed from local inheritance.
The host may have other credentials in its normal environment, accessible to local
trusted commands. Run the engine with a minimal, least-privileged environment.

Known values are masked before JSONL persistence and SSE publication, including
split writes in a single stream. Masking is best-effort. Encoded/transformed data,
stdout/stderr interleaving, files, artifacts, external network calls, process lists,
and Docker exec argument visibility are outside its protection. Scripts/env are
excluded from metadata, but user-chosen IDs, paths, and artifact names are ordinary
metadata: never put secrets there. Do not submit raw secrets as workflow literals.
Source snapshots and artifact contents may include sensitive files; review them.
Do not check `.env`, secret files, or runtime data into Git.

## Files and archives

Identifiers are constrained; safeJoin checks relative containment. Artifacts and
snapshots additionally use Go `os.Root` constrained opens so symlink swaps cannot
escape the workspace. Every artifact path prefix and traversed member rejects
symlinks and special files. Archives use relative names and ordinary permissions,
without ownership/setuid data. The implementation does not extract archives, so
there is no archive extraction surface. Hard links created by a trusted command
are ordinary workspace files; local commands can already read the host.

The data directory is trusted engine-owned storage. Do not let other users replace
its directories, symlinks, or metadata. Protect data backups as sensitive material.
One OS lock prevents multiple scheduler owners of a data directory; it does not
prevent a privileged attacker or local workflow from changing files.

## Resource exhaustion and service exposure

Workers: 1..256; active workflows: 1..128 (defaults four/eight). Workflow input is
limited to 2 MiB locally or 1 MiB through HTTP. A workflow may contain at most
10,000 jobs and 1,000 steps/job. A snapshot is at most 1 GiB/100,000 files per copy.
Per-run logs stop persistence around 64 MiB; output is still drained. Log chunks
are about 16 KiB. Each subscriber has 128 buffered events; overflow disconnects it.
HTTP log reads return at most 8 MiB. SSE writes have ten-second deadlines.
Artifacts are limited to 100/run, 100 MiB raw/artifact, and 10,000 files/artifact.

These per-item limits are not a total disk quota. Many runs, job snapshot copies,
command outputs/files, cached Docker images, and history can fill disks. Run under
OS/container quotas, monitor storage, and prune snapshots/logs offline. Artifact
retention only runs in a live daemon, checks once per minute, and excludes active
runs. Synchronous filesystem operations may delay cancellation/shutdown.

The API defaults to loopback. Non-loopback bind requires `CICD_API_TOKEN`; deploy
behind a TLS/authenticated access boundary. There is no TLS listener, RBAC,
multi-tenancy, CSRF browser integration, submission rate limit, or secure untrusted
build service. HTTP request/header/time and active-run limits provide bounded
handling, but authentication and a trusted network remain necessary.

## Persistence guarantees

Metadata is a synced temporary JSON file renamed into place. Windows replacement
is synchronized with in-process readers and retried briefly for external readers.
This is not a transactional database across files, and directory fsync/power-loss
resilience is not guaranteed. Upload crashes may leave unpublished archives.
Do not claim exactly-once effects: retries and operator reruns can duplicate
external actions. Logs and snapshots are not encrypted at rest.
