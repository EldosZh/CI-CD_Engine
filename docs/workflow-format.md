# Workflow format (version 1)

Exactly one YAML document is accepted. Unknown fields, duplicate mapping keys, and
unknown workflow versions are rejected. IDs match `[a-zA-Z0-9_-]+` with a 128-byte
limit. Workflow limits: 10,000 jobs and 1,000 steps per job. CLI parsing is capped
at 2 MiB; the API imposes a stricter 1 MiB request limit.

## Workflow fields

| Field | Type | Meaning |
| --- | --- | --- |
| `version` | integer | Required, exactly `1` |
| `name` | string | Required, nonblank workflow label |
| `on` | string list | Optional; only `manual` supported |
| `env` | string mapping | Workflow environment scope |
| `timeout` | duration | Positive Go duration; omitted means one hour |
| `jobs` | job mapping | Required jobs keyed by safe identifiers |

## Job fields

| Field | Type | Meaning |
| --- | --- | --- |
| `needs` | string list | Existing unique job IDs; all must finish before evaluation |
| `runs-on` | string | `local` (default) or `docker` |
| `container` | mapping | Docker only, requires `image` |
| `env` | string mapping | Overrides workflow scope |
| `timeout` | duration | Optional positive child deadline |
| `if` | condition string | Default `success()` |
| `steps` | step list | Required, sequential |

`container.image` must begin with an alphanumeric and contain only letters,
digits, `.`, `_`, `/`, `:`, `@`, and `-`; maximum 256 bytes. Docker resolves the
reference. There is no port, network, host mount, privilege, or resource override
in the workflow schema. A valid-looking missing image still fails execution.

## Step fields

| Field | Type | Meaning |
| --- | --- | --- |
| `name` | string | Display label; default `step-N` |
| `run` | string | Shell script, mutually exclusive with other actions |
| `command` | string list | Direct executable argv, no shell expansion |
| `shell` | string | For `run` only: sh/bash/cmd/powershell/pwsh |
| `uses` | string | Only built-in `checkout` |
| `artifact` | mapping | Archive upload, mutually exclusive with other actions |
| `env` | string mapping | Overrides job scope |
| `timeout` | duration | Covers the entire step, including retries/backoff |
| `retry` | mapping | Policy below |
| `if` | condition string | Default `success()` |
| `continue_on_error` | boolean | Preserve optional failed metadata but let job succeed |

Exactly one of `run`, `command`, `uses`, or `artifact` is required. Empty executable
argv[0] is rejected. `run` uses the explicitly selected shell or platform default
(`sh` on Unix; `powershell` on Windows). No shell is automatically installed.

## Retry fields

| Field | Type | Meaning |
| --- | --- | --- |
| `max_attempts` | integer | 1..10; omitted or zero means 1 |
| `initial_delay` | duration | Default `100ms`; positive, at most `1m` |
| `max_delay` | duration | Default `10s`; positive, at most `1m` |
| `multiplier` | number | 1..10; omitted or zero means 2 |

The computed delay is capped by max_delay, even when initial_delay is larger.
There is no jitter. Only failures retry; context cancellation/deadline stops retry.

## Artifacts

`artifact.name` is a unique safe identifier across the workflow.
`artifact.path` is one relative workspace file or directory, not a glob. POSIX `/`
is the portable separator. Backslashes, colons, absolute paths, traversal,
symlink path prefixes, symlink members, special files, and empty directories are
rejected. Ordinary permission bits are preserved. Upload failure follows normal
step failure semantics; `continue_on_error` can make it optional.

## Conditions

Only literal `success()`, `failure()`, `always()`, and `cancelled()` are supported.
No operators, general expressions, JavaScript, Go evaluation, matrix variables,
or compatibility expression language exists. For jobs, conditions inspect all
terminal direct dependencies. Success requires every dependency SUCCESS. Failure
means any direct dependency FAILED, TIMED_OUT, or INTERRUPTED; SKIPPED alone does
not count as failure. For steps, failure means an earlier non-optional failure.
Global cancellation prevents new work even with `always()` or `cancelled()`.

## Environments

Keys match `[a-zA-Z_][a-zA-Z0-9_]*`. Values are strings. Step > job > workflow >
engine defaults > inherited host environment for local execution. Docker only
receives defaults and explicit scopes. `CICD_SECRET_*` source values are never
inherited automatically. `${{ secrets.NAME }}` is expanded in environment values
only, from the engine process's `CICD_SECRET_NAME`. Missing secrets fail the step.
References are not expanded in commands, labels, images, paths, or retry values.

## Complete example

```yaml
version: 1
name: Sample
on: [manual]
timeout: 5m
env:
  MODE: ci
jobs:
  test:
    timeout: 1m
    steps:
      - uses: checkout
      - command: [go, test, ./...]
        timeout: 30s
        retry:
          max_attempts: 2
          initial_delay: 100ms
          max_delay: 1s
          multiplier: 2
  report:
    needs: [test]
    if: always()
    steps:
      - run: echo report
```

Cache, strategy/matrix, remote actions, webhooks, schedules, reusable workflows,
artifact downloads into jobs, job retries, and per-workflow worker limits are not
implemented. The worker limit belongs to the engine configuration or CLI.
