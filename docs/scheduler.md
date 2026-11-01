# Scheduler algorithm

The coordinator owns the results map, remaining-dependency counters, lexical
ready heap, and active/finished counts. Workers receive job IDs on an unbuffered
channel and send results on a completion channel sized to the worker count.
There are exactly `workers` worker goroutines per run, with at most `max_runs`
active runs. An engine-wide semaphore limits total running execution to `workers`.

1. Validate the complete workflow and construct `Needs` and `Children` maps.
2. Set all jobs PENDING; enqueue roots in a lexical min-heap.
3. For each eligible job while per-run worker capacity is available, transition
   PENDING -> READY, acquire global capacity, transition READY -> RUNNING, persist,
   and send its ID to a worker.
4. A worker executes the job without coordinator locks, releases global capacity,
   and returns a terminal result. The coordinator records it and propagates it.
5. For each child, decrement its remaining dependency count. Only at zero evaluate
   its condition using all direct dependency outcomes. Enqueue eligible children;
   mark others SKIPPED and propagate those outcomes through an iterative queue.
6. On cancellation, mark undispatched PENDING/READY jobs CANCELLED or TIMED_OUT,
   stop dispatching, and drain active completion messages before joining workers.
7. Finish once every node is terminal.

```text
while terminal_count < node_count:
    if context ended:
        terminate every undispatched node
        empty ready heap
    while ready and local capacity and context active:
        id = lexical_min(ready)
        evaluate root condition
        acquire shared engine slot or cancel
        READY -> RUNNING
        dispatch id
    if active:
        result = receive_completion()
        RUNNING -> result.terminal_state
        propagate counts, eligibility, and skipped descendants
    else if no ready nodes and unfinished nodes:
        report an internal scheduler stall
```

The global slot is acquired before the RUNNING transition. Jobs can therefore
wait in READY for another run's capacity; the configured bound applies to actual
executors across all runs. A released slot may be reused before the previous
completion snapshot is observed. Persisted timestamps are not an exact concurrency
trace. Metrics derive slot usage from the actual semaphore.

DFS colors detect cycles: white/unvisited -> gray/in recursion -> black/finished.
A gray edge reports the active stack segment as a cycle. Graph validation is
O(V+E), excluding sorting/validation fields. Lexical heap operations add O(V log V).
Failure propagation is iterative O(V+E). Workers never mutate scheduler maps.

Ready choice is deterministic among currently eligible nodes; concurrent completion
order is not deterministic. FIFO/fair scheduling across separate runs is not
guaranteed. Independent jobs continue after failures. `always()` still waits for
all dependencies to terminate. Global cancellation prevents cleanup dispatch.

The engine serializes only short metadata updates, not subprocess execution or
network streaming. Artifacts use a separate engine synchronization section to
serialize publication/count checks without holding the control mutex.
Local filesystem latency can delay completion. Scheduler benchmarks
use synthetic no-op execution and exclude metadata writes, snapshots, and processes.

