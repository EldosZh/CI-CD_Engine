package scheduler

import (
	"cicd-engine/internal/dag"
	"cicd-engine/internal/workflow"
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

type State string

const (
	Pending     State = "PENDING"
	Ready       State = "READY"
	Running     State = "RUNNING"
	Success     State = "SUCCESS"
	Failed      State = "FAILED"
	Cancelled   State = "CANCELLED"
	Skipped     State = "SKIPPED"
	TimedOut    State = "TIMED_OUT"
	Interrupted State = "INTERRUPTED"
)

func (s State) Terminal() bool {
	return s == Success || s == Failed || s == Cancelled || s == Skipped || s == TimedOut || s == Interrupted
}
func Transition(from, to State) error {
	valid := false
	switch from {
	case Pending:
		valid = to == Ready || to == Skipped || to == Cancelled || to == TimedOut || to == Interrupted
	case Ready:
		valid = to == Running || to == Cancelled || to == TimedOut || to == Interrupted
	case Running:
		valid = to == Success || to == Failed || to == Cancelled || to == TimedOut || to == Interrupted
	}
	if !valid {
		return fmt.Errorf("invalid state transition %s -> %s", from, to)
	}
	return nil
}

type StepResult struct {
	Name       string    `json:"name"`
	State      State     `json:"state"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	ExitCode   int       `json:"exit_code"`
	Attempt    int       `json:"attempt"`
	Error      string    `json:"error,omitempty"`
}
type JobResult struct {
	ID         string       `json:"id"`
	Needs      []string     `json:"needs"`
	State      State        `json:"state"`
	StartedAt  time.Time    `json:"started_at,omitempty"`
	FinishedAt time.Time    `json:"finished_at,omitempty"`
	Steps      []StepResult `json:"steps"`
	Error      string       `json:"error,omitempty"`
}
type Run struct {
	SchemaVersion int                  `json:"schema_version"`
	ID            string               `json:"id"`
	Name          string               `json:"name"`
	State         State                `json:"state"`
	CreatedAt     time.Time            `json:"created_at"`
	StartedAt     time.Time            `json:"started_at"`
	FinishedAt    time.Time            `json:"finished_at,omitempty"`
	Jobs          map[string]JobResult `json:"jobs"`
	Error         string               `json:"error,omitempty"`
}
type Execute func(context.Context, string, workflow.Job) JobResult

// Schedule owns all scheduler state. Workers only return immutable completion messages.
// slots is shared across runs, bounding total process-wide job concurrency.
func Schedule(ctx context.Context, w *workflow.Workflow, workers int, slots chan struct{}, exec Execute, update func(JobResult)) (map[string]JobResult, error) {
	g, err := dag.Build(w)
	if err != nil {
		return nil, err
	}
	if workers < 1 || workers > 256 {
		return nil, fmt.Errorf("workers must be 1..256")
	}
	results := map[string]JobResult{}
	remaining := map[string]int{}
	queue := &dag.Queue{}
	heap.Init(queue)
	set := func(id string, to State) {
		r := results[id]
		if err := Transition(r.State, to); err != nil {
			panic(err)
		}
		r.State = to
		if to.Terminal() {
			r.FinishedAt = time.Now().UTC()
		}
		results[id] = r
		update(r)
	}
	for _, id := range w.IDs() {
		results[id] = JobResult{ID: id, Needs: w.Jobs[id].Needs, State: Pending}
		remaining[id] = len(g.Needs[id])
		if remaining[id] == 0 {
			heap.Push(queue, id)
		}
	}
	tasks := make(chan string)
	done := make(chan JobResult, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range tasks {
				var r JobResult
				if ctx.Err() != nil {
					r = JobResult{ID: id, State: ContextState(ctx)}
				} else {
					r = exec(ctx, id, w.Jobs[id])
				}
				<-slots
				r.ID = id
				r.Needs = w.Jobs[id].Needs
				done <- r
			}
		}()
	}
	defer func() { close(tasks); wg.Wait() }()
	active, finished := 0, 0
	// Terminal propagation uses a queue, so skipped chains cannot stall or overflow recursion.
	var propagate func(string)
	propagate = func(id string) {
		todo := []string{id}
		for len(todo) > 0 {
			v := todo[0]
			todo = todo[1:]
			for _, c := range g.Children[v] {
				remaining[c]--
				if remaining[c] != 0 {
					continue
				}
				ok, failed, cancel := true, false, false
				for _, n := range g.Needs[c] {
					s := results[n].State
					ok = ok && s == Success
					failed = failed || s == Failed || s == TimedOut || s == Interrupted
					cancel = cancel || s == Cancelled
				}
				if workflow.Condition(w.Jobs[c].If, ok, failed, cancel) {
					heap.Push(queue, c)
				} else {
					set(c, Skipped)
					finished++
					todo = append(todo, c)
				}
			}
		}
	}
	for finished < len(w.Jobs) {
		if ctx.Err() != nil {
			for _, id := range w.IDs() {
				s := results[id].State
				if s == Pending || s == Ready {
					set(id, ContextState(ctx))
					finished++
				}
			}
			*queue = nil
		}
		for ctx.Err() == nil && active < workers && queue.Len() > 0 {
			id := heap.Pop(queue).(string)
			if len(g.Needs[id]) == 0 && !workflow.Condition(w.Jobs[id].If, true, false, false) {
				set(id, Skipped)
				finished++
				propagate(id)
				continue
			}
			set(id, Ready)
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				set(id, ContextState(ctx))
				finished++
				continue
			}
			set(id, Running)
			r := results[id]
			r.StartedAt = time.Now().UTC()
			results[id] = r
			update(r)
			tasks <- id
			active++
		}
		if active == 0 {
			if finished == len(w.Jobs) {
				break
			}
			if queue.Len() == 0 {
				return results, fmt.Errorf("scheduler stalled")
			}
			continue
		}
		r := <-done
		active--
		old := results[r.ID]
		if !r.State.Terminal() {
			r.State = Failed
			r.Error = "executor returned nonterminal state"
		}
		if err := Transition(old.State, r.State); err != nil {
			return results, err
		}
		if r.StartedAt.IsZero() {
			r.StartedAt = old.StartedAt
		}
		r.FinishedAt = time.Now().UTC()
		results[r.ID] = r
		update(r)
		finished++
		if ctx.Err() == nil {
			propagate(r.ID)
		}
	}
	return results, nil
}
func ContextState(ctx context.Context) State {
	if ctx.Err() == context.DeadlineExceeded {
		return TimedOut
	}
	return Cancelled
}
