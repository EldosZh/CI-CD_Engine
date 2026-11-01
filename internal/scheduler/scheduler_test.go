package scheduler

import (
	"cicd-engine/internal/workflow"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func jobs(ids ...string) *workflow.Workflow {
	w := &workflow.Workflow{Version: 1, Name: "test", Jobs: map[string]workflow.Job{}}
	for _, id := range ids {
		w.Jobs[id] = workflow.Job{Steps: []workflow.Step{{Run: "noop"}}}
	}
	return w
}
func needs(w *workflow.Workflow, id string, ns ...string) {
	j := w.Jobs[id]
	j.Needs = ns
	w.Jobs[id] = j
}
func TestParallelBoundAndOrdering(t *testing.T) {
	w := jobs("a", "b", "c", "d")
	needs(w, "d", "a", "b", "c")
	started := make(chan string, 4)
	release := make(chan struct{})
	finished := make(chan map[string]JobResult, 1)
	var active, maxSeen atomic.Int32
	go func() {
		rs, err := Schedule(context.Background(), w, 2, make(chan struct{}, 2), func(ctx context.Context, id string, j workflow.Job) JobResult {
			n := active.Add(1)
			for old := maxSeen.Load(); n > old; old = maxSeen.Load() {
				if maxSeen.CompareAndSwap(old, n) {
					break
				}
			}
			started <- id
			if id != "d" {
				<-release
			}
			active.Add(-1)
			return JobResult{ID: id, State: Success}
		}, func(JobResult) {})
		if err != nil {
			t.Error(err)
		}
		finished <- rs
	}()
	a, b := <-started, <-started
	if a == "d" || b == "d" || a == b {
		t.Fatal("dependency ran early")
	}
	select {
	case id := <-started:
		t.Fatalf("third job exceeded worker limit: %s", id)
	case <-time.After(20 * time.Millisecond):
	}
	release <- struct{}{}
	c := <-started
	if c == "d" {
		t.Fatal("downstream ran before third root")
	}
	release <- struct{}{}
	release <- struct{}{}
	if id := <-started; id != "d" {
		t.Fatal(id)
	}
	rs := <-finished
	if maxSeen.Load() != 2 || rs["d"].State != Success {
		t.Fatalf("max=%d results=%v", maxSeen.Load(), rs)
	}
}
func TestFailurePropagation(t *testing.T) {
	w := jobs("a", "b", "c", "independent", "cleanup")
	needs(w, "b", "a")
	needs(w, "c", "b")
	needs(w, "cleanup", "c")
	j := w.Jobs["cleanup"]
	j.If = "always()"
	w.Jobs["cleanup"] = j
	rs, e := Schedule(context.Background(), w, 2, make(chan struct{}, 2), func(ctx context.Context, id string, j workflow.Job) JobResult {
		s := Success
		if id == "a" {
			s = Failed
		}
		return JobResult{ID: id, State: s}
	}, func(JobResult) {})
	if e != nil {
		t.Fatal(e)
	}
	for id, s := range map[string]State{"a": Failed, "b": Skipped, "c": Skipped, "independent": Success, "cleanup": Success} {
		if rs[id].State != s {
			t.Fatalf("%s: %s", id, rs[id].State)
		}
	}
}
func TestCancelAndTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			w := jobs("a", "b")
			needs(w, "b", "a")
			ctx, cancel := context.WithCancel(context.Background())
			if timeout {
				ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
			}
			defer cancel()
			started := make(chan struct{})
			if !timeout {
				go func() { <-started; cancel() }()
			}
			rs, e := Schedule(ctx, w, 2, make(chan struct{}, 2), func(ctx context.Context, id string, j workflow.Job) JobResult {
				close(started)
				<-ctx.Done()
				return JobResult{ID: id, State: ContextState(ctx)}
			}, func(JobResult) {})
			if e != nil {
				t.Fatal(e)
			}
			if rs["a"].State != ContextState(ctx) || rs["b"].State != ContextState(ctx) {
				t.Fatal(rs)
			}
		})
	}
}
func TestTerminalTransitions(t *testing.T) {
	for _, s := range []State{Success, Failed, Cancelled, Skipped, TimedOut, Interrupted} {
		if Transition(s, Running) == nil {
			t.Fatal(s)
		}
	}
}
func TestStress(t *testing.T) {
	w := jobs()
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("j%04d", i)
		j := workflow.Job{Steps: []workflow.Step{{Run: "noop"}}}
		if i > 0 {
			j.Needs = []string{fmt.Sprintf("j%04d", rng.Intn(i))}
		}
		w.Jobs[id] = j
	}
	var mu sync.Mutex
	complete := map[string]bool{}
	var active, max atomic.Int32
	rs, e := Schedule(context.Background(), w, 16, make(chan struct{}, 16), func(ctx context.Context, id string, j workflow.Job) JobResult {
		n := active.Add(1)
		for p := max.Load(); n > p; p = max.Load() {
			if max.CompareAndSwap(p, n) {
				break
			}
		}
		mu.Lock()
		for _, d := range j.Needs {
			if !complete[d] {
				t.Errorf("%s before %s", id, d)
			}
		}
		if complete[id] {
			t.Error("duplicate")
		}
		complete[id] = true
		mu.Unlock()
		active.Add(-1)
		return JobResult{ID: id, State: Success}
	}, func(JobResult) {})
	if e != nil || len(rs) != 1000 || len(complete) != 1000 || max.Load() > 16 {
		t.Fatalf("results=%d err=%v max=%d", len(rs), e, max.Load())
	}
}
func BenchmarkScheduler(b *testing.B) {
	w := jobs()
	for i := 0; i < 1000; i++ {
		w.Jobs[fmt.Sprint(i)] = workflow.Job{Steps: []workflow.Step{{Run: "noop"}}}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, e := Schedule(context.Background(), w, 16, make(chan struct{}, 16), func(ctx context.Context, id string, j workflow.Job) JobResult {
			return JobResult{ID: id, State: Success}
		}, func(JobResult) {})
		if e != nil {
			b.Fatal(e)
		}
	}
}
func TestGlobalCapacityAcrossRuns(t *testing.T) {
	slots := make(chan struct{}, 2)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var wg sync.WaitGroup
	var active, max atomic.Int32
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := Schedule(context.Background(), jobs("a", "b"), 2, slots, func(ctx context.Context, id string, j workflow.Job) JobResult {
				n := active.Add(1)
				for p := max.Load(); n > p; p = max.Load() {
					if max.CompareAndSwap(p, n) {
						break
					}
				}
				started <- struct{}{}
				<-release
				active.Add(-1)
				return JobResult{ID: id, State: Success}
			}, func(JobResult) {})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	<-started
	<-started
	select {
	case <-started:
		t.Fatal("global capacity exceeded")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if max.Load() != 2 {
		t.Fatal(max.Load())
	}
}
