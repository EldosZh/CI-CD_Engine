package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cicd-engine/internal/logs"
	"cicd-engine/internal/runner"
	"cicd-engine/internal/scheduler"
	"cicd-engine/internal/storage"
	"cicd-engine/internal/workflow"
)

type Config struct {
	Workers           int
	MaxRuns           int
	ArtifactRetention time.Duration
	MaxArtifactBytes  int64
	MaxArtifacts      int
}
type activeRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	sink   *logs.Sink
}
type Engine struct {
	Store             *storage.Store
	Config            Config
	mu                sync.Mutex
	artifactMu        sync.Mutex
	active            map[string]*activeRun
	closing           bool
	slots             chan struct{}
	unlock            func()
	owner             string
	wg                sync.WaitGroup
	maintenanceCancel context.CancelFunc
	runsTotal         atomic.Int64
	jobsTotal         atomic.Int64
	jobsActive        atomic.Int64
}

func Open(dir string, c Config) (*Engine, error) {
	if c.Workers == 0 {
		c.Workers = 4
	}
	if c.MaxRuns == 0 {
		c.MaxRuns = 8
	}
	if c.ArtifactRetention == 0 {
		c.ArtifactRetention = 7 * 24 * time.Hour
	}
	if c.MaxArtifactBytes == 0 {
		c.MaxArtifactBytes = 100 << 20
	}
	if c.MaxArtifacts == 0 {
		c.MaxArtifacts = 100
	}
	if c.Workers < 1 || c.Workers > 256 || c.MaxRuns < 1 || c.MaxRuns > 128 || c.ArtifactRetention < time.Second || c.MaxArtifactBytes < 1 || c.MaxArtifacts < 1 {
		return nil, fmt.Errorf("invalid engine limits")
	}
	s, err := storage.New(dir)
	if err != nil {
		return nil, err
	}
	unlock, err := s.Lock()
	if err != nil {
		return nil, fmt.Errorf("data directory already owned by another engine: %w", err)
	}
	if err = s.Recover(); err != nil {
		unlock()
		return nil, err
	}
	h := sha256.Sum256([]byte(s.Dir))
	e := &Engine{Store: s, Config: c, active: map[string]*activeRun{}, slots: make(chan struct{}, c.Workers), unlock: unlock, owner: hex.EncodeToString(h[:16])}
	if runner.DockerAvailable() == nil {
		if err = runner.CleanupDocker(e.owner); err != nil {
			unlock()
			return nil, fmt.Errorf("orphan Docker cleanup: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.maintenanceCancel = cancel
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := s.Prune(c.ArtifactRetention)
				if err != nil {
					slog.Error("artifact retention failed", "error", err)
				}
			}
		}
	}()
	return e, nil
}
func SecretsFromEnv() map[string]string {
	m := map[string]string{}
	for _, v := range os.Environ() {
		k, val, ok := strings.Cut(v, "=")
		if ok && strings.HasPrefix(k, "CICD_SECRET_") && val != "" {
			m[strings.TrimPrefix(k, "CICD_SECRET_")] = val
		}
	}
	return m
}
func SecretValues(m map[string]string) []string {
	v := []string{}
	for _, s := range m {
		v = append(v, s)
	}
	sort.Slice(v, func(i, j int) bool { return len(v[i]) > len(v[j]) })
	return v
}

var secretRef = regexp.MustCompile(`\$\{\{\s*secrets\.([a-zA-Z0-9_]+)\s*\}\}`)

func Environment(defaults map[string]string, scopes []map[string]string, secrets map[string]string) ([]string, error) {
	env := map[string]string{}
	for _, v := range os.Environ() {
		k, val, ok := strings.Cut(v, "=")
		if ok && !strings.HasPrefix(k, "CICD_SECRET_") {
			env[k] = val
		}
	}
	for k, v := range defaults {
		env[k] = v
	}
	for _, scope := range scopes {
		for k, v := range scope {
			var missing string
			v = secretRef.ReplaceAllStringFunc(v, func(ref string) string {
				key := secretRef.FindStringSubmatch(ref)[1]
				val, ok := secrets[key]
				if !ok {
					missing = key
				}
				return val
			})
			if missing != "" {
				return nil, fmt.Errorf("missing secret %s", missing)
			}
			env[k] = v
		}
	}
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []string{}
	for _, k := range keys {
		out = append(out, k+"="+env[k])
	}
	return out, nil
}
func child(ctx context.Context, d string) (context.Context, context.CancelFunc) {
	if d != "" {
		return context.WithTimeout(ctx, workflow.Duration(d))
	}
	return context.WithCancel(ctx)
}
func (e *Engine) Submit(parent context.Context, w *workflow.Workflow, source string, secrets map[string]string) (string, error) {
	if err := w.Validate(); err != nil {
		return "", err
	}
	if len(SecretValues(secrets)) > 128 {
		return "", fmt.Errorf("too many secrets")
	}
	for _, v := range secrets {
		if len(v) > 4096 {
			return "", fmt.Errorf("secret too long")
		}
	}
	// Own an immutable workflow and secret snapshot, independent of caller mutations.
	own := *w
	own.Jobs = make(map[string]workflow.Job, len(w.Jobs))
	own.Env = cloneEnv(w.Env)
	for id, j := range w.Jobs {
		j.Env = cloneEnv(j.Env)
		j.Needs = append([]string{}, j.Needs...)
		j.Steps = append([]workflow.Step{}, j.Steps...)
		for i := range j.Steps {
			j.Steps[i].Env = cloneEnv(j.Steps[i].Env)
			j.Steps[i].Command = append([]string{}, j.Steps[i].Command...)
			if j.Steps[i].Artifact != nil {
				a := *j.Steps[i].Artifact
				j.Steps[i].Artifact = &a
			}
		}
		if j.Container != nil {
			c := *j.Container
			j.Container = &c
		}
		own.Jobs[id] = j
	}
	w = &own
	secrets = cloneEnv(secrets)
	e.mu.Lock()
	if e.closing || len(e.active) >= e.Config.MaxRuns {
		e.mu.Unlock()
		return "", fmt.Errorf("engine unavailable or concurrent run limit reached")
	}
	id := storage.ID()
	dir, _ := e.Store.RunDir(id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		e.mu.Unlock()
		return "", err
	}
	ctx, cancel := child(parent, w.Timeout)
	if w.Timeout == "" {
		cancel()
		ctx, cancel = context.WithTimeout(parent, time.Hour)
	}
	sink, err := logs.Open(filepath.Join(dir, "logs.jsonl"), id, SecretValues(secrets))
	if err != nil {
		cancel()
		e.mu.Unlock()
		return "", err
	}
	active := &activeRun{cancel: cancel, done: make(chan struct{}), sink: sink}
	e.active[id] = active
	e.wg.Add(1)
	e.mu.Unlock()
	r := &scheduler.Run{SchemaVersion: 1, ID: id, Name: logs.Mask(w.Name, SecretValues(secrets)), State: scheduler.Running, CreatedAt: time.Now().UTC(), StartedAt: time.Now().UTC(), Jobs: map[string]scheduler.JobResult{}}
	for _, j := range w.IDs() {
		r.Jobs[j] = scheduler.JobResult{ID: j, Needs: w.Jobs[j].Needs, State: scheduler.Pending}
	}
	if err = e.Store.Save(r); err != nil {
		cancel()
		sink.Close()
		e.mu.Lock()
		delete(e.active, id)
		close(active.done)
		e.mu.Unlock()
		e.wg.Done()
		return "", err
	}
	e.runsTotal.Add(1)
	go func() {
		defer e.wg.Done()
		defer cancel()
		defer func() { e.mu.Lock(); delete(e.active, id); close(active.done); e.mu.Unlock() }()
		var stateMu sync.Mutex
		var persistErr error
		update := func(j scheduler.JobResult) {
			stateMu.Lock()
			defer stateMu.Unlock()
			j.Steps = append([]scheduler.StepResult{}, j.Steps...)
			r.Jobs[j.ID] = j
			if err := e.Store.Save(r); err != nil {
				persistErr = err
				cancel()
			}
		}
		snapshot := filepath.Join(dir, "source")
		prepErr := storage.CopySnapshot(source, snapshot, e.Store.Dir)
		if prepErr == nil && ctx.Err() == nil {
			_, err = scheduler.Schedule(ctx, w, e.Config.Workers, e.slots, func(ctx context.Context, id string, j workflow.Job) scheduler.JobResult {
				e.jobsTotal.Add(1)
				e.jobsActive.Add(1)
				defer e.jobsActive.Add(-1)
				return e.executeJob(ctx, r.ID, id, j, w, snapshot, secrets, sink, update)
			}, update)
		} else {
			err = prepErr
			if ctx.Err() != nil {
				err = ctx.Err()
			}
		}
		r.State = scheduler.Success
		for id, j := range r.Jobs {
			if !j.State.Terminal() {
				j.State = scheduler.Interrupted
				j.FinishedAt = time.Now().UTC()
				r.Jobs[id] = j
			}
			if j.State == scheduler.Failed || j.State == scheduler.Interrupted {
				r.State = scheduler.Failed
			}
			if j.State == scheduler.TimedOut && r.State == scheduler.Success {
				r.State = scheduler.TimedOut
			}
		}
		if err != nil {
			r.State = scheduler.Failed
			r.Error = logs.Mask(err.Error(), SecretValues(secrets))
		}
		if ctx.Err() != nil {
			r.State = scheduler.ContextState(ctx)
			for id, j := range r.Jobs {
				if j.State == scheduler.Interrupted {
					j.State = r.State
					r.Jobs[id] = j
				}
			}
		}
		if closeErr := sink.Close(); closeErr != nil {
			r.State = scheduler.Failed
			r.Error = "log persistence failed"
		}
		if persistErr != nil {
			r.State = scheduler.Failed
			r.Error = "run persistence failed"
		}
		r.FinishedAt = time.Now().UTC()
		if saveErr := e.Store.Save(r); saveErr != nil {
			slog.Error("run final persistence failed", "run_id", r.ID, "error", saveErr)
		}
		slog.Info("workflow finished", "run_id", id, "status", r.State, "duration", r.FinishedAt.Sub(r.StartedAt))
	}()
	return id, nil
}
func cloneEnv(m map[string]string) map[string]string {
	n := map[string]string{}
	for k, v := range m {
		n[k] = v
	}
	return n
}
func (e *Engine) executeJob(parent context.Context, run, id string, j workflow.Job, w *workflow.Workflow, source string, secrets map[string]string, sink *logs.Sink, update func(scheduler.JobResult)) (res scheduler.JobResult) {
	ctx, cancel := child(parent, j.Timeout)
	defer cancel()
	res = scheduler.JobResult{ID: id, Needs: j.Needs, State: scheduler.Running, StartedAt: time.Now().UTC()}
	for i, s := range j.Steps {
		name := s.Name
		if name == "" {
			name = fmt.Sprintf("step-%d", i+1)
		}
		res.Steps = append(res.Steps, scheduler.StepResult{Name: logs.Mask(name, SecretValues(secrets)), State: scheduler.Pending, ExitCode: -1})
	}
	workspace := filepath.Join(filepath.Dir(source), "workspace", id)
	defer func() {
		if ctx.Err() != nil {
			res.State = scheduler.ContextState(ctx)
		}
		if !res.State.Terminal() {
			res.State = scheduler.Failed
		}
		res.Error = logs.Mask(res.Error, SecretValues(secrets))
		for i := range res.Steps {
			if !res.Steps[i].State.Terminal() {
				if ctx.Err() != nil {
					res.Steps[i].State = scheduler.ContextState(ctx)
				} else {
					res.Steps[i].State = scheduler.Skipped
				}
				res.Steps[i].FinishedAt = time.Now().UTC()
			}
		}
		res.FinishedAt = time.Now().UTC()
	}()
	if err := storage.CopySnapshot(source, workspace, e.Store.Dir); err != nil {
		res.Error = err.Error()
		return
	}
	var session runner.Session = &runner.Local{}
	if j.RunsOn == "docker" {
		d, err := runner.NewDocker(ctx, workspace, j.Container.Image, run, id, e.owner)
		if err != nil {
			res.Error = err.Error()
			return
		}
		session = d
	}
	defer func() {
		if err := session.Close(); err != nil {
			res.State = scheduler.Failed
			res.Error = "runner cleanup failed"
		}
	}()
	failed := false
	for i, s := range j.Steps {
		if ctx.Err() != nil {
			break
		}
		stepID := fmt.Sprintf("%d", i+1)
		sr := &res.Steps[i]
		if !workflow.Condition(s.If, !failed, failed, false) {
			sr.State = scheduler.Skipped
			sr.FinishedAt = time.Now().UTC()
			update(res)
			continue
		}
		_ = scheduler.Transition(sr.State, scheduler.Ready)
		sr.State = scheduler.Ready
		_ = scheduler.Transition(sr.State, scheduler.Running)
		sr.State = scheduler.Running
		sr.StartedAt = time.Now().UTC()
		update(res)
		sc, stop := child(ctx, s.Timeout)
		max := s.Retry.MaxAttempts
		if max == 0 {
			max = 1
		}
		var stepErr error
		for attempt := 1; attempt <= max; attempt++ {
			sr.Attempt = attempt
			defaults := map[string]string{"CICD_RUN_ID": run, "CICD_JOB_ID": id, "CICD_STEP_ID": stepID, "CICD_WORKSPACE": workspace, "CICD_ATTEMPT": fmt.Sprint(attempt)}
			if j.RunsOn == "docker" {
				defaults["CICD_WORKSPACE"] = "/workspace"
			}
			env, err := Environment(defaults, []map[string]string{w.Env, j.Env, s.Env}, secrets)
			if j.RunsOn == "docker" {
				allowed := cloneEnv(defaults)
				for _, scope := range []map[string]string{w.Env, j.Env, s.Env} {
					for k := range scope {
						allowed[k] = ""
					}
				}
				filtered := []string{}
				for _, v := range env {
					k, _, _ := strings.Cut(v, "=")
					if _, ok := allowed[k]; ok {
						filtered = append(filtered, v)
					}
				}
				env = filtered
			}
			if err == nil && sc.Err() != nil {
				err = sc.Err()
			}
			if err == nil {
				out := sink.Writer(id, stepID, "stdout")
				errout := sink.Writer(id, stepID, "stderr")
				switch {
				case s.Uses == "checkout":
					sink.Note(id, stepID, "source snapshot already prepared")
					sr.ExitCode = 0
				case s.Artifact != nil:
					e.artifactMu.Lock()
					as, ae := e.Store.Artifacts(run)
					if ae == nil && len(as) >= e.Config.MaxArtifacts {
						ae = fmt.Errorf("artifact count limit reached")
					}
					if ae == nil {
						_, ae = e.Store.Upload(run, id, workspace, *s.Artifact, e.Config.MaxArtifactBytes)
					}
					e.artifactMu.Unlock()
					err = ae
					if err == nil {
						sr.ExitCode = 0
					}
				default:
					sr.ExitCode, err = session.Execute(sc, runner.Request{Run: s.Run, Command: s.Command, Shell: s.Shell, Dir: workspace, Env: env, Stdout: out, Stderr: errout})
				}
				out.Close()
				errout.Close()
			}
			stepErr = err
			if err == nil || sc.Err() != nil || attempt == max {
				break
			}
			delay := Backoff(s.Retry, attempt)
			sink.Note(id, stepID, fmt.Sprintf("retry after %s", delay))
			timer := time.NewTimer(delay)
			select {
			case <-sc.Done():
				timer.Stop()
			case <-timer.C:
			}
			if sc.Err() != nil {
				stepErr = sc.Err()
				break
			}
		}
		sr.State = scheduler.Success
		if stepErr != nil {
			sr.State = scheduler.Failed
			sr.Error = logs.Mask(stepErr.Error(), SecretValues(secrets))
		}
		if sc.Err() != nil {
			sr.State = scheduler.ContextState(sc)
		}
		stop()
		sr.FinishedAt = time.Now().UTC()
		if sink.Error() != nil {
			sr.State = scheduler.Failed
			sr.Error = "log persistence failed"
		}
		if sr.State != scheduler.Success && !s.ContinueOnError {
			failed = true
			if sr.State == scheduler.TimedOut {
				res.State = scheduler.TimedOut
			}
		}
		update(res)
	}
	if !res.State.Terminal() {
		if failed {
			res.State = scheduler.Failed
		} else {
			res.State = scheduler.Success
		}
	}
	return
}
func Backoff(p workflow.RetryPolicy, attempt int) time.Duration {
	initial := workflow.Duration(p.InitialDelay)
	if initial == 0 {
		initial = 100 * time.Millisecond
	}
	capDelay := workflow.Duration(p.MaxDelay)
	if capDelay == 0 {
		capDelay = 10 * time.Second
	}
	mult := p.Multiplier
	if mult == 0 {
		mult = 2
	}
	d := float64(initial) * math.Pow(mult, float64(attempt-1))
	if d > float64(capDelay) {
		return capDelay
	}
	return time.Duration(d)
}
func (e *Engine) Wait(ctx context.Context, id string) (*scheduler.Run, error) {
	e.mu.Lock()
	a := e.active[id]
	e.mu.Unlock()
	if a != nil {
		select {
		case <-a.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return e.Store.Load(id)
}
func (e *Engine) Cancel(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if a := e.active[id]; a != nil {
		a.cancel()
		return nil
	}
	_, err := e.Store.Load(id)
	return err
}
func (e *Engine) Subscribe(id string) (<-chan logs.Event, func(), error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if a := e.active[id]; a != nil {
		c, unsub := a.sink.Subscribe()
		return c, unsub, nil
	}
	if _, err := e.Store.Load(id); err != nil {
		return nil, nil, err
	}
	c := make(chan logs.Event)
	close(c)
	return c, func() {}, nil
}
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closing {
		e.mu.Unlock()
		return nil
	}
	e.closing = true
	for _, a := range e.active {
		a.cancel()
	}
	e.mu.Unlock()
	e.maintenanceCancel()
	e.wg.Wait()
	e.unlock()
	return nil
}
func (e *Engine) Ready() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing {
		return errors.New("shutting down")
	}
	f, err := os.CreateTemp(e.Store.Dir, ".ready-*")
	if err != nil {
		return err
	}
	name := f.Name()
	err = f.Close()
	_ = os.Remove(name)
	return err
}
func (e *Engine) Metrics() string {
	e.mu.Lock()
	n := len(e.active)
	e.mu.Unlock()
	return fmt.Sprintf("# TYPE cicd_runs_total counter\ncicd_runs_total %d\n# TYPE cicd_runs_active gauge\ncicd_runs_active %d\n# TYPE cicd_jobs_total counter\ncicd_jobs_total %d\n# TYPE cicd_jobs_active gauge\ncicd_jobs_active %d\n# TYPE cicd_worker_slots_used gauge\ncicd_worker_slots_used %d\n# TYPE cicd_worker_slots_total gauge\ncicd_worker_slots_total %d\n", e.runsTotal.Load(), n, e.jobsTotal.Load(), e.jobsActive.Load(), len(e.slots), cap(e.slots))
}
