package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cicd-engine/internal/api"
	"cicd-engine/internal/dag"
	"cicd-engine/internal/engine"
	"cicd-engine/internal/logs"
	"cicd-engine/internal/scheduler"
	"cicd-engine/internal/storage"
	"cicd-engine/internal/workflow"
)

const version = "0.1.0"
const help = `CI/CD Engine - trusted, single-node workflow execution

Usage:
  cicd validate FILE
  cicd run FILE [--workers 4] [--workspace .] [--job ID] [--data data]
  cicd runs list [--data data]
  cicd runs show ID [--data data]
  cicd logs ID [--job ID] [--step NUMBER] [--follow] [--data data]
  cicd cancel ID [--server http://127.0.0.1:8080]
  cicd artifacts list ID [--data data]
  cicd artifacts download ID NAME [--output NAME.tar.gz] [--data data]
  cicd serve [--addr 127.0.0.1:8080] [--workspace .] [--workers 4] [--data data]
             [--artifact-retention 168h] [--max-runs 8]
  cicd benchmark scheduler [--jobs 10000] [--workers 32]
  cicd version

Flags follow positional arguments. API token: CICD_API_TOKEN.
Run secrets: CICD_SECRET_<NAME>, referenced in env as ${{ secrets.NAME }}.
Cancellation of daemon runs uses the API; foreground runs use Ctrl+C.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
func flags(name string) *flag.FlagSet { return flag.NewFlagSet(name, flag.ContinueOnError) }
func dump(v any) error                { e := json.NewEncoder(os.Stdout); e.SetIndent("", "  "); return e.Encode(v) }
func readWorkflow(path string) (*workflow.Workflow, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	return workflow.Parse(f)
}
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
func run(args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "help" || args[0] == "-h" {
		fmt.Print(help)
		return nil
	}
	switch args[0] {
	case "version":
		fmt.Println("cicd", version)
		return nil
	case "validate":
		if len(args) != 2 {
			return errors.New("usage: cicd validate FILE")
		}
		w, e := readWorkflow(args[1])
		if e != nil {
			return e
		}
		g, e := dag.Build(w)
		if e != nil {
			return e
		}
		order, e := g.Topological()
		if e != nil {
			return e
		}
		fmt.Printf("valid: %s (%s)\n", w.Name, strings.Join(order, ", "))
		return nil
	case "run":
		if len(args) < 2 {
			return errors.New("workflow file required")
		}
		f := flags("run")
		workers := f.Int("workers", 4, "global job slots")
		source := f.String("workspace", ".", "source snapshot directory")
		data := f.String("data", "data", "data directory")
		only := f.String("job", "", "selected job and transitive dependencies")
		if e := f.Parse(args[2:]); e != nil {
			return e
		}
		w, e := readWorkflow(args[1])
		if e != nil {
			return e
		}
		if *only != "" {
			if _, ok := w.Jobs[*only]; !ok {
				return errors.New("unknown selected job")
			}
			jobs := map[string]workflow.Job{}
			var include func(string)
			include = func(id string) {
				if _, ok := jobs[id]; ok {
					return
				}
				jobs[id] = w.Jobs[id]
				for _, n := range w.Jobs[id].Needs {
					include(n)
				}
			}
			include(*only)
			w.Jobs = jobs
		}
		ctx, cancel := signalContext()
		defer cancel()
		eng, e := engine.Open(*data, engine.Config{Workers: *workers})
		if e != nil {
			return e
		}
		defer eng.Close()
		id, e := eng.Submit(ctx, w, *source, engine.SecretsFromEnv())
		if e != nil {
			return e
		}
		fmt.Println("run:", id)
		c, unsub, e := eng.Subscribe(id)
		if e != nil {
			return e
		}
		defer unsub()
		for ev := range c {
			fmt.Printf("%s [%s/%s] %s: %s", ev.Timestamp.Format("15:04:05.000"), ev.JobID, ev.StepID, ev.Stream, ev.Message)
			if !strings.HasSuffix(ev.Message, "\n") {
				fmt.Println()
			}
		}
		r, e := eng.Wait(context.Background(), id)
		if e != nil {
			return e
		}
		if e = dump(r); e != nil {
			return e
		}
		if r.State != scheduler.Success {
			return fmt.Errorf("run %s: %s", id, r.State)
		}
		return nil
	case "runs":
		if len(args) < 2 {
			return errors.New("runs list|show")
		}
		f := flags("runs")
		data := f.String("data", "data", "data directory")
		pos := 2
		if args[1] == "show" {
			pos = 3
		}
		if len(args) < pos {
			return errors.New("run ID required")
		}
		if e := f.Parse(args[pos:]); e != nil {
			return e
		}
		s, e := storage.New(*data)
		if e != nil {
			return e
		}
		if args[1] == "list" {
			v, e := s.List()
			if e != nil {
				return e
			}
			return dump(v)
		}
		if args[1] == "show" {
			v, e := s.Load(args[2])
			if e != nil {
				return e
			}
			return dump(v)
		}
		return errors.New("runs list|show")
	case "logs":
		return logCommand(args[1:])
	case "cancel":
		if len(args) < 2 {
			return errors.New("run ID required")
		}
		f := flags("cancel")
		server := f.String("server", "http://127.0.0.1:8080", "API server")
		if e := f.Parse(args[2:]); e != nil {
			return e
		}
		if !workflow.ValidID(args[1]) {
			return errors.New("invalid run ID")
		}
		req, e := http.NewRequest("POST", strings.TrimRight(*server, "/")+"/api/v1/runs/"+args[1]+"/cancel", nil)
		if e != nil {
			return e
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("CICD_API_TOKEN"))
		client := &http.Client{Timeout: 10 * time.Second}
		resp, e := client.Do(req)
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		io.Copy(os.Stdout, io.LimitReader(resp.Body, 1<<20))
		if resp.StatusCode != 200 {
			return errors.New(resp.Status)
		}
		return nil
	case "artifacts":
		return artifactCommand(args[1:])
	case "serve":
		return serve(args[1:])
	case "benchmark":
		return benchmark(args[1:])
	}
	return fmt.Errorf("unknown command %q", args[0])
}
func logCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("run ID required")
	}
	f := flags("logs")
	data := f.String("data", "data", "data directory")
	job := f.String("job", "", "job filter")
	step := f.String("step", "", "step number filter")
	follow := f.Bool("follow", false, "follow persisted logs until run terminal")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	s, e := storage.New(*data)
	if e != nil {
		return e
	}
	dir, e := s.RunDir(args[0])
	if e != nil {
		return e
	}
	ctx, cancel := signalContext()
	defer cancel()
	var sequence uint64
	for {
		file, e := os.Open(filepath.Join(dir, "logs.jsonl"))
		if e != nil {
			return e
		}
		d := json.NewDecoder(file)
		for {
			var event logs.Event
			e = d.Decode(&event)
			if e != nil {
				break
			}
			if event.Sequence > sequence {
				sequence = event.Sequence
				if (*job == "" || event.JobID == *job) && (*step == "" || event.StepID == *step) {
					if e = json.NewEncoder(os.Stdout).Encode(event); e != nil {
						file.Close()
						return e
					}
				}
			}
		}
		file.Close()
		if e != io.EOF && !(*follow && errors.Is(e, io.ErrUnexpectedEOF)) {
			return e
		}
		if !*follow {
			return nil
		}
		r, e := s.Load(args[0])
		if e != nil {
			return e
		}
		if r.State.Terminal() {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(200 * time.Millisecond):
		}
	}
}
func artifactCommand(args []string) error {
	if len(args) < 2 {
		return errors.New("artifacts list ID | download ID NAME")
	}
	pos := 2
	if args[0] == "download" {
		pos = 3
	}
	if len(args) < pos {
		return errors.New("artifact name required")
	}
	f := flags("artifacts")
	data := f.String("data", "data", "data directory")
	output := f.String("output", "", "download destination")
	if e := f.Parse(args[pos:]); e != nil {
		return e
	}
	s, e := storage.New(*data)
	if e != nil {
		return e
	}
	if args[0] == "list" {
		as, e := s.Artifacts(args[1])
		if e != nil {
			return e
		}
		return dump(as)
	}
	if args[0] != "download" {
		return errors.New("artifacts list|download")
	}
	path, e := s.ArtifactPath(args[1], args[2])
	if e != nil {
		return e
	}
	if *output == "" {
		*output = args[2] + ".tar.gz"
	}
	in, e := os.Open(path)
	if e != nil {
		return e
	}
	defer in.Close()
	out, e := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	_, e = io.Copy(out, in)
	return errors.Join(e, out.Close())
}
func serve(args []string) error {
	f := flags("serve")
	addr := f.String("addr", "127.0.0.1:8080", "listen address")
	data := f.String("data", "data", "data directory")
	source := f.String("workspace", ".", "fixed source directory for API runs")
	workers := f.Int("workers", 4, "global job slots")
	maxRuns := f.Int("max-runs", 8, "active run bound")
	retention := f.Duration("artifact-retention", 168*time.Hour, "terminal artifact retention")
	if e := f.Parse(args); e != nil {
		return e
	}
	host, _, e := net.SplitHostPort(*addr)
	if e != nil {
		return e
	}
	ip := net.ParseIP(host)
	if (ip == nil || !ip.IsLoopback()) && os.Getenv("CICD_API_TOKEN") == "" {
		return errors.New("non-loopback API binding requires CICD_API_TOKEN")
	}
	eng, e := engine.Open(*data, engine.Config{Workers: *workers, MaxRuns: *maxRuns, ArtifactRetention: *retention})
	if e != nil {
		return e
	}
	defer eng.Close()
	a := &api.API{Engine: eng, Source: *source, Token: os.Getenv("CICD_API_TOKEN")}
	srv := &http.Server{Addr: *addr, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	listener, e := net.Listen("tcp", *addr)
	if e != nil {
		return e
	}
	ctx, stop := signalContext()
	defer stop()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	fmt.Println("listening:", listener.Addr())
	select {
	case e = <-done:
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	case <-ctx.Done():
	}
	_ = listener.Close()
	eng.Close()
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}
func benchmark(args []string) error {
	if len(args) == 0 || args[0] != "scheduler" {
		return errors.New("benchmark scheduler required")
	}
	f := flags("benchmark scheduler")
	count := f.Int("jobs", 10000, "synthetic jobs")
	workers := f.Int("workers", 32, "workers")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if *count < 1 || *count > 10000 || *workers < 1 || *workers > 256 {
		return errors.New("jobs 1..10000, workers 1..256")
	}
	w := &workflow.Workflow{Version: 1, Name: "scheduler-only", Jobs: map[string]workflow.Job{}}
	for i := 0; i < *count; i++ {
		j := workflow.Job{Steps: []workflow.Step{{Run: "noop"}}}
		if i >= *workers {
			j.Needs = []string{strconv.Itoa(i - *workers)}
		}
		w.Jobs[strconv.Itoa(i)] = j
	}
	start := time.Now()
	delays := []time.Duration{}
	_, e := scheduler.Schedule(context.Background(), w, *workers, make(chan struct{}, *workers), func(ctx context.Context, id string, j workflow.Job) scheduler.JobResult {
		return scheduler.JobResult{ID: id, State: scheduler.Success}
	}, func(r scheduler.JobResult) {
		if r.State == scheduler.Success {
			delays = append(delays, time.Since(r.StartedAt))
		}
	})
	if e != nil {
		return e
	}
	elapsed := time.Since(start)
	sort.Slice(delays, func(i, j int) bool { return delays[i] < delays[j] })
	fmt.Printf("scheduler-only, no processes: jobs=%d workers=%d elapsed=%s throughput=%.0f jobs/s dispatch-to-observed-completion p50=%s p95=%s\n", *count, *workers, elapsed, float64(*count)/elapsed.Seconds(), delays[len(delays)/2], delays[(len(delays)-1)*95/100])
	return nil
}
