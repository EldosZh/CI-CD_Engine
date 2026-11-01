package engine

import (
	"cicd-engine/internal/scheduler"
	"cicd-engine/internal/workflow"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEngineHelper(t *testing.T) {
	if os.Getenv("CICD_ENGINE_HELPER") != "1" {
		return
	}
	switch os.Getenv("MODE") {
	case "retry":
		if os.Getenv("CICD_ATTEMPT") != "3" {
			os.Exit(5)
		}
	case "fail":
		os.Exit(7)
	case "sleep":
		time.Sleep(time.Minute)
	case "artifact":
		os.MkdirAll("dist", 0700)
		os.WriteFile("dist/app.txt", []byte("artifact contents"), 0600)
	case "env":
		if os.Getenv("VALUE") != "step" || os.Getenv("CICD_RUN_ID") == "" || os.Getenv("CICD_JOB_ID") != "a" || os.Getenv("CICD_STEP_ID") != "1" {
			os.Exit(8)
		}
	case "secret":
		fmt.Print(os.Getenv("TOKEN"))
	}
	fmt.Println("helper success")
	os.Exit(0)
}
func testStep(mode string) workflow.Step {
	return workflow.Step{Name: mode, Command: []string{os.Args[0], "-test.run=^TestEngineHelper$"}, Env: map[string]string{"CICD_ENGINE_HELPER": "1", "MODE": mode}}
}
func testWorkflow(steps ...workflow.Step) *workflow.Workflow {
	return &workflow.Workflow{Version: 1, Name: "integration", Jobs: map[string]workflow.Job{"a": {Steps: steps}}}
}
func openTest(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), "data"), Config{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}
func runTest(t *testing.T, e *Engine, w *workflow.Workflow, secrets map[string]string) *scheduler.Run {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	id, err := e.Submit(ctx, w, t.TempDir(), secrets)
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.Wait(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestRetryEnvironmentAndConditions(t *testing.T) {
	e := openTest(t)
	step := testStep("retry")
	step.Retry = workflow.RetryPolicy{MaxAttempts: 3, InitialDelay: "1ms", MaxDelay: "2ms", Multiplier: 2}
	r := runTest(t, e, testWorkflow(step), nil)
	if r.State != scheduler.Success || r.Jobs["a"].Steps[0].Attempt != 3 {
		t.Fatalf("%+v", r)
	}
	step = testStep("env")
	step.Env["VALUE"] = "step"
	w := testWorkflow(step)
	w.Env = map[string]string{"VALUE": "workflow"}
	j := w.Jobs["a"]
	j.Env = map[string]string{"VALUE": "job"}
	w.Jobs["a"] = j
	r = runTest(t, e, w, nil)
	if r.State != scheduler.Success {
		t.Fatal(r)
	}
	failure := testStep("fail")
	normal := testStep("normal")
	cleanup := testStep("cleanup")
	cleanup.If = "always()"
	r = runTest(t, e, testWorkflow(failure, normal, cleanup), nil)
	ss := r.Jobs["a"].Steps
	if r.State != scheduler.Failed || ss[0].State != scheduler.Failed || ss[1].State != scheduler.Skipped || ss[2].State != scheduler.Success {
		t.Fatalf("%+v", r)
	}
	failure.ContinueOnError = true
	r = runTest(t, e, testWorkflow(failure, normal), nil)
	if r.State != scheduler.Success || r.Jobs["a"].Steps[0].State != scheduler.Failed || r.Jobs["a"].Steps[1].State != scheduler.Success {
		t.Fatal(r)
	}
}
func TestSecretAndArtifactPersistence(t *testing.T) {
	e := openTest(t)
	secret := "test-token-" + strings.Repeat("x", 20)
	step := testStep("secret")
	step.Env["TOKEN"] = "${{ secrets.TOKEN }}"
	r := runTest(t, e, testWorkflow(step, testStep("artifact"), workflow.Step{Artifact: &workflow.ArtifactConfig{Name: "app", Path: "dist"}}), map[string]string{"TOKEN": secret})
	if r.State != scheduler.Success {
		t.Fatalf("%+v", r)
	}
	dir, _ := e.Store.RunDir(r.ID)
	for _, name := range []string{"run.json", "logs.jsonl"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || strings.Contains(string(b), secret) {
			t.Fatalf("secret in %s or read error %v", name, err)
		}
	}
	as, err := e.Store.Artifacts(r.ID)
	if err != nil || len(as) != 1 {
		t.Fatal(as, err)
	}
	p, _ := e.Store.ArtifactPath(r.ID, "app")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	if hex.EncodeToString(h[:]) != as[0].Checksum {
		t.Fatal("checksum mismatch")
	}
	dir = e.Store.Dir
	e.Close()
	restarted, err := Open(dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	got, err := restarted.Store.Load(r.ID)
	if err != nil || got.State != scheduler.Success || len(got.Jobs["a"].Steps) != 3 {
		t.Fatal(got, err)
	}
}
func TestTimeouts(t *testing.T) {
	for _, scope := range []string{"workflow", "job", "step"} {
		t.Run(scope, func(t *testing.T) {
			e := openTest(t)
			s := testStep("sleep")
			w := testWorkflow(s)
			switch scope {
			case "workflow":
				w.Timeout = "150ms"
			case "job":
				j := w.Jobs["a"]
				j.Timeout = "150ms"
				w.Jobs["a"] = j
			case "step":
				j := w.Jobs["a"]
				j.Steps[0].Timeout = "150ms"
				w.Jobs["a"] = j
			}
			start := time.Now()
			r := runTest(t, e, w, nil)
			if r.State != scheduler.TimedOut || time.Since(start) > 5*time.Second {
				t.Fatalf("%+v elapsed=%s", r, time.Since(start))
			}
		})
	}
}
func TestCancellationAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprint(shutdown), func(t *testing.T) {
			e := openTest(t)
			w := testWorkflow(testStep("sleep"))
			w.Jobs["b"] = workflow.Job{Needs: []string{"a"}, Steps: []workflow.Step{testStep("unused")}}
			id, err := e.Submit(context.Background(), w, t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				r, err := e.Store.Load(id)
				if err != nil {
					t.Fatal(err)
				}
				if j := r.Jobs["a"]; len(j.Steps) > 0 && j.Steps[0].State == scheduler.Running {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("never started: %+v jobs=%+v", r, r.Jobs)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if shutdown {
				e.Close()
			} else {
				if err = e.Cancel(id); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r, err := e.Wait(ctx, id)
			if err != nil || r.State != scheduler.Cancelled || r.Jobs["b"].State != scheduler.Cancelled {
				t.Fatalf("%+v %v", r, err)
			}
			if err = e.Cancel(id); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestInvalidNeverExecutesAndLimit(t *testing.T) {
	e := openTest(t)
	w := testWorkflow(testStep("normal"))
	w.Version = 2
	if _, err := e.Submit(context.Background(), w, t.TempDir(), nil); err == nil {
		t.Fatal("invalid workflow accepted")
	}
	rs, _ := e.Store.List()
	if len(rs) != 0 {
		t.Fatal("invalid created run")
	}
	e.Config.MaxRuns = 1
	w.Version = 1
	id, err := e.Submit(context.Background(), testWorkflow(testStep("sleep")), t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.Submit(context.Background(), testWorkflow(testStep("normal")), t.TempDir(), nil); err == nil {
		t.Fatal("run limit ignored")
	}
	e.Cancel(id)
}
func TestEnvironmentMissingSecretAndBackoff(t *testing.T) {
	_, err := Environment(nil, []map[string]string{{"TOKEN": "${{ secrets.MISSING }}"}}, nil)
	if err == nil {
		t.Fatal("missing secret accepted")
	}
	p := workflow.RetryPolicy{InitialDelay: "1ms", MaxDelay: "3ms", Multiplier: 2}
	for i, want := range []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond} {
		if got := Backoff(p, i+1); got != want {
			t.Fatal(got, want)
		}
	}
}
