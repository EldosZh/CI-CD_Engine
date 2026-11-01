package runner

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestHelperProcess(t *testing.T) {
	if os.Getenv("CICD_RUNNER_HELPER") != "1" {
		return
	}
	switch os.Getenv("CICD_HELPER_MODE") {
	case "sleep":
		time.Sleep(time.Minute)
	case "fail":
		os.Exit(7)
	default:
		dir, _ := os.Getwd()
		os.Stdout.WriteString("stdout:" + os.Getenv("VALUE") + ":" + dir)
		os.Stderr.WriteString("stderr")
	}
	os.Exit(0)
}
func helper(t *testing.T, mode string) Request {
	t.Helper()
	return Request{Command: []string{os.Args[0], "-test.run=^TestHelperProcess$"}, Dir: t.TempDir(), Env: append(os.Environ(), "CICD_RUNNER_HELPER=1", "CICD_HELPER_MODE="+mode, "VALUE=hello")}
}
func TestLocal(t *testing.T) {
	r := helper(t, "")
	var out, errout bytes.Buffer
	r.Stdout = &out
	r.Stderr = &errout
	code, e := (&Local{}).Execute(context.Background(), r)
	if e != nil || code != 0 || !strings.Contains(out.String(), "stdout:hello:"+r.Dir) || errout.String() != "stderr" {
		t.Fatalf("%d %v %s %s", code, e, out.String(), errout.String())
	}
	r = helper(t, "fail")
	code, e = (&Local{}).Execute(context.Background(), r)
	if e == nil || code != 7 {
		t.Fatal(code, e)
	}
}
func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, e := (&Local{}).Execute(ctx, helper(t, "sleep"))
	if e != context.DeadlineExceeded || time.Since(start) > 5*time.Second {
		t.Fatal(e, time.Since(start))
	}
}
func TestShell(t *testing.T) {
	var b bytes.Buffer
	_, e := (&Local{}).Execute(context.Background(), Request{Run: "echo shell-ok", Dir: t.TempDir(), Env: os.Environ(), Stdout: &b, Stderr: &b})
	if e != nil || !strings.Contains(b.String(), "shell-ok") {
		t.Fatal(b.String(), e)
	}
}
func TestDockerLifecycle(t *testing.T) {
	if e := DockerAvailable(); e != nil {
		t.Skipf("Docker daemon unavailable: %v", e)
	}
	imageCtx, imageCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer imageCancel()
	if e := exec.CommandContext(imageCtx, "docker", "image", "inspect", "alpine:3.21").Run(); e != nil {
		t.Skipf("offline Docker test requires cached alpine:3.21: %v", e)
	}
	if runtime.GOOS == "windows" {
		t.Log("requires Docker Linux containers with host bind support")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ws := t.TempDir()
	d, e := NewDocker(ctx, ws, "alpine:3.21", "test-run", "test-job", "cicd-engine-integration-test")
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	var b bytes.Buffer
	r := Request{Run: "printf hello > /workspace/shared; echo $VALUE", Env: []string{"VALUE=present"}, Stdout: &b, Stderr: &b}
	if _, e = d.Execute(ctx, r); e != nil {
		t.Fatal(e, b.String())
	}
	r.Run = "cat /workspace/shared"
	if _, e = d.Execute(ctx, r); e != nil || !strings.Contains(b.String(), "present") || !strings.Contains(b.String(), "hello") {
		t.Fatal(e, b.String())
	}
	if content, e := os.ReadFile(filepath.Join(ws, "shared")); e != nil || string(content) != "hello" {
		t.Fatal(e)
	}
	r.Run = "exit 9"
	if code, e := d.Execute(ctx, r); e == nil || code != 9 {
		t.Fatal(code, e)
	}
	short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	r.Run = "sleep 60"
	if _, e = d.Execute(short, r); e == nil {
		t.Fatal("timeout ignored")
	}
	if exec.Command("docker", "inspect", d.ID).Run() == nil {
		t.Fatal("container leaked")
	}
}
