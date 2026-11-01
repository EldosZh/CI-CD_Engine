package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

type Request struct {
	Run            string
	Command        []string
	Shell          string
	Dir            string
	Env            []string
	Stdout, Stderr io.Writer
}
type Session interface {
	Execute(context.Context, Request) (int, error)
	Close() error
}
type Local struct{}

func ShellArgs(shell, script string) (string, []string, error) {
	if shell == "" {
		if runtime.GOOS == "windows" {
			shell = "powershell"
		} else {
			shell = "sh"
		}
	}
	switch shell {
	case "sh", "bash":
		return shell, []string{"-c", script}, nil
	case "cmd":
		return "cmd", []string{"/D", "/S", "/C", script}, nil
	case "powershell", "pwsh":
		return shell, []string{"-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script}, nil
	}
	return "", nil, fmt.Errorf("unsupported shell")
}
func (l *Local) Execute(ctx context.Context, r Request) (int, error) {
	args := r.Command
	if len(args) == 0 {
		exe, a, e := ShellArgs(r.Shell, r.Run)
		if e != nil {
			return -1, e
		}
		args = append([]string{exe}, a...)
	}
	if len(args) == 0 {
		return -1, fmt.Errorf("empty command")
	}
	return command(ctx, args, r.Dir, r.Env, r.Stdout, r.Stderr)
}
func (l *Local) Close() error { return nil }
func command(ctx context.Context, args []string, dir string, env []string, out, errout io.Writer) (int, error) {
	c := exec.CommandContext(ctx, args[0], args[1:]...)
	c.Dir = dir
	c.Env = env
	c.Stdout = out
	c.Stderr = errout
	c.WaitDelay = 2 * time.Second
	configure(c)
	e := c.Run()
	code := 0
	if e != nil {
		code = -1
		var ex *exec.ExitError
		if errors.As(e, &ex) {
			code = ex.ExitCode()
		}
	}
	if ctx.Err() != nil {
		return code, ctx.Err()
	}
	return code, e
}

type Docker struct {
	ID    string
	owner string
}

func NewDocker(ctx context.Context, workspace, image, run, job, owner string) (*Docker, error) {
	args := []string{"create", "--label", "cicd.engine=true", "--label", "cicd.run=" + run, "--label", "cicd.job=" + job, "--label", "cicd.owner=" + owner, "--mount", "type=bind,source=" + workspace + ",target=/workspace", "--workdir", "/workspace", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--memory", "1g", "--cpus", "2", "--pids-limit", "256", "--entrypoint", "sh", image, "-c", "while :; do sleep 3600; done"}
	b, e := exec.CommandContext(ctx, "docker", args...).Output()
	id := strings.TrimSpace(string(b))
	if e != nil {
		return nil, fmt.Errorf("docker create: %w", e)
	}
	if id == "" || strings.ContainsAny(id, " \r\n\t") {
		return nil, fmt.Errorf("invalid container ID")
	}
	d := &Docker{ID: id, owner: owner}
	if e = exec.CommandContext(ctx, "docker", "start", id).Run(); e != nil {
		d.Close()
		return nil, fmt.Errorf("docker start: %w", e)
	}
	return d, nil
}
func (d *Docker) Execute(ctx context.Context, r Request) (int, error) {
	args := []string{"docker", "exec", "--workdir", "/workspace"}
	for _, e := range r.Env {
		args = append(args, "--env", e)
	}
	args = append(args, d.ID)
	if len(r.Command) > 0 {
		args = append(args, r.Command...)
	} else {
		shell := r.Shell
		if shell == "" {
			shell = "sh"
		}
		exe, a, e := ShellArgs(shell, r.Run)
		if e != nil {
			return -1, e
		}
		args = append(args, exe)
		args = append(args, a...)
	}
	code, e := command(ctx, args, "", nil, r.Stdout, r.Stderr)
	if ctx.Err() != nil {
		_ = d.Close()
	}
	return code, e
}
func (d *Docker) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "rm", "-f", d.ID).Run()
}
func DockerAvailable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "info").Run()
}

// Owner scopes crash cleanup to this engine data directory; unrelated containers are untouched.
func CleanupDocker(owner string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	b, e := exec.CommandContext(ctx, "docker", "ps", "-aq", "--filter", "label=cicd.engine=true", "--filter", "label=cicd.owner="+owner).Output()
	if e != nil {
		return e
	}
	var errs []error
	for _, id := range strings.Fields(string(b)) {
		errs = append(errs, exec.CommandContext(ctx, "docker", "rm", "-f", id).Run())
	}
	return errors.Join(errs...)
}
