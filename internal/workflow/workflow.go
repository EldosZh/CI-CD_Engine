package workflow

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Workflow struct {
	Version int               `yaml:"version" json:"version"`
	Name    string            `yaml:"name" json:"name"`
	On      []string          `yaml:"on,omitempty" json:"on,omitempty"`
	Env     map[string]string `yaml:"env,omitempty" json:"-"`
	Timeout string            `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	Jobs    map[string]Job    `yaml:"jobs" json:"jobs"`
}
type Job struct {
	Needs     []string          `yaml:"needs,omitempty" json:"needs,omitempty"`
	RunsOn    string            `yaml:"runs-on,omitempty" json:"runs_on,omitempty"`
	Container *ContainerConfig  `yaml:"container,omitempty" json:"container,omitempty"`
	Env       map[string]string `yaml:"env,omitempty" json:"-"`
	Timeout   string            `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	If        string            `yaml:"if,omitempty" json:"if,omitempty"`
	Steps     []Step            `yaml:"steps" json:"steps"`
}
type ContainerConfig struct {
	Image string `yaml:"image" json:"image"`
}
type Step struct {
	Name            string            `yaml:"name,omitempty" json:"name"`
	Run             string            `yaml:"run,omitempty" json:"-"`
	Command         []string          `yaml:"command,omitempty" json:"-"`
	Shell           string            `yaml:"shell,omitempty" json:"shell,omitempty"`
	Uses            string            `yaml:"uses,omitempty" json:"uses,omitempty"`
	Env             map[string]string `yaml:"env,omitempty" json:"-"`
	Timeout         string            `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	Retry           RetryPolicy       `yaml:"retry,omitempty" json:"retry"`
	If              string            `yaml:"if,omitempty" json:"if,omitempty"`
	ContinueOnError bool              `yaml:"continue_on_error,omitempty" json:"continue_on_error"`
	Artifact        *ArtifactConfig   `yaml:"artifact,omitempty" json:"artifact,omitempty"`
}
type RetryPolicy struct {
	MaxAttempts  int     `yaml:"max_attempts,omitempty" json:"max_attempts"`
	InitialDelay string  `yaml:"initial_delay,omitempty" json:"initial_delay,omitempty"`
	MaxDelay     string  `yaml:"max_delay,omitempty" json:"max_delay,omitempty"`
	Multiplier   float64 `yaml:"multiplier,omitempty" json:"multiplier,omitempty"`
}
type ArtifactConfig struct {
	Name string `yaml:"name" json:"name"`
	Path string `yaml:"path" json:"path"`
}

var identifier = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
var envName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
var imageName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/:@-]*$`)

func ValidID(s string) bool { return len(s) > 0 && len(s) <= 128 && identifier.MatchString(s) }
func SafeRelative(s string) bool {
	return s != "" && !strings.Contains(s, `\`) && !strings.Contains(s, ":") && filepath.IsLocal(s)
}
func Parse(r io.Reader) (*Workflow, error) {
	b, err := io.ReadAll(io.LimitReader(r, (2<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 2<<20 {
		return nil, errors.New("workflow exceeds 2 MiB")
	}
	d := yaml.NewDecoder(strings.NewReader(string(b)))
	d.KnownFields(true)
	var w Workflow
	if err := d.Decode(&w); err != nil {
		return nil, fmt.Errorf("parse workflow: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("exactly one YAML document required")
	}
	if err := w.Validate(); err != nil {
		return nil, err
	}
	return &w, nil
}
func Duration(s string) time.Duration { d, _ := time.ParseDuration(s); return d }
func Condition(c string, success, failed, cancelled bool) bool {
	switch c {
	case "", "success()":
		return success
	case "failure()":
		return failed
	case "always()":
		return true
	case "cancelled()":
		return cancelled
	}
	return false
}
func validCondition(c string) bool {
	switch c {
	case "", "success()", "failure()", "always()", "cancelled()":
		return true
	}
	return false
}
func (w *Workflow) IDs() []string {
	ids := make([]string, 0, len(w.Jobs))
	for id := range w.Jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
func (w *Workflow) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	dur := func(label, s string) {
		if s != "" {
			d, e := time.ParseDuration(s)
			if e != nil || d <= 0 {
				add("%s: invalid positive duration %q", label, s)
			}
		}
	}
	env := func(label string, e map[string]string) {
		for k := range e {
			if !envName.MatchString(k) {
				add("%s: invalid environment key %q", label, k)
			}
		}
	}
	if w.Version != 1 {
		add("version must be 1")
	}
	if strings.TrimSpace(w.Name) == "" {
		add("workflow name required")
	}
	if len(w.Jobs) == 0 || len(w.Jobs) > 10000 {
		add("jobs must contain 1..10000 entries")
	}
	for _, t := range w.On {
		if t != "manual" {
			add("unsupported trigger %q", t)
		}
	}
	dur("workflow timeout", w.Timeout)
	env("workflow", w.Env)
	artifacts := map[string]bool{}
	for _, id := range w.IDs() {
		j := w.Jobs[id]
		if !ValidID(id) {
			add("invalid job ID %q", id)
		}
		if j.RunsOn != "" && j.RunsOn != "local" && j.RunsOn != "docker" {
			add("%s: invalid runner", id)
		}
		if j.RunsOn == "docker" {
			if j.Container == nil || !imageName.MatchString(j.Container.Image) || len(j.Container.Image) > 256 {
				add("%s: valid Docker image required", id)
			}
		} else if j.Container != nil {
			add("%s: container requires Docker runner", id)
		}
		dur(id+" timeout", j.Timeout)
		env(id, j.Env)
		if !validCondition(j.If) {
			add("%s: unsupported condition %q", id, j.If)
		}
		seen := map[string]bool{}
		for _, n := range j.Needs {
			if n == id {
				add("%s: self dependency", id)
			}
			if _, ok := w.Jobs[n]; !ok {
				add("%s: unknown dependency %q", id, n)
			}
			if seen[n] {
				add("%s: duplicate dependency %q", id, n)
			}
			seen[n] = true
		}
		if len(j.Steps) == 0 || len(j.Steps) > 1000 {
			add("%s: steps must contain 1..1000 entries", id)
		}
		for i, s := range j.Steps {
			label := fmt.Sprintf("%s step %d", id, i+1)
			n := 0
			if s.Run != "" {
				n++
			}
			if len(s.Command) > 0 {
				n++
				if s.Command[0] == "" {
					add("%s: empty executable", label)
				}
			}
			if s.Uses != "" {
				n++
				if s.Uses != "checkout" {
					add("%s: unknown action %q", label, s.Uses)
				}
			}
			if s.Artifact != nil {
				n++
				a := s.Artifact
				if !ValidID(a.Name) || !SafeRelative(a.Path) {
					add("%s: unsafe artifact name/path", label)
				}
				if artifacts[a.Name] {
					add("%s: duplicate artifact name %q", label, a.Name)
				}
				artifacts[a.Name] = true
			}
			if n != 1 {
				add("%s: exactly one of run, command, uses, artifact required", label)
			}
			switch s.Shell {
			case "", "sh", "bash", "cmd", "powershell", "pwsh":
			default:
				add("%s: unsupported shell", label)
			}
			if s.Shell != "" && s.Run == "" {
				add("%s: shell requires run", label)
			}
			dur(label+" timeout", s.Timeout)
			env(label, s.Env)
			if !validCondition(s.If) {
				add("%s: unsupported condition", label)
			}
			p := s.Retry
			if p.MaxAttempts < 0 || p.MaxAttempts > 10 {
				add("%s: max_attempts must be 1..10 (omitted means 1)", label)
			}
			dur(label+" initial_delay", p.InitialDelay)
			dur(label+" max_delay", p.MaxDelay)
			if p.Multiplier != 0 && (p.Multiplier < 1 || p.Multiplier > 10) {
				add("%s: multiplier must be 1..10", label)
			}
			if Duration(p.InitialDelay) > time.Minute || Duration(p.MaxDelay) > time.Minute {
				add("%s: retry delays capped at 1m", label)
			}
		}
	}
	// DFS colors also records a concrete cycle path.
	color := map[string]int{}
	var stack []string
	var visit func(string)
	visit = func(id string) {
		if color[id] == 2 {
			return
		}
		if color[id] == 1 {
			start := 0
			for i, v := range stack {
				if v == id {
					start = i
					break
				}
			}
			add("dependency cycle detected: %s", strings.Join(append(append([]string{}, stack[start:]...), id), " -> "))
			return
		}
		color[id] = 1
		stack = append(stack, id)
		for _, n := range w.Jobs[id].Needs {
			if _, ok := w.Jobs[n]; ok {
				visit(n)
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = 2
	}
	for _, id := range w.IDs() {
		visit(id)
	}
	return errors.Join(errs...)
}
