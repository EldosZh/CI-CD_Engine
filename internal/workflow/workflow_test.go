package workflow

import (
	"strings"
	"testing"
)

const minimal = "version: 1\nname: test\njobs:\n  a:\n    steps:\n      - run: echo ok\n"

func TestParseAndValidation(t *testing.T) {
	tests := []struct {
		name, body string
		valid      bool
	}{
		{"minimal", minimal, true},
		{"unknown field", minimal + "unknown: 1\n", false},
		{"multiple documents", minimal + "---\n" + minimal, false},
		{"duplicate IDs", "version: 1\nname: test\njobs:\n  a: {steps: [{run: a}]}\n  a: {steps: [{run: b}]}\n", false},
		{"unknown dependency", strings.Replace(minimal, "    steps:", "    needs: [missing]\n    steps:", 1), false},
		{"self dependency", strings.Replace(minimal, "    steps:", "    needs: [a]\n    steps:", 1), false},
		{"invalid runner", strings.Replace(minimal, "    steps:", "    runs-on: remote\n    steps:", 1), false},
		{"invalid timeout", strings.Replace(minimal, "    steps:", "    timeout: -1s\n    steps:", 1), false},
		{"invalid schema", strings.Replace(minimal, "version: 1", "version: 2", 1), false},
		{"unsafe artifact", strings.Replace(minimal, "run: echo ok", "artifact: {name: data, path: ../../escape}", 1), false},
		{"empty step", strings.Replace(minimal, "run: echo ok", "name: empty", 1), false},
		{"incompatible", strings.Replace(minimal, "run: echo ok", "run: echo ok\n        uses: checkout", 1), false},
		{"retry", strings.Replace(minimal, "run: echo ok", "run: echo ok\n        retry: {max_attempts: 3, initial_delay: 1ms, max_delay: 2ms, multiplier: 2}", 1), true},
		{"invalid retry", strings.Replace(minimal, "run: echo ok", "run: echo ok\n        retry: {max_attempts: -1}", 1), false},
		{"cycle", "version: 1\nname: cycle\njobs:\n  a: {needs: [b], steps: [{run: a}]}\n  b: {needs: [c], steps: [{run: b}]}\n  c: {needs: [a], steps: [{run: c}]}\n", false},
		{"invalid YAML", "[broken", false},
		{"bad env", strings.Replace(minimal, "    steps:", "    env: {'A-B': value}\n    steps:", 1), false},
		{"bad image", strings.Replace(minimal, "    steps:", "    runs-on: docker\n    container: {image: '--help'}\n    steps:", 1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tt.body))
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%v err=%v", tt.valid, err)
			}
		})
	}
}
func TestConditions(t *testing.T) {
	if !Condition("always()", false, true, false) || Condition("success()", false, true, false) || !Condition("failure()", false, true, false) || !Condition("cancelled()", false, false, true) {
		t.Fatal("conditions")
	}
}
func FuzzParse(f *testing.F) {
	f.Add(minimal)
	f.Add("[bad")
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 65536 {
			t.Skip()
		}
		_, _ = Parse(strings.NewReader(s))
	})
}
func TestInputLimit(t *testing.T) {
	if _, e := Parse(strings.NewReader(minimal + strings.Repeat("# padding\n", 1<<18))); e == nil {
		t.Fatal("oversized workflow accepted")
	}
}
