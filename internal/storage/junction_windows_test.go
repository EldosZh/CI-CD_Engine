//go:build windows

package storage

import (
	"cicd-engine/internal/workflow"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestWindowsJunctionEscape(t *testing.T) {
	s, e := New(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	ws := t.TempDir()
	outside := t.TempDir()
	if e = os.WriteFile(filepath.Join(outside, "secret"), []byte("outside file"), 0600); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(ws, "link")
	cmd := exec.Command("powershell", "-NoProfile", "-NonInteractive", "-Command", "New-Item -ItemType Junction -Path $env:CICD_TEST_LINK -Target $env:CICD_TEST_TARGET | Out-Null")
	cmd.Env = append(os.Environ(), "CICD_TEST_LINK="+link, "CICD_TEST_TARGET="+outside)
	if b, e := cmd.CombinedOutput(); e != nil {
		t.Skipf("junction creation unavailable: %v %s", e, b)
	}
	defer os.Remove(link)
	for _, p := range []string{"link", "link/secret"} {
		if _, e = s.Upload("run", "job", ws, workflow.ArtifactConfig{Name: "escape", Path: p}, 1024); e == nil {
			t.Fatalf("junction escape accepted: %s", p)
		}
	}
	if e = CopySnapshot(ws, t.TempDir(), s.Dir); e == nil {
		t.Fatal("snapshot junction accepted")
	}
}
