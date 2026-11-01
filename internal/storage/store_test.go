package storage

import (
	"archive/tar"
	"cicd-engine/internal/scheduler"
	"cicd-engine/internal/workflow"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestArtifactAndChecksum(t *testing.T) {
	s, e := New(filepath.Join(t.TempDir(), "data"))
	if e != nil {
		t.Fatal(e)
	}
	ws := t.TempDir()
	os.MkdirAll(filepath.Join(ws, "dist"), 0700)
	os.WriteFile(filepath.Join(ws, "dist", "app.txt"), []byte("hello"), 0600)
	a, e := s.Upload("run", "job", ws, workflow.ArtifactConfig{Name: "app", Path: "dist"}, 1024)
	if e != nil {
		t.Fatal(e)
	}
	path, _ := s.ArtifactPath("run", "app")
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	h := sha256.Sum256(b)
	if a.Checksum != hex.EncodeToString(h[:]) || a.Files != 1 || a.Size != int64(len(b)) {
		t.Fatal(a)
	}
	f, _ := os.Open(path)
	defer f.Close()
	gz, e := gzip.NewReader(f)
	if e != nil {
		t.Fatal(e)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	hdr, e := tr.Next()
	if e != nil || hdr.Name != "dist/app.txt" {
		t.Fatalf("%v %v", hdr, e)
	}
	content, _ := io.ReadAll(tr)
	if string(content) != "hello" {
		t.Fatal(string(content))
	}
	as, e := s.Artifacts("run")
	if e != nil || len(as) != 1 {
		t.Fatal(e)
	}
	if _, e = s.Upload("run", "job", ws, workflow.ArtifactConfig{Name: "large", Path: "dist"}, 1); e == nil {
		t.Fatal("size limit ignored")
	}
}
func TestPathSafety(t *testing.T) {
	for _, p := range []string{"../outside", "/etc/passwd", `C:\Windows`, "a/../../b", `..\outside`, "x:stream"} {
		if _, e := SafeJoin(t.TempDir(), p); e == nil {
			t.Fatalf("accepted %s", p)
		}
	}
	if _, e := SafeJoin(t.TempDir(), "dist/app.txt"); e != nil {
		t.Fatal(e)
	}
}
func TestSymlinkEscape(t *testing.T) {
	s, _ := New(t.TempDir())
	ws := t.TempDir()
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "secret"), []byte("host secret"), 0600)
	if e := os.Symlink(outside, filepath.Join(ws, "link")); e != nil {
		t.Skipf("symlink privilege unavailable: %v", e)
	}
	for _, p := range []string{"link", "link/secret"} {
		if _, e := s.Upload("run", "job", ws, workflow.ArtifactConfig{Name: "bad", Path: p}, 1024); e == nil {
			t.Fatal("symlink escape accepted")
		}
	}
	if e := CopySnapshot(ws, t.TempDir(), s.Dir); e == nil {
		t.Fatal("snapshot symlink accepted")
	}
}
func TestPersistenceRecoveryAndLock(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(dir)
	unlock, e := s.Lock()
	if e != nil {
		t.Fatal(e)
	}
	if second, e := s.Lock(); e == nil {
		second()
		t.Fatal("duplicate owner accepted")
	}
	unlock()
	now := time.Now().UTC()
	r := &scheduler.Run{SchemaVersion: 1, ID: "run", Name: "test", State: scheduler.Running, CreatedAt: now, Jobs: map[string]scheduler.JobResult{"a": {ID: "a", State: scheduler.Running, Steps: []scheduler.StepResult{{State: scheduler.Running}}}}}
	if e = s.Save(r); e != nil {
		t.Fatal(e)
	}
	s2, _ := New(dir)
	if e = s2.Recover(); e != nil {
		t.Fatal(e)
	}
	got, e := s2.Load("run")
	if e != nil || got.State != scheduler.Interrupted || got.Jobs["a"].State != scheduler.Interrupted || got.Jobs["a"].Steps[0].State != scheduler.Interrupted {
		t.Fatalf("%+v %v", got, e)
	}
	rs, e := s2.List()
	if e != nil || len(rs) != 1 {
		t.Fatal(e)
	}
}
func TestRetention(t *testing.T) {
	s, _ := New(t.TempDir())
	for _, id := range []string{"old", "active", "new"} {
		path, _ := s.ArtifactPath(id, "app")
		os.MkdirAll(filepath.Dir(path), 0700)
		os.WriteFile(path, []byte("archive"), 0600)
		state := scheduler.Success
		finished := time.Now().Add(-48 * time.Hour)
		if id == "active" {
			state = scheduler.Running
		}
		if id == "new" {
			finished = time.Now()
		}
		s.Save(&scheduler.Run{SchemaVersion: 1, ID: id, State: state, FinishedAt: finished})
	}
	if e := s.Prune(24 * time.Hour); e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{"old", "active", "new"} {
		p, _ := s.ArtifactPath(id, "app")
		_, e := os.Stat(p)
		if (e == nil) != (id != "old") {
			t.Fatal(id, e)
		}
	}
}
