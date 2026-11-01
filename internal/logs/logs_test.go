package logs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestMaskAcrossWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs")
	s, e := Open(path, "run", []string{"secret-123", "secret"})
	if e != nil {
		t.Fatal(e)
	}
	w := s.Writer("job", "1", "stdout")
	for _, c := range []string{"before sec", "ret-", "123 after\n", strings.Repeat("x", 100000), "secret-123"} {
		w.Write([]byte(c))
	}
	w.Close()
	s.Close()
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(b), "secret") || !strings.Contains(string(b), "***") {
		t.Fatal("raw secret persisted")
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var ev Event
		if e = json.Unmarshal([]byte(line), &ev); e != nil {
			t.Fatal(e)
		}
		if len(ev.Message) > 16386 {
			t.Fatal("unbounded chunk")
		}
	}
}
func TestSlowSubscriber(t *testing.T) {
	s, e := Open(filepath.Join(t.TempDir(), "logs"), "run", nil)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	c, unsub := s.Subscribe()
	defer unsub()
	for i := 0; i < 1000; i++ {
		s.Note("j", "1", "event")
	}
	count := 0
	for range c {
		count++
	}
	if count != 128 {
		t.Fatal(count)
	}
}
func TestConcurrentWritersAndSubscriptions(t *testing.T) {
	s, e := Open(filepath.Join(t.TempDir(), "logs"), "run", []string{"token"})
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, u := s.Subscribe()
			defer u()
			w := s.Writer("j", "1", "stdout")
			defer w.Close()
			for i := 0; i < 100; i++ {
				w.Write([]byte("token output\n"))
			}
		}()
	}
	wg.Wait()
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestLiveShortLineWithLongSecret(t *testing.T) {
	s, e := Open(filepath.Join(t.TempDir(), "logs"), "run", []string{strings.Repeat("secret", 100)})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	c, u := s.Subscribe()
	defer u()
	w := s.Writer("j", "1", "stdout")
	defer w.Close()
	w.Write([]byte("short live line\n"))
	select {
	case ev := <-c:
		if ev.Message != "short live line\n" {
			t.Fatal(ev)
		}
	default:
		t.Fatal("line unnecessarily buffered behind unrelated secret")
	}
}
