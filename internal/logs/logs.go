package logs

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Event struct {
	Sequence  uint64    `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
	RunID     string    `json:"run_id"`
	JobID     string    `json:"job_id"`
	StepID    string    `json:"step_id"`
	Stream    string    `json:"stream"`
	Message   string    `json:"message"`
}
type Sink struct {
	mu      sync.Mutex
	file    *os.File
	id      string
	seq     uint64
	bytes   int64
	err     error
	subs    map[chan Event]bool
	secrets []string
	closed  bool
}

func Open(path, id string, secrets []string) (*Sink, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	vals := []string{}
	for _, v := range secrets {
		if v != "" {
			vals = append(vals, v)
		}
	}
	sort.Slice(vals, func(i, j int) bool { return len(vals[i]) > len(vals[j]) })
	return &Sink{file: f, id: id, subs: map[chan Event]bool{}, secrets: vals}, nil
}
func Mask(s string, secrets []string) string {
	for _, v := range secrets {
		if v != "" {
			s = strings.ReplaceAll(s, v, "***")
		}
	}
	return s
}
func (s *Sink) emit(job, step, stream, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.err != nil {
		return
	}
	if s.bytes >= 64<<20 {
		return
	}
	s.seq++
	e := Event{s.seq, time.Now().UTC(), s.id, job, step, stream, msg}
	b, err := json.Marshal(e)
	if err == nil {
		b = append(b, '\n')
		_, err = s.file.Write(b)
		s.bytes += int64(len(b))
	}
	if err != nil {
		s.err = err
		return
	}
	for c := range s.subs {
		select {
		case c <- e:
		default:
			close(c)
			delete(s.subs, c)
		}
	}
}
func (s *Sink) Subscribe() (<-chan Event, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := make(chan Event, 128)
	if s.closed {
		close(c)
	} else {
		s.subs[c] = true
	}
	return c, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.subs[c] {
			delete(s.subs, c)
			close(c)
		}
	}
}
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.err
	}
	s.closed = true
	for c := range s.subs {
		close(c)
		delete(s.subs, c)
	}
	e := s.file.Close()
	if s.err != nil {
		return s.err
	}
	return e
}

// Writer bounds output chunks, retaining enough raw bytes to mask split secrets.
type Writer struct {
	sink              *Sink
	job, step, stream string
	pending, out      []byte
}

func (s *Sink) Writer(job, step, stream string) *Writer {
	return &Writer{sink: s, job: job, step: step, stream: stream}
}
func (w *Writer) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		take := 4096
		if len(p) < take {
			take = len(p)
		}
		w.pending = append(w.pending, p[:take]...)
		p = p[take:]
		w.drain(false)
	}
	return n, nil
}
func (w *Writer) drain(final bool) {
	for len(w.pending) > 0 {
		if !final {
			// Retain only an actual incomplete secret prefix, allowing short
			// unrelated lines to reach live readers immediately.
			for _, v := range w.sink.secrets {
				if len(w.pending) < len(v) && strings.HasPrefix(v, string(w.pending)) {
					return
				}
			}
		}
		matched := false
		for _, v := range w.sink.secrets {
			if len(w.pending) >= len(v) && string(w.pending[:len(v)]) == v {
				w.out = append(w.out, "***"...)
				w.pending = w.pending[len(v):]
				matched = true
				break
			}
		}
		if !matched {
			b := w.pending[0]
			w.pending = w.pending[1:]
			w.out = append(w.out, b)
		}
		if len(w.out) >= 16384 || w.out[len(w.out)-1] == '\n' {
			w.flush()
		}
	}
}
func (w *Writer) flush() {
	if len(w.out) > 0 {
		w.sink.emit(w.job, w.step, w.stream, string(w.out))
		w.out = w.out[:0]
	}
}
func (w *Writer) Close() error { w.drain(true); w.flush(); return nil }
func Read(path string, dst io.Writer, max int64) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	_, e = io.Copy(dst, io.LimitReader(f, max))
	return e
}
func (s *Sink) Error() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *Sink) Note(job, step, msg string) {
	w := s.Writer(job, step, "engine")
	fmt.Fprintln(w, msg)
	w.Close()
}
