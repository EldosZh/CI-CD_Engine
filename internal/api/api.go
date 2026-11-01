package api

import (
	"cicd-engine/internal/engine"
	"cicd-engine/internal/logs"
	"cicd-engine/internal/workflow"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type API struct {
	Engine *engine.Engine
	Source string
	Token  string
}

func (a *API) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	m.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if err := a.Engine.Ready(); err != nil {
			fail(w, 503, "NOT_READY", err)
			return
		}
		write(w, 200, map[string]string{"status": "ready"})
	})
	m.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, a.Engine.Metrics())
	})
	m.HandleFunc("POST /api/v1/runs", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		b, err := io.ReadAll(r.Body)
		if err != nil {
			fail(w, 413, "BODY_TOO_LARGE", err)
			return
		}
		wf, err := workflow.Parse(strings.NewReader(string(b)))
		if err != nil {
			fail(w, 400, "WORKFLOW_INVALID", err)
			return
		}
		// Run context belongs to engine lifecycle, not the short-lived HTTP request.
		id, err := a.Engine.Submit(context.Background(), wf, a.Source, engine.SecretsFromEnv())
		if err != nil {
			fail(w, 429, "RUN_REJECTED", err)
			return
		}
		write(w, 202, map[string]string{"id": id})
	})
	m.HandleFunc("GET /api/v1/runs", func(w http.ResponseWriter, r *http.Request) {
		rs, err := a.Engine.Store.List()
		if err != nil {
			fail(w, 500, "STORE_ERROR", err)
			return
		}
		if len(rs) > 100 {
			rs = rs[:100]
		}
		write(w, 200, rs)
	})
	m.HandleFunc("GET /api/v1/runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		run, err := a.Engine.Store.Load(r.PathValue("id"))
		if err != nil {
			fail(w, 404, "RUN_NOT_FOUND", err)
			return
		}
		write(w, 200, run)
	})
	m.HandleFunc("GET /api/v1/runs/{id}/jobs", func(w http.ResponseWriter, r *http.Request) {
		run, err := a.Engine.Store.Load(r.PathValue("id"))
		if err != nil {
			fail(w, 404, "RUN_NOT_FOUND", err)
			return
		}
		write(w, 200, run.Jobs)
	})
	m.HandleFunc("POST /api/v1/runs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		if err := a.Engine.Cancel(r.PathValue("id")); err != nil {
			fail(w, 404, "RUN_NOT_FOUND", err)
			return
		}
		write(w, 200, map[string]string{"status": "cancellation requested or run already terminal"})
	})
	m.HandleFunc("GET /api/v1/runs/{id}/logs", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if _, err := a.Engine.Store.Load(id); err != nil {
			fail(w, 404, "RUN_NOT_FOUND", err)
			return
		}
		dir, _ := a.Engine.Store.RunDir(id)
		f, err := os.Open(filepath.Join(dir, "logs.jsonl"))
		if err != nil {
			fail(w, 404, "LOGS_NOT_FOUND", err)
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "application/x-ndjson")
		io.Copy(w, io.LimitReader(f, 8<<20))
	})
	m.HandleFunc("GET /api/v1/runs/{id}/logs/stream", a.stream)
	m.HandleFunc("GET /api/v1/runs/{id}/artifacts", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if _, err := a.Engine.Store.Load(id); err != nil {
			fail(w, 404, "RUN_NOT_FOUND", err)
			return
		}
		v, err := a.Engine.Store.Artifacts(id)
		if err != nil {
			fail(w, 400, "ARTIFACT_ERROR", err)
			return
		}
		write(w, 200, v)
	})
	m.HandleFunc("GET /api/v1/runs/{id}/artifacts/{name}", func(w http.ResponseWriter, r *http.Request) {
		id, name := r.PathValue("id"), r.PathValue("name")
		path, err := a.Engine.Store.ArtifactPath(id, name)
		if err != nil {
			fail(w, 400, "ARTIFACT_INVALID", err)
			return
		}
		as, err := a.Engine.Store.Artifacts(id)
		found := false
		for _, v := range as {
			if v.Name == name {
				found = true
				w.Header().Set("X-Checksum-SHA256", v.Checksum)
			}
		}
		if err != nil || !found {
			fail(w, 404, "ARTIFACT_NOT_FOUND", errors.New("artifact unavailable"))
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.tar.gz"`, name))
		http.ServeFile(w, r, path)
	})
	return lifecycleMiddleware(a.Token, m)
}
func (a *API) stream(w http.ResponseWriter, r *http.Request) {
	c, unsub, err := a.Engine.Subscribe(r.PathValue("id"))
	if err != nil {
		fail(w, 404, "RUN_NOT_FOUND", err)
		return
	}
	defer unsub()
	controller := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprint(w, ": connected\n\n")
	if controller.Flush() != nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		var event *logs.Event
		select {
		case <-r.Context().Done():
			return
		case v, ok := <-c:
			if !ok {
				return
			}
			event = &v
		case <-ticker.C:
		}
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if event != nil {
			b, _ := json.Marshal(event)
			_, err = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", event.Sequence, b)
		} else {
			_, err = fmt.Fprint(w, ": heartbeat\n\n")
		}
		if err != nil || controller.Flush() != nil {
			return
		}
	}
}
func write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, code int, kind string, err error) {
	message := err.Error()
	if code >= 500 {
		message = "service unavailable"
	}
	write(w, code, map[string]any{"error": map[string]string{"code": kind, "message": message}})
}
