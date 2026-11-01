package api

import (
	"bufio"
	"cicd-engine/internal/engine"
	"cicd-engine/internal/scheduler"
	"cicd-engine/internal/workflow"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestAPIHelper(t *testing.T) {
	if os.Getenv("CICD_API_HELPER") != "1" {
		return
	}
	switch os.Getenv("MODE") {
	case "sleep":
		time.Sleep(time.Minute)
	case "logs":
		time.Sleep(200 * time.Millisecond)
		fmt.Println("live-event")
	case "artifact":
		f, e := os.Create("app.txt")
		if e != nil {
			os.Exit(2)
		}
		for i := 0; i < 2048; i++ {
			if _, e = f.WriteString(strings.Repeat("a", 1024)); e != nil {
				os.Exit(3)
			}
		}
		f.Close()
	}
	os.Exit(0)
}
func fixture(t *testing.T, token string) (*engine.Engine, *httptest.Server) {
	t.Helper()
	e, err := engine.Open(t.TempDir(), engine.Config{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&API{Engine: e, Source: t.TempDir(), Token: token}).Handler())
	t.Cleanup(func() { e.Close(); server.Close() })
	return e, server
}
func wf(mode string) *workflow.Workflow {
	return &workflow.Workflow{Version: 1, Name: "API test", Jobs: map[string]workflow.Job{"a": {Steps: []workflow.Step{{Command: []string{os.Args[0], "-test.run=^TestAPIHelper$"}, Env: map[string]string{"CICD_API_HELPER": "1", "MODE": mode}}}}}}
}
func submit(t *testing.T, s *httptest.Server, w *workflow.Workflow) string {
	t.Helper()
	body, err := yaml.Marshal(w)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s.Client().Post(s.URL+"/api/v1/runs", "application/yaml", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 202 {
		t.Fatalf("submit %d %s", resp.StatusCode, b)
	}
	var result struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(b, &result); err != nil {
		t.Fatal(err)
	}
	return result.ID
}
func get(t *testing.T, s *httptest.Server, path string) *http.Response {
	t.Helper()
	resp, err := s.Client().Get(s.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
func wait(t *testing.T, e *engine.Engine, id string) *scheduler.Run {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r, err := e.Wait(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestAPIHealthValidationAndAuth(t *testing.T) {
	_, s := fixture(t, "")
	for _, path := range []string{"/health", "/ready", "/metrics", "/api/v1/runs"} {
		r := get(t, s, path)
		r.Body.Close()
		if r.StatusCode != 200 {
			t.Fatal(path, r.StatusCode)
		}
	}
	r, err := s.Client().Post(s.URL+"/api/v1/runs", "application/yaml", strings.NewReader("invalid: yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 400 {
		t.Fatal(r.StatusCode)
	}
	var problem map[string]any
	if err = json.NewDecoder(r.Body).Decode(&problem); err != nil || problem["error"] == nil {
		t.Fatal(problem, err)
	}
	large, err := s.Client().Post(s.URL+"/api/v1/runs", "application/yaml", strings.NewReader(strings.Repeat("x", (1<<20)+1)))
	if err != nil {
		t.Fatal(err)
	}
	large.Body.Close()
	if large.StatusCode != 413 {
		t.Fatal(large.StatusCode)
	}
	_, auth := fixture(t, "test-api-token")
	r = get(t, auth, "/api/v1/runs")
	r.Body.Close()
	if r.StatusCode != 401 {
		t.Fatal(r.StatusCode)
	}
	req, _ := http.NewRequest("GET", auth.URL+"/ready", nil)
	req.Header.Set("Authorization", "Bearer test-api-token")
	r, err = auth.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatal(r.StatusCode)
	}
}
func TestAPIRunArtifactAndLogs(t *testing.T) {
	e, s := fixture(t, "")
	w := wf("artifact")
	j := w.Jobs["a"]
	j.Steps = append(j.Steps, workflow.Step{Artifact: &workflow.ArtifactConfig{Name: "app", Path: "app.txt"}})
	w.Jobs["a"] = j
	id := submit(t, s, w)
	if r := wait(t, e, id); r.State != scheduler.Success {
		t.Fatal(r)
	}
	for _, path := range []string{"/api/v1/runs/" + id, "/api/v1/runs/" + id + "/jobs", "/api/v1/runs/" + id + "/artifacts", "/api/v1/runs/" + id + "/logs"} {
		r := get(t, s, path)
		r.Body.Close()
		if r.StatusCode != 200 {
			t.Fatal(path, r.StatusCode)
		}
	}
	resp := get(t, s, "/api/v1/runs/"+id+"/artifacts/app")
	defer resp.Body.Close()
	h := sha256.New()
	_, err := io.Copy(h, resp.Body)
	if err != nil || resp.StatusCode != 200 || hex.EncodeToString(h.Sum(nil)) != resp.Header.Get("X-Checksum-SHA256") {
		t.Fatal(err, resp.StatusCode)
	}
	resp2 := get(t, s, "/api/v1/runs/missing/artifacts/app")
	resp2.Body.Close()
	if resp2.StatusCode != 404 {
		t.Fatal(resp2.StatusCode)
	}
}
func TestAPICancelAndSSE(t *testing.T) {
	e, s := fixture(t, "")
	id := submit(t, s, wf("sleep"))
	resp, err := s.Client().Post(s.URL+"/api/v1/runs/"+id+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || wait(t, e, id).State != scheduler.Cancelled {
		t.Fatal(resp.StatusCode)
	}
	id = submit(t, s, wf("logs"))
	resp = get(t, s, "/api/v1/runs/"+id+"/logs/stream")
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal(resp.Header)
	}
	scan := bufio.NewScanner(resp.Body)
	found := false
	for scan.Scan() {
		if strings.Contains(scan.Text(), "live-event") {
			found = true
			break
		}
	}
	resp.Body.Close()
	if !found {
		t.Fatal("SSE event missing", scan.Err())
	}
	if wait(t, e, id).State != scheduler.Success {
		t.Fatal("run failed")
	}
}
