package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestServer builds an app over a 5-node cluster and an httptest server for
// its handlers, registering cleanup. The background pump is not started; tests
// drive the handlers directly.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	a := newApp(5, 1)
	t.Cleanup(a.cluster.Stop)
	mux := http.NewServeMux()
	mux.HandleFunc("/events", a.handleEvents)
	mux.HandleFunc("/cmd/", a.handleCmd)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url string, body any) (map[string]any, int) {
	t.Helper()
	b, _ := json.Marshal(body)
	r, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer r.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(r.Body).Decode(&m)
	return m, r.StatusCode
}

func TestCmdPutThenGet(t *testing.T) {
	srv := newTestServer(t)
	// Speed up elections so the bounded KV ops comfortably find a leader.
	post(t, srv.URL+"/cmd/speed", map[string]string{"speed": "fast"})

	res, code := post(t, srv.URL+"/cmd/put", map[string]string{"key": "a", "value": "1"})
	if code != 200 || res["ok"] != true {
		t.Fatalf("put: code=%d res=%v", code, res)
	}

	res, code = post(t, srv.URL+"/cmd/get", map[string]string{"key": "a"})
	if code != 200 || res["ok"] != true {
		t.Fatalf("get: code=%d res=%v", code, res)
	}
	if res["found"] != true || res["value"] != "1" {
		t.Fatalf("get a = %v, want value=1 found=true", res)
	}
}

func TestCmdUnknownNode(t *testing.T) {
	srv := newTestServer(t)
	_, code := post(t, srv.URL+"/cmd/crash", map[string]string{"id": "bogus"})
	if code != http.StatusBadRequest {
		t.Fatalf("crash bogus: code=%d, want 400", code)
	}
}

func TestCmdUnknownAction(t *testing.T) {
	srv := newTestServer(t)
	_, code := post(t, srv.URL+"/cmd/frobnicate", map[string]string{})
	if code != http.StatusBadRequest {
		t.Fatalf("unknown action: code=%d, want 400", code)
	}
}

func TestEventsStreamsState(t *testing.T) {
	srv := newTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer resp.Body.Close()

	// Parse SSE frames line-by-line until we see the primed "state" frame.
	sc := bufio.NewScanner(resp.Body)
	var lastEvent string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event:"):
			lastEvent = strings.TrimSpace(line[len("event:"):])
		case strings.HasPrefix(line, "data:") && lastEvent == "state":
			var st struct {
				Nodes []struct {
					ID string `json:"id"`
				} `json:"nodes"`
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(line[len("data:"):])), &st); err != nil {
				t.Fatalf("decode state: %v", err)
			}
			if len(st.Nodes) != 5 {
				t.Fatalf("state has %d nodes, want 5", len(st.Nodes))
			}
			return // success
		}
	}
	t.Fatalf("never received a state frame (scanner err: %v)", sc.Err())
}
