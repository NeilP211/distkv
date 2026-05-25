package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/NeilP211/distkv/internal/websim"
)

// kvTimeout bounds a single browser-initiated KV operation. If the cluster
// cannot reach quorum within this window the op fails with a clear message.
const kvTimeout = 2 * time.Second

// rpcFrame is the "rpc" SSE payload used to animate a message between nodes.
type rpcFrame struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Kind    string `json:"kind"`
	Dropped bool   `json:"dropped"`
}

// logFrame is the "log" SSE payload carrying one node's log entries.
type logFrame struct {
	ID      string                `json:"id"`
	Entries []websim.LogEntryView `json:"entries"`
}

// kvResultFrame is the "kvresult" SSE payload describing a KV op outcome.
type kvResultFrame struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Found bool   `json:"found"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// cmdReq is the superset of fields a /cmd/{action} request may carry.
type cmdReq struct {
	ID     string     `json:"id"`
	Groups [][]string `json:"groups"`
	Rate   float64    `json:"rate"`
	Key    string     `json:"key"`
	Value  string     `json:"value"`
	Expect string     `json:"expect"`
	Speed  string     `json:"speed"`
}

// handleEvents serves the Server-Sent Events stream: it primes the client with
// the latest state and recent events, then streams frames until disconnect.
func (a *app) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := a.hub.Add()
	defer a.hub.Remove(ch)

	// Prime the new client so it is consistent immediately.
	st, evs := a.snapshotForConnect()
	writeFrame(w, flusher, websim.Frame{Event: "state", Data: st})
	for _, ev := range evs {
		writeFrame(w, flusher, websim.Frame{Event: "event", Data: ev})
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case f, ok := <-ch:
			if !ok {
				return
			}
			writeFrame(w, flusher, f)
		}
	}
}

// writeFrame encodes one SSE frame ("event:" + "data:") and flushes it.
func writeFrame(w http.ResponseWriter, flusher http.Flusher, f websim.Frame) {
	data, err := json.Marshal(f.Data)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", f.Event, data)
	flusher.Flush()
}

// handleCmd dispatches POST /cmd/{action} to the matching cluster operation.
func (a *app) handleCmd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/cmd/")

	var req cmdReq
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req) // empty body is fine for heal/reset
	}

	switch action {
	case "crash", "recover", "restart":
		if !a.knownID(req.ID) {
			badRequest(w, "unknown node id: "+req.ID)
			return
		}
		switch action {
		case "crash":
			a.cluster.CrashNode(req.ID)
		case "recover":
			a.cluster.RecoverNode(req.ID)
		case "restart":
			a.cluster.RestartNode(req.ID)
		}
		ok(w)

	case "partition":
		for _, g := range req.Groups {
			for _, id := range g {
				if !a.knownID(id) {
					badRequest(w, "unknown node id in partition: "+id)
					return
				}
			}
		}
		a.cluster.Partition(req.Groups)
		ok(w)

	case "heal":
		a.cluster.Heal()
		ok(w)

	case "drop":
		a.cluster.SetDrop(req.Rate)
		ok(w)

	case "speed":
		a.cluster.SetSpeed(websim.ParseSpeed(req.Speed))
		ok(w)

	case "reset":
		a.cluster.Reset()
		ok(w)

	case "put", "get", "del", "cas":
		a.handleKV(w, action, req)

	default:
		badRequest(w, "unknown action: "+action)
	}
}

// handleKV runs a KV operation, broadcasts a kvresult frame, and returns JSON.
func (a *app) handleKV(w http.ResponseWriter, action string, req cmdReq) {
	if req.Key == "" {
		badRequest(w, "key required")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	defer cancel()

	res := kvResultFrame{Op: action, Key: req.Key}
	switch action {
	case "put":
		v, err := a.cluster.Put(ctx, req.Key, req.Value)
		fill(&res, v, true, err)
	case "del":
		_, err := a.cluster.Delete(ctx, req.Key)
		fill(&res, "", true, err)
	case "cas":
		v, err := a.cluster.CAS(ctx, req.Key, req.Expect, req.Value)
		fill(&res, v, true, err)
	case "get":
		v, found, err := a.cluster.Get(ctx, req.Key)
		fill(&res, v, found, err)
	}

	a.hub.Broadcast(websim.Frame{Event: "kvresult", Data: res})
	writeJSON(w, res)
}

// fill populates the value/found/ok/error fields of a kvResultFrame.
func fill(res *kvResultFrame, value string, found bool, err error) {
	if err != nil {
		res.OK = false
		res.Error = err.Error()
		return
	}
	res.OK = true
	res.Value = value
	res.Found = found
}

// knownID reports whether id is one of the cluster's node ids.
func (a *app) knownID(id string) bool {
	for _, x := range a.ids {
		if x == id {
			return true
		}
	}
	return false
}

func ok(w http.ResponseWriter) { writeJSON(w, map[string]bool{"ok": true}) }

func badRequest(w http.ResponseWriter, msg string) {
	w.WriteHeader(http.StatusBadRequest)
	writeJSON(w, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
