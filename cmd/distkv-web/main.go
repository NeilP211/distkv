// Command distkv-web serves the DistKV live web showcase: a single binary that
// runs an in-process Raft cluster (internal/websim) and a terminal-styled web
// UI over it. Open the printed URL in a browser to watch elections and
// replication, inject faults, and run key-value operations live.
package main

import (
	"embed"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/simnet"
	"github.com/NeilP211/distkv/internal/websim"
)

//go:embed web
var webFS embed.FS

// statePeriod is how often the cluster snapshot is broadcast; logPeriod (a
// multiple of it) throttles the heavier per-node log frames.
const (
	statePeriod = 100 * time.Millisecond
	logEvery    = 5 // every 5th state tick → ~500ms
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	nodes := flag.Int("nodes", 5, "number of cluster nodes")
	seed := flag.Int64("seed", 1, "simnet seed")
	flag.Parse()

	a := newApp(*nodes, *seed)
	go a.pump()

	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("embed web assets: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", a.handleEvents)
	mux.HandleFunc("/cmd/", a.handleCmd)
	mux.Handle("/", http.FileServer(http.FS(sub)))

	log.Printf("DistKV web showcase → http://localhost%s  (%d nodes)", *addr, *nodes)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

// app holds the running cluster, the SSE hub, and the latest state plus a small
// ring of recent events for replay to freshly-connected browsers.
type app struct {
	cluster *websim.Cluster
	hub     *websim.Hub
	ids     []string

	mu        sync.Mutex
	lastState websim.ClusterState
	recent    []websim.Event
	seq       int
}

const recentCap = 60

func newApp(nodes int, seed int64) *app {
	a := &app{hub: websim.NewHub()}
	obs := func(ev simnet.MessageEvent) {
		a.hub.Broadcast(websim.Frame{Event: "rpc", Data: rpcFrame{
			From: string(ev.From), To: string(ev.To), Kind: rpcKind(ev.Type), Dropped: ev.Dropped,
		}})
	}
	a.cluster = websim.New(nodes, seed, websim.SpeedNormal, obs)
	a.ids = a.cluster.IDs()
	a.lastState = a.cluster.Snapshot()
	return a
}

// pump periodically snapshots the cluster, broadcasts the state, derives and
// broadcasts narration events, and (less often) broadcasts per-node logs.
func (a *app) pump() {
	t := time.NewTicker(statePeriod)
	defer t.Stop()
	tick := 0
	for range t.C {
		tick++
		cur := a.cluster.Snapshot()

		a.mu.Lock()
		prev := a.lastState
		a.lastState = cur
		a.mu.Unlock()

		a.hub.Broadcast(websim.Frame{Event: "state", Data: cur})

		for _, ev := range websim.DiffEvents(prev, cur) {
			a.mu.Lock()
			a.seq++
			ev.Seq = a.seq
			a.recent = append(a.recent, ev)
			if len(a.recent) > recentCap {
				a.recent = a.recent[len(a.recent)-recentCap:]
			}
			a.mu.Unlock()
			a.hub.Broadcast(websim.Frame{Event: "event", Data: ev})
		}

		if tick%logEvery == 0 {
			for _, id := range a.ids {
				a.hub.Broadcast(websim.Frame{Event: "log", Data: logFrame{
					ID: id, Entries: a.cluster.NodeLog(id),
				}})
			}
		}
	}
}

// snapshotForConnect returns the latest state and a copy of the recent events,
// used to prime a freshly-connected browser.
func (a *app) snapshotForConnect() (websim.ClusterState, []websim.Event) {
	a.mu.Lock()
	defer a.mu.Unlock()
	evs := make([]websim.Event, len(a.recent))
	copy(evs, a.recent)
	return a.lastState, evs
}

// rpcKind maps a Raft message type to a short kind the front-end colors by.
func rpcKind(t raft.MsgType) string {
	switch t {
	case raft.MsgRequestVote:
		return "vote"
	case raft.MsgRequestVoteResp:
		return "voteResp"
	case raft.MsgAppendEntries:
		return "append"
	case raft.MsgAppendEntriesResp:
		return "appendResp"
	case raft.MsgInstallSnapshot:
		return "snapshot"
	case raft.MsgInstallSnapshotResp:
		return "snapshotResp"
	default:
		return "other"
	}
}
