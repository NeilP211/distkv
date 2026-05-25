# DistKV Live Web Showcase — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A single self-contained binary (`cmd/distkv-web`) that hosts a terminal-styled web UI over an in-process Raft cluster, letting anyone watch elections/replication, inject faults, and run KV ops live in the browser.

**Architecture:** A new `internal/websim` package mirrors `internal/chaos.Cluster` (the verified lincheck path stays untouched) and adds an all-node `Snapshot()`, per-node log access, leader-aware Put/Get/CAS/Del, speed presets, an event deriver, and an SSE hub. `internal/simnet` gains one additive `SetObserver` RPC tap. The browser talks to the server over Server-Sent Events (state/rpc/event/log/kvresult frames) + REST POST commands. Frontend is vanilla HTML/CSS/JS + Canvas, `go:embed`ed — no Node toolchain, no new Go deps.

**Tech Stack:** Go (stdlib `net/http`, `go:embed`), existing `internal/raft` + `internal/server` + `internal/simnet`, vanilla JS + Canvas + SSE (`EventSource`).

Spec: `docs/superpowers/specs/2026-05-24-distkv-web-showcase-design.md`

---

## Key existing APIs (verified)

- `simnet.NewNetwork(seed)`, `.Register(id, rn.Step)`, `.Node(id) transport.Transport`, `.Partition(groups...)`, `.Heal()`, `.SetDrop(rate)`, `.SetDelay(min,max)`, `.Crash(id)`, `.Recover(id)`. `send` is the routing chokepoint.
- `server.NewRaftNode(server.RaftNodeConfig{Raft: raft.Config{...}, TickInterval})`, `.Start()`, `.Stop()`, `.Step(msg)`, `.Status() server.Status{ID,Role,Term,Leader,CommitIndex,Members}`, `.Propose(ctx, store.Command) (string,error)`, `.LinearizableGet(ctx, key) (string,bool,error)`.
- `raft.Config{ID, Peers, Storage, Transport, ElectionTimeoutMin, ElectionTimeoutMax, HeartbeatInterval}`. `raft.NewMemStorage()`. `raft.ErrNotLeader`.
- `raft.Node.LogEntries(lo, hi)` is half-open `[lo,hi)`; `.FirstIndex()`, `.CommitIndex()`. **No exported `LastIndex()` — Task 0 adds one.**
- `store.Command{Op (OpPut|OpDelete|OpCAS), Key, Value, ExpectValue}`. `server.LinearizableGet` returns `raft.ErrNotLeader` off-leader, `server.ErrLeadershipLost` on lost quorum.
- `raft.MsgType`: `MsgRequestVote`, `MsgRequestVoteResp`, `MsgAppendEntries`, `MsgAppendEntriesResp`, `MsgInstallSnapshot`, `MsgInstallSnapshotResp` (has `.String()`).

---

## Task 0: Add `raft.Node.LastIndex()` accessor

**Files:**
- Modify: `internal/raft/node.go` (add method)
- Test: `internal/raft/node_test.go` (add test)

- [ ] **Step 1: Add the accessor** next to other read-only accessors in `node.go`:

```go
// LastIndex returns the index of the last entry in the log (0 if empty).
// Read-only; safe for concurrent use.
func (n *Node) LastIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.lastIndex()
}
```

- [ ] **Step 2: Test** — append entries and assert `LastIndex()` matches. Reuse existing node_test helpers (a freshly-constructed node has `LastIndex()==0`; after `Propose` on a single-node leader, `LastIndex()>=1`).
- [ ] **Step 3:** `go test ./internal/raft/ -run LastIndex -v` → PASS.
- [ ] **Step 4: Commit** `feat(raft): export Node.LastIndex accessor`.

---

## Task 1: simnet observer tap (`SetObserver`)

**Files:**
- Modify: `internal/simnet/simnet.go`
- Test: `internal/simnet/simnet_test.go`

- [ ] **Step 1: Write failing test** — register an observer, send through a delivered path, a dropped (drop=1.0) path, and a down/partitioned path; assert the observer records the right `MessageEvent{From,To,Type,Dropped}` for each, and that an unset observer is a safe no-op.

- [ ] **Step 2: Add types + setter + tap.** Add to `Network`:

```go
// MessageEvent describes one attempted send for observability (web UI tap).
type MessageEvent struct {
	From, To raft.NodeID
	Type     raft.MsgType
	Dropped  bool // dropped by drop-rate, partition, or a down endpoint
}

// observer field on Network (guarded by mu):
//   observer func(MessageEvent)

// SetObserver installs (or clears, with nil) a callback invoked once per
// attempted send. The callback runs WITHOUT n.mu held and is never on the
// RNG path, so it cannot affect routing or determinism.
func (n *Network) SetObserver(fn func(MessageEvent)) {
	n.mu.Lock()
	n.observer = fn
	n.mu.Unlock()
}
```

In `send`, compute the existing `dropped`/unreachable decision exactly as today, capture `obs := n.observer` under the lock, then AFTER releasing the lock (and before/after delay is fine) call `obs(MessageEvent{from,to,msg.Type, dropped||unreachable})` if non-nil. Down/partition return `ErrUnreachable`; emit those as `Dropped:true` too. **Do not** call the RNG for observation.

- [ ] **Step 3:** `go test ./internal/simnet/ -v` → PASS (new test + all existing pass, determinism intact).
- [ ] **Step 4: Commit** `feat(simnet): additive SetObserver RPC tap`.

---

## Task 2: `websim.Cluster` construction + Snapshot + faults

**Files:**
- Create: `internal/websim/cluster.go`
- Test: `internal/websim/cluster_test.go`

Mirror `chaos.Cluster` construction (per-node `MemStorage` retained for restart). Speed presets set `TickInterval`. Election timeouts: min 6, max 12 ticks, heartbeat 2 ticks (same ratios as chaos).

- [ ] **Step 1: Types + New + Snapshot + Stop.**

```go
package websim

type Speed int
const (SpeedNormal Speed = iota; SpeedSlow; SpeedFast)

func (s Speed) tick() time.Duration {
	switch s { case SpeedSlow: return 250*time.Millisecond
	case SpeedFast: return 60*time.Millisecond
	default: return 150*time.Millisecond }
}

type NodeState struct {
	ID string `json:"id"`; Role string `json:"role"`; Term uint64 `json:"term"`
	Leader string `json:"leader"`; Commit uint64 `json:"commit"`
	LastIndex uint64 `json:"lastIndex"`; Up bool `json:"up"`
}
type ClusterState struct {
	Nodes []NodeState `json:"nodes"`
	Partitions [][]string `json:"partitions"` // nil = fully connected
	Drop float64 `json:"drop"`; Speed string `json:"speed"`
}

type node struct { rn *server.RaftNode; storage *raft.MemStorage }

type Cluster struct {
	net *simnet.Network
	ids []raft.NodeID
	mu sync.Mutex
	nodes map[raft.NodeID]*node
	down map[raft.NodeID]bool       // mirror of simnet down for Snapshot
	parts [][]raft.NodeID; drop float64; speed Speed
	obs func(simnet.MessageEvent)   // forwarded RPC tap
}

func New(n int, seed int64, speed Speed, obs func(simnet.MessageEvent)) *Cluster { ... }
func (c *Cluster) buildNode(id, st) *server.RaftNode { ... } // same as chaos.buildNode but TickInterval=c.speed.tick()
func (c *Cluster) Snapshot() ClusterState { ... }            // per node: Status() + node.LastIndex(); Up = !down[id]
func (c *Cluster) Stop() { ... }
func (c *Cluster) IDs() []string { ... }
```

`New` installs the observer via `net.SetObserver(obs)`.

- [ ] **Step 2: Fault injection** (track state for Snapshot, then delegate):

```go
func (c *Cluster) CrashNode(id string)   // set down[id]=true; net.Crash
func (c *Cluster) RecoverNode(id string) // down[id]=false; net.Recover
func (c *Cluster) RestartNode(id string) // stop+rebuild over same storage; re-register; start (down stays false)
func (c *Cluster) Partition(groups [][]string) // store parts; net.Partition
func (c *Cluster) Heal()                  // parts=nil; net.Heal
func (c *Cluster) SetDrop(rate float64)   // drop=rate; net.SetDrop
```

- [ ] **Step 3: Tests** (reuse chaos test patterns; helper `waitLeader(t,c,timeout)`):
  - leader elected within 2s
  - crash the leader → a *different* leader elected within 2s
  - partition `[[n1,n2],[n3,n4,n5]]` → minority side ({n1,n2}) has no Leader, majority elects one
  - `Snapshot()` reports correct `Up` after crash/recover

- [ ] **Step 4:** `go test ./internal/websim/ -race -v` → PASS.
- [ ] **Step 5: Commit** `feat(websim): cluster construction, snapshot, fault injection`.

---

## Task 3: `websim` KV operations (leader-aware) + per-node log

**Files:**
- Modify: `internal/websim/cluster.go`
- Test: `internal/websim/cluster_test.go`

- [ ] **Step 1: Leader-aware ops + log view.** Mirror chaos `proposeOn/getOn` but resolve the leader and retry with bounded backoff so callers can target the cluster, not a specific node.

```go
type LogEntryView struct {
	Index uint64 `json:"index"`; Term uint64 `json:"term"`
	Kind string `json:"kind"`; Summary string `json:"summary"` // "PUT k=v", "DEL k", "CAS ...", "noop", "conf"
}

func (c *Cluster) leader() (raft.NodeID, bool)   // scan Status: Role==Leader && Leader==id
func (c *Cluster) Put(ctx, key, val string) (string, error)
func (c *Cluster) Delete(ctx, key string) (string, error)
func (c *Cluster) CAS(ctx, key, expect, val string) (string, error)
func (c *Cluster) Get(ctx, key string) (string, bool, error) // LinearizableGet on leader
func (c *Cluster) NodeLog(id string) []LogEntryView          // node.LogEntries(First, Last+1) → views (decode store.Command for Summary)
```

Propose/Get loop: find leader; if none, sleep 20ms and retry until ctx deadline; on `raft.ErrNotLeader`/`ErrLeadershipLost` re-resolve leader and retry. Callers pass a ctx with a ~2s timeout; on deadline return a clear error (surfaced to UI as "no quorum / no leader").

- [ ] **Step 2: Tests:**
  - `Put("a","1")` then `Get("a")` → `"1", true`
  - `Get` of missing key → `"", false`
  - `CAS("a","1","2")` succeeds; `CAS("a","wrong","3")` returns mismatch error
  - after a `Put`, the leader's `NodeLog` contains a `PUT a=1` entry and `Snapshot` commit advanced
  - Put with the majority partitioned away from a target → returns error within the timeout (no panic)

- [ ] **Step 3:** `go test ./internal/websim/ -race -v` → PASS.
- [ ] **Step 4: Commit** `feat(websim): leader-aware KV ops and log views`.

---

## Task 4: `websim` event deriver + speed/reset

**Files:**
- Create: `internal/websim/events.go`
- Modify: `internal/websim/cluster.go` (SetSpeed, Reset)
- Test: `internal/websim/events_test.go`

- [ ] **Step 1: Event type + diff (pure function, easy to test).**

```go
type Event struct {
	Seq int `json:"seq"`; Kind string `json:"kind"`; Text string `json:"text"`
}

// DiffEvents compares prev→cur snapshots and returns human-readable events:
//   role change:   "n3 → Candidate (term 5)" / "n1 became Leader (term 5)"
//   leader change: "leader is now n1"
//   commit advance:"n1 commit → 42"  (leader only, to limit noise)
//   up/down:       "n2 crashed" / "n2 recovered"
func DiffEvents(prev, cur ClusterState) []Event { ... } // Seq filled by caller
```

- [ ] **Step 2: SetSpeed + Reset on Cluster** (rebuild nodes; re-install observer; preserve `down`=false, `parts`=nil after reset; SetSpeed rebuilds each node with new TickInterval over same storage).

- [ ] **Step 3: Tests** for `DiffEvents`:
  - follower→candidate→leader transitions produce the expected texts
  - a crash (Up true→false) yields `"n2 crashed"`
  - identical snapshots yield no events

- [ ] **Step 4:** `go test ./internal/websim/ -race -v` → PASS.
- [ ] **Step 5: Commit** `feat(websim): event deriver, speed presets, reset`.

---

## Task 5: SSE hub

**Files:**
- Create: `internal/websim/hub.go`
- Test: `internal/websim/hub_test.go`

- [ ] **Step 1: Hub.**

```go
type Frame struct { Event string; Data any } // Event → SSE "event:" line; Data → JSON payload

type Hub struct {
	mu sync.Mutex
	clients map[chan Frame]struct{}
}
func NewHub() *Hub
func (h *Hub) Add() chan Frame      // buffered (e.g. 256)
func (h *Hub) Remove(ch chan Frame)
func (h *Hub) Broadcast(f Frame)    // non-blocking; drop frame for a full client
func (h *Hub) Count() int
```

- [ ] **Step 2: Tests:** Add a client, Broadcast, receive the frame; a full client doesn't block Broadcast; Remove closes/forgets the channel.
- [ ] **Step 3:** `go test ./internal/websim/ -race -v` → PASS.
- [ ] **Step 4: Commit** `feat(websim): SSE hub fan-out`.

---

## Task 6: `cmd/distkv-web` HTTP server + wiring

**Files:**
- Create: `cmd/distkv-web/main.go`
- Create: `cmd/distkv-web/server.go` (handlers; keep main thin)
- Test: `cmd/distkv-web/server_test.go`

- [ ] **Step 1: App wiring.** `main`: parse flags (`-addr :8080`, `-nodes 5`, `-seed`); build `Hub`; build `websim.New(nodes, seed, SpeedNormal, obs)` where `obs` converts `simnet.MessageEvent`→`rpc` Frame and broadcasts; start two background goroutines: (a) state pump — every 100ms `Broadcast state=Snapshot()`, run `DiffEvents` vs last snapshot and `Broadcast event` for each (assign incrementing Seq), and every 500ms `Broadcast log` per node; serve `/`.

- [ ] **Step 2: Handlers (`server.go`).**
  - `GET /events`: set SSE headers, `flusher,_ := w.(http.Flusher)`, `ch := hub.Add()`, immediately write a `state` frame (snapshot) + recent events; loop writing frames from `ch` (`event: <name>\ndata: <json>\n\n` + flush) until `r.Context().Done()`; `defer hub.Remove(ch)`.
  - `POST /cmd/{action}`: route on path; decode JSON; call cluster method; `200` or `400` w/ message. Actions: `crash`,`recover`,`restart` `{id}`; `partition` `{groups:[[..]]}`; `heal`; `drop` `{rate}`; `put` `{key,value}`; `get` `{key}`; `del` `{key}`; `cas` `{key,expect,value}`; `speed` `{speed:"slow|normal|fast"}`; `reset`. KV ops use a 2s ctx; results broadcast as `kvresult` and returned in the response body.
  - `GET /` + assets: `http.FileServer(http.FS(sub))` over the embedded `web/` dir.

- [ ] **Step 3: Embed.** `//go:embed web/*` `var webFS embed.FS`; `sub,_ := fs.Sub(webFS, "web")`.

- [ ] **Step 4: Tests (`httptest`):**
  - `POST /cmd/put {key:"a",value:"1"}` → 200; then `POST /cmd/get {key:"a"}` → body contains `"1"` (allow a short retry for election).
  - `POST /cmd/crash {id:"bogus"}` → 400.
  - `GET /events` with a short-deadline ctx receives a `state` frame containing 5 nodes.

- [ ] **Step 5:** `go build ./... && go test ./cmd/distkv-web/ -race -v` → PASS.
- [ ] **Step 6: Commit** `feat(distkv-web): HTTP server, SSE stream, command API`.

---

## Task 7: Frontend — terminal-console UI

**Files:**
- Create: `cmd/distkv-web/web/index.html`
- Create: `cmd/distkv-web/web/style.css`
- Create: `cmd/distkv-web/web/app.js`

No automated JS tests (no Node toolchain by design); verified in-browser.

- [ ] **Step 1: `index.html`** — 4-panel grid: header (title + cluster summary + speed/reset), left node-state list, center `<canvas id="topo">`, right controls (per-node Kill/Restart buttons rendered from state, partition builder, drop slider, KV form), bottom split event-log + per-node log viewer. Load `app.js` as a module.

- [ ] **Step 2: `style.css`** — terminal aesthetic: near-black bg `#0a0e0a`, phosphor green `#5fff87` primary, amber `#ffcf5f` accents, magenta for snapshots; monospace (`ui-monospace, "JetBrains Mono", Menlo, monospace`); subtle scanline overlay (`repeating-linear-gradient`) + text-shadow glow on headings; bordered panels.

- [ ] **Step 3: `app.js`** —
  - `const es = new EventSource('/events')`; `es.addEventListener('state'|'rpc'|'event'|'log'|'kvresult', ...)`.
  - State model: latest `ClusterState`, ring layout positions per node (computed from node count), pending RPC animations `[{from,to,type,t0}]`, event log lines, per-node logs.
  - `requestAnimationFrame` render loop: draw edges, draw nodes (color by role: leader bright-green w/ glow + "LEADER", candidate amber, follower dim-green, down grey w/ ✕), draw in-flight RPC dots interpolated over ~300ms by type color, draw partition groups as separated clusters / greyed cross-links.
  - On `rpc` frame push an animation; on `state` reconcile node positions/roles; on `event` prepend to event log (cap ~200 lines); on `log` update the log viewer (highlight ≤ commit index).
  - Controls call `fetch('/cmd/<action>', {method:'POST', body: JSON.stringify(...)})`; KV form shows the `kvresult`.
  - `EventSource` auto-reconnects; on reconnect the server resends a `state` snapshot.

- [ ] **Step 4: Manual verify** with agent-browser: launch the binary, open the page, confirm a leader appears, click "Kill leader", confirm re-election animates, run a Put then Get. Screenshot.

- [ ] **Step 5: Commit** `feat(distkv-web): terminal-console web UI (canvas topology + controls)`.

---

## Task 8: Makefile target, README, full verification, integrate

**Files:**
- Modify: `Makefile`
- Modify: `README.md`

- [ ] **Step 1: Makefile** — add:

```make
demo-web:
	go build -o bin/distkv-web ./cmd/distkv-web
	@echo "DistKV web showcase → http://localhost:8080"
	bin/distkv-web
```

Add `demo-web` to `.PHONY`.

- [ ] **Step 2: README** — new "Live web showcase" section: what it shows, `make demo-web`, the four panels, the "kill the leader, watch re-election" money shot. Leave placeholders for a GIF + screenshots (Neil records from the local demo).

- [ ] **Step 3: Full verification:** `go build ./... && go vet ./... && go test -race ./...` → all PASS. Record status.

- [ ] **Step 4: Commit** `docs: web showcase Makefile target and README section`.

- [ ] **Step 5: Integrate** — push `web-showcase`; open a PR to `main` (or fast-forward merge), preserving history. Report the branch/PR link.

---

## Self-review notes
- Spec §3 features → Tasks 2–3 (consensus state + KV), Task 1 + Task 7 (RPC animation), Tasks 2/4 (fault injection + events), Task 3 (logs). All four covered.
- No new Go deps; SSE + `go:embed` are stdlib. ✓
- `internal/chaos` untouched; only additive `simnet.SetObserver` + `raft.Node.LastIndex`. ✓
- Type names consistent across tasks: `ClusterState`, `NodeState`, `LogEntryView`, `Event`, `Frame`, `MessageEvent`, `Speed`.
