# DistKV — Live Web Showcase (Raft Visualizer)

**Date:** 2026-05-24
**Status:** Approved
**Repo:** `~/projects/distkv` · module `github.com/NeilP211/distkv` · private GitHub repo

## 1. Purpose

Make DistKV's invisible distributed-systems behavior — leader election, log
replication, partition tolerance, failover — **visible and interactive in a web
browser**. A single self-contained binary hosts a terminal-styled web UI over an
in-process Raft cluster, so anyone can watch heartbeats and vote requests fly
between nodes, kill the leader and watch a new one get elected, partition the
network and watch quorum behavior, and use the cluster as a real key-value store.

The primary deliverable is a **locally-runnable demo** (`make demo-web` →
`localhost:8080`) from which Neil records a GIF + screenshots for the repo
README. This turns the project from "trust my tests" into "watch it happen."

## 2. Goals and non-goals

### Goals
- One command (`make demo-web`) launches one process; open a browser and the
  cluster is already running and visualized.
- Visualize the **real `internal/raft` consensus core** — not a reimplementation.
  Only the network between nodes is the existing `simnet`.
- Four interactive feature areas (all in scope):
  1. **Consensus visualization** — topology with per-node role/term, animated
     RPCs (heartbeats, vote requests, AppendEntries), election animations.
  2. **Fault injection** — kill/restart nodes, partition the network, drop/delay
     messages.
  3. **Client KV operations** — Put/Get/CAS/Del, watch writes replicate + commit,
     read back (linearizable via ReadIndex).
  4. **Log & event timeline** — per-node log entries replicating with the commit
     index advancing, plus a scrolling narrated event log.
- Terminal / ops-console aesthetic (dark, monospace, phosphor green/amber, subtle
  scanline + glow).
- Single binary: all web assets `go:embed`ed; no Node/npm toolchain; no new Go
  dependencies.
- Do **not** modify or risk the verified `internal/chaos` + `internal/lincheck`
  path that backs the 152k-op linearizability headline.
- Full `go test -race ./...` stays green after every milestone.

### Non-goals
- Real multi-process / multi-container cluster orchestration (explicitly chose the
  self-contained simulator over a real-cluster dashboard for demo reliability).
- Hosted/public deployment. Local-first; recordings are the shareable artifact.
- Authentication, multi-tenancy, persistence of the web session across restarts.
- A JS build pipeline, framework, or component library.
- Replacing the existing `cmd/distkv-tui` terminal dashboard; this is additive.

## 3. Architecture overview

```
cmd/distkv-web/                 NEW single binary: HTTP server + embedded web UI
  main.go                       wires websim.Cluster + Hub + HTTP handlers
  web/                          go:embed'd static assets
    index.html                  terminal-console layout (4 panels)
    app.js                      SSE client, Canvas topology renderer, command POSTs
    style.css                   terminal aesthetic (monospace, phosphor, scanlines)

internal/websim/                NEW package
  cluster.go                    N-node simnet cluster (mirrors chaos.Cluster):
                                 Snapshot() of all nodes, per-node log access,
                                 leader-aware Put/Get/CAS/Del, fault-injection
                                 delegations, Reset/rebuild, speed presets.
  events.go                     polls Status @~10Hz, diffs prior→current to emit
                                 semantic events (role/leader/term/commit changes,
                                 crash, restart, partition, heal).
  hub.go                        SSE fan-out: register/unregister browser clients,
                                 broadcast frames, send full snapshot on connect.
  cluster_test.go, events_test.go

internal/simnet/simnet.go       MODIFIED (additive only):
                                 SetObserver(func(MessageEvent)) RPC tap.
```

### 3.1 Why a separate `internal/websim` (not reusing `chaos`)
`internal/chaos.Cluster` is the exact in-process cluster shape needed, but it
backs the verified lincheck headline and its client ops/Status accessors are
unexported. Rather than couple the web feature to that path (and risk the headline
result), `websim` reuses the same ~100-line construction recipe
(`server.NewRaftNode` over `simnet`, shared `MemStorage` per node for
crash-restart) and adds the web-specific surface. Acceptable ~100-line duplication
in exchange for full isolation of the verified path.

### 3.2 The one shared-package change: `simnet.SetObserver`
A single additive method on `*simnet.Network`:

```go
type MessageEvent struct {
    From, To raft.NodeID
    Type     raft.MsgType
    Dropped  bool   // dropped by drop-rate or partition/down
}
func (n *Network) SetObserver(fn func(MessageEvent))
```

Called inside `send` after the routing decision is computed, **without holding the
lock during fn** and **without consuming the seeded RNG for observation** — so
determinism and existing chaos/lincheck tests are unaffected. No-op when unset.

## 4. Components

### 4.1 `websim.Cluster`
- `New(n int, opts) *Cluster` — build & start `n` nodes (default 5) over one
  seeded `simnet.Network`; install the observer tap.
- `Snapshot() ClusterState` — for every node: `{ID, Role, Term, Leader,
  CommitIndex, LastLogIndex, Up bool}`; plus current partition groups and
  drop/delay config. Built from `RaftNode.Status()` + `Node.LogEntries`.
- `NodeLog(id) []LogEntryView` — per-node entries `{Index, Term, Type, Summary}`
  via `Node.LogEntries(first, last)`; `Summary` decodes the command for display.
- `Put/Get/CAS/Del(ctx, ...)` — **leader-aware**: resolve current leader via
  Status hints, issue against it, retry with bounded backoff on `ErrNotLeader`
  (mirrors `pkg/client` behavior in-process); surface leader redirects as events.
- Fault injection: `CrashNode/RecoverNode/RestartNode/Partition/Heal/SetDrop/
  SetDelay` — thin delegations to the simnet (same as chaos).
- `SetSpeed(preset)` — Slow / Normal / Fast. Implemented by rebuilding the cluster
  with a different `TickInterval` (~250 / 150 / 60 ms). Demo default = Normal so
  RPCs are human-watchable.
- `Reset()` — stop and rebuild a fresh cluster (new seed), broadcast new snapshot.

### 4.2 `websim` event deriver (`events.go`)
A goroutine polling `Snapshot()` at ~10Hz, diffing against the previous snapshot to
emit human-readable events, e.g.:
- role change → `"n3 timed out → Candidate (term 5)"`, `"n1 became Leader (term 5)"`
- commit advance → `"commit index → 42"`
- crash/restart/partition/heal/drop/delay changes
- KV outcomes and leader redirects (emitted directly by the Put/Get path)

Events feed the Hub as `event` frames and populate the scrolling UI log.

### 4.3 `websim.Hub`
- Tracks connected SSE clients (channel per client).
- `Broadcast(frame)` fans out to all; slow clients get a bounded buffer and are
  dropped if they back up (a browser tab, not a critical consumer).
- On new connection: immediately send a full `state` snapshot + recent event
  backlog so a fresh/reconnecting client is consistent at once.

### 4.4 `cmd/distkv-web/main.go` (HTTP layer)
- `GET /` and assets → embedded `web/` via `go:embed` + `http.FileServer`.
- `GET /events` → SSE stream (`text/event-stream`, `http.Flusher`); registers a
  Hub client; streams frames until the request context is cancelled.
- `POST /cmd/{action}` → decode JSON body, validate, call the matching Cluster
  method, return `200`/`4xx`. Actions: `crash`, `recover`, `restart`, `partition`,
  `heal`, `drop`, `delay`, `put`, `get`, `cas`, `del`, `speed`, `reset`.
- Frame types streamed over SSE (JSON, tagged by `event:` field):
  `state` (~10Hz snapshot), `rpc` (per-message, for dot animation), `event`
  (narration), `log` (per-node entries, ~2Hz), `kvresult` (op responses).

### 4.5 Frontend (`web/`)
- **Layout (4 panels), terminal aesthetic:**
  - center: **topology Canvas** — nodes on a ring; leader glow/crown; animated
    dots traveling edges colored by RPC type (heartbeat dim, vote amber,
    append-entries green, snapshot magenta); partitions drawn as separated groups
    with greyed cross-links; crashed nodes dimmed with an `✕`.
  - left: **node-state list** (id, role, term, commit, last-log-index).
  - right: **controls** — per-node Kill/Restart, partition builder, drop/delay
    sliders, KV op form (key/value + Put/Get/CAS/Del), speed presets, Reset.
  - bottom: **event log** (scrolling) and a **per-node log viewer** with a
    commit-index marker.
- **`app.js`:** one `EventSource('/events')`; dispatch on frame type to update a
  small client-side state model; `requestAnimationFrame` loop drives the Canvas
  (RPC dots animate over a fixed ~300ms visual travel time, decoupled from the
  simnet's instantaneous synchronous delivery). Buttons `fetch('/cmd/...')`.
- **No framework, no build step.** Plain ES modules in one or two files.

## 5. Data flow

1. Binary starts → `websim.New(5)` builds & starts the cluster; event deriver and
   Hub goroutines start; HTTP server listens on `:8080`.
2. Browser loads `/` then opens `/events`; Hub sends full snapshot + backlog.
3. Cluster runs: ticks drive Raft; every RPC hits the simnet observer → `rpc`
   frame; the deriver diffs Status → `event` frames; periodic `state`/`log` frames.
4. User clicks "Kill n1" → `POST /cmd/crash {id:"n1"}` → `Cluster.CrashNode` →
   next ticks show an election → frames stream it → UI animates the new leader.
5. User submits Put → `POST /cmd/put` → leader-aware propose → `kvresult` +
   `event`; replication visible as AppendEntries dots and advancing commit index.

## 6. Error handling

- **Op against a follower:** leader-aware path redirects/retries; redirect shown
  as an event. Succeeds transparently.
- **No quorum (user partitions away the majority):** writes block then time out
  with a clear `"no quorum / no leader — write cannot commit"` `kvresult`. This is
  surfaced as a **feature/teaching moment**, not an error state.
- **Invalid command** (unknown node id, malformed partition, bad drop rate) →
  `400` with a message; UI shows it in the event log, no cluster mutation.
- **SSE disconnect:** `EventSource` auto-reconnects; Hub re-sends a fresh snapshot
  so the client re-syncs (no incremental replay needed).
- **Slow/backed-up browser client:** dropped from the Hub after its buffer fills;
  it reconnects and re-syncs. Never blocks the cluster.
- **Reset / speed change:** stop nodes cleanly, rebuild, broadcast new snapshot;
  in-flight ops fail fast with a clear message.

## 7. Testing

- `internal/simnet`: a test that `SetObserver` observes the expected
  from/to/type/dropped for delivered, dropped, partitioned, and crashed sends; and
  that enabling an observer does not change routing outcomes vs. the existing
  deterministic tests (determinism preserved).
- `internal/websim/cluster_test.go`: leader elected within timeout; crash the
  leader → a new leader is elected; minority partition → minority side has no
  leader, majority side keeps one; Put then Get round-trips; CAS semantics; log
  grows and commit index advances after a Put.
- `internal/websim/events_test.go`: diffing two snapshots yields the expected
  event set (role change, leader change, commit advance, crash, partition).
- `cmd/distkv-web`: `httptest` coverage of `/cmd/*` validation + success paths and
  SSE frame encoding (one client receives a snapshot then a broadcast frame).
- **Visual/interaction:** verified manually in a real browser (load page, kill
  leader, observe re-election, run a Put/Get) and captured in the demo recording.
  No JS unit harness (keeps the no-Node-toolchain goal).
- Run full `go build ./...` + `go test -race ./...` after each milestone; report
  status.

## 8. Deliverables

- `cmd/distkv-web` binary + embedded `web/` assets.
- `internal/websim` package + `simnet.SetObserver`.
- `make demo-web` target (build + run + print the URL).
- README section ("Live web showcase") with an animated GIF + 2–3 screenshots
  (the GIF/screenshots are produced by Neil from the local demo).
- All committed and pushed to the private GitHub repo (preserving history).

## 9. Open questions / deferred

- Exact node count for the demo (default 5; trivially configurable via a flag).
- Whether to show RequestVote vote tallies on the canvas during an election
  (nice-to-have; include if cheap, otherwise event-log only).
- GIF capture tooling is Neil's choice (e.g., screen recording → GIF); out of
  scope for the code.
