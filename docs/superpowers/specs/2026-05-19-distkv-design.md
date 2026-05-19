# DistKV — Raft-Based Distributed Key-Value Store

**Date:** 2026-05-19
**Status:** Approved
**Repo:** `~/projects/distkv` · module `github.com/NeilP211/distkv` · private GitHub repo

## 1. Purpose

A replicated, fault-tolerant key-value store built to *understand* distributed
systems rather than consume them. Multiple nodes form a cluster, elect a leader
via Raft consensus, replicate writes durably, survive node crashes and minority
network partitions without data loss, and serve linearizable reads/writes
through any node.

Portfolio project (Project 1 of a numbered series). The README tells the build
honestly — every performance number is measured on a real local cluster, never
aspirational.

## 2. Goals and non-goals

### Goals
- Raft consensus implemented from scratch from the paper (no consensus libraries).
- Durability: committed writes survive any single-node crash with zero data loss.
- Availability: cluster serves requests while a majority is alive; survives a
  minority-side network partition.
- Linearizable reads and writes through any node.
- Verifiable correctness: deterministic chaos testing + linearizability checking
  over 100k+ randomized operations.
- Honest, reproducible benchmarks.
- Production-shaped ops: Docker, Kubernetes manifests, Helm chart, CI, Prometheus
  metrics, Grafana dashboard.

### Non-goals
- Live cloud deployment (GKE/DigitalOcean). Manifests and Helm chart are provided
  and statically validated, but no live cluster is stood up; the README will not
  claim a live deployment.
- Multi-Raft / sharding. Single Raft group only.
- Transactions across multiple keys beyond single-key compare-and-swap.
- Authentication / TLS / multi-tenancy.

## 3. Architecture

The system decomposes into isolated units, each with one responsibility, a
well-defined interface, and independent tests.

| Package | Responsibility | Depends on |
|---|---|---|
| `internal/wal` | Single-node append-only WAL: checksummed records, replay, crash recovery | — |
| `internal/store` | KV state machine: in-memory map + snapshot/restore + CAS | — |
| `internal/raft` | Raft core: roles, election, log replication, commit/apply, snapshots, joint-consensus membership. Pure logic; storage + transport injected | `raftstore`, `transport` (interfaces) |
| `internal/raftstore` | Raft log + `HardState` + snapshot persistence in bbolt | `raft` (types) |
| `internal/transport` | RPC transport interface; gRPC + simnet implementations | `proto` |
| `internal/simnet` | In-process network simulator: partition / drop / delay / reorder | `transport` |
| `internal/proto` (`api/`) | Protobuf: internal Raft RPC + client KV service | — |
| `internal/server` | Wires raft+store+gRPC; leader forwarding, ReadIndex linearizable reads | all above |
| `pkg/client` | Go client library: leader discovery, retry, redirect-follow | `proto` |
| `internal/lincheck` | Linearizability verifier (Wing-Gong-Liu algorithm) over op histories | — |
| `internal/chaos` | Chaos harness: simnet faults + crash/restart injection | `simnet`, `raft`, `lincheck` |
| `internal/bench` | Benchmark suite: throughput, P50/P95/P99, recovery time | `client` |
| `internal/metrics` | Prometheus instrumentation | — |
| `cmd/distkvd` | Node daemon | `server` |
| `cmd/distkvctl` | CLI client | `client` |
| `cmd/distkv-tui` | tview live cluster dashboard | `client` |

### Key design decisions

- **Two storage stories, deliberately.** The single-node KV phase uses a raw
  append-only WAL file (records: length + CRC32 + payload) to tell the
  crash-recovery narrative concretely. Once Raft is in, the Raft log + HardState +
  snapshots persist in bbolt (`raftstore`). The state machine (`store`) is
  in-memory, rebuilt from the latest snapshot + replayed log on restart.

- **Transport is an interface.** `raft` depends on a `Transport` interface, not
  gRPC. Two implementations: `grpcTransport` (production) and `simnet`
  (in-process, fault-injecting, seeded for determinism). Chaos and linearizability
  tests run entirely in-process — no real sockets, no flaky timing. The exact same
  Raft code runs under both.

- **Storage is an interface.** `raft` depends on a `Storage` interface
  (`raftstore` is the bbolt implementation; an in-memory implementation backs
  fast unit tests).

- **Linearizable reads via ReadIndex.** A read does not append a log entry. The
  leader records its current commit index, confirms it still holds leadership via
  a heartbeat round to a majority, waits for its apply index to catch up to that
  recorded index, then serves. Stale leaders are rejected.

- **Leader routing.** Any node accepts client requests. Followers reply with a
  `NotLeader` error carrying the current leader hint; the client library follows
  the redirect. `distkvd` can optionally forward instead of redirecting (config
  flag); default is redirect.

- **Linearizability checked for real.** `lincheck` implements the Wing-Gong-Liu
  linearizability-checking algorithm. Every client operation in the chaos suite is
  recorded with call and return timestamps; the resulting history is verified.

## 4. Data flow

### Write path
1. Client sends `Put`/`Delete`/`CAS` to any node.
2. Non-leader returns `NotLeader{leaderHint}`; client retries against the leader.
3. Leader appends the command to its Raft log, persists via `raftstore`,
   replicates via `AppendEntries`.
4. On majority acknowledgement the entry is committed; the leader advances
   `commitIndex`.
5. Committed entries apply to the `store` state machine in log order.
6. Leader responds to the client after the entry is applied.

### Read path (linearizable)
1. Client sends `Get` to the leader.
2. Leader captures `readIndex = commitIndex`, runs a heartbeat round to confirm
   leadership with a majority.
3. Leader waits until `appliedIndex >= readIndex`, then reads from `store`.

### Recovery path
1. On start, `raftstore` loads `HardState` (currentTerm, votedFor) and the latest
   snapshot.
2. `store` restores from the snapshot.
3. Persisted log entries after the snapshot replay into `store`.
4. Node joins as a follower and catches up via `AppendEntries` / `InstallSnapshot`.

## 5. Error handling

- **Crash mid-write:** WAL/Raft-log records are checksummed; a torn trailing
  record is detected and truncated on replay. Only fully-persisted, committed
  entries are applied.
- **Leader crash:** followers time out and start an election; a new leader is
  elected within the election-timeout window. Target: sub-2s failover.
- **Network partition:** the majority side keeps a leader and serves; the minority
  side cannot commit (no majority) and rejects writes. On heal, minority nodes
  reconcile via log repair / `InstallSnapshot`.
- **Stale leader:** ReadIndex heartbeat round fails to reach a majority → read is
  rejected; the deposed leader steps down on seeing a higher term.
- **Client-visible errors:** `NotLeader`, `Timeout`, `Unavailable` — all
  retryable by the client library with leader rediscovery and bounded backoff.

## 6. Testing strategy

- **Unit:** Go `testing` + `testify` per package.
- **Property-based:** `pgregory.net/rapid` — WAL append/replay round-trips,
  Raft log-matching and election-safety invariants.
- **Chaos:** deterministic, `simnet`-seeded scenarios — leader crash, follower
  crash, symmetric and asymmetric partitions, message drop/delay/reorder,
  rolling restarts.
- **Linearizability:** `lincheck` verifies recorded histories from chaos runs;
  target 100k+ randomized ops verified linearizable.
- **CI:** GitHub Actions runs `go test ./...`, `golangci-lint`, builds and pushes
  the container image to GHCR.

## 7. Observability and ops

- **Metrics (Prometheus):** request rate, leader-election count, log-replication
  lag, commit latency histogram, apply latency, current role/term per node.
- **Grafana:** dashboard JSON committed to the repo with pre-built panels.
- **Docker:** multi-stage build, minimal final image.
- **Kubernetes:** StatefulSet (stable network identity + per-node PVC), headless
  Service, ConfigMap. Statically validated; not deployed live.
- **Helm:** `helm/distkv` chart parameterizing replica count, image, resources.
- **TUI:** `distkv-tui` shows live per-node role, term, commit index, log lag.

## 8. Success criteria

Measured on a real local 3-node `localhost` cluster; README reports actual
numbers, whatever they are.

- Sustained write throughput benchmarked and reported (target 10k+ ops/sec).
- Leader failover time measured (target sub-2s).
- Linearizability verified across 100k+ randomized ops under partition/crash
  injection.
- P50/P95/P99 write latency measured and reported.
- Zero data loss across any single-node crash (verified by chaos + lincheck).
- Cluster survives a minority-side partition (verified by chaos suite).

## 9. Build order

Each phase is committed and pushed to the private GitHub repo.

1. Repo scaffold: module, layout, CI skeleton, README stub, lint config.
2. `wal` + single-node `store` + crash recovery.
3. `proto` definitions + `transport` interface + `simnet`.
4. `raft` core: leader election + log replication.
5. `raftstore` bbolt persistence + cluster crash recovery.
6. `server` + `client`: gRPC wiring, leader routing, ReadIndex reads.
7. Snapshots + log compaction.
8. Joint-consensus membership changes.
9. `lincheck` + `chaos` suite.
10. `bench` + measured Results section.
11. `metrics` + Grafana + Docker + Kubernetes + Helm.
12. `distkv-tui` demo + final README.
