# DistKV

A Raft-based, fault-tolerant, linearizable distributed key-value store, built in Go from scratch.

**Status: in development**

## What it is

DistKV is a pedagogical distributed key-value store implementing the Raft consensus protocol from the ground up — no etcd/raft library dependency. It includes:

- A full Raft implementation (leader election, log replication, snapshotting, membership changes, ReadIndex for linearizable reads)
- A gRPC transport layer for inter-node and client-facing communication
- A leader-aware, retrying `pkg/client` that redirects to the leader transparently
- A fault-injection chaos suite with a WGL linearizability checker
- A throughput/latency/failover benchmark suite

## Benchmark Results

These numbers were measured on a real 3-node localhost cluster on this machine. They are actual measurements, not targets or estimates.

**Hardware / environment:**
- Apple M2, Darwin 24.6.0 (arm64)
- Go 1.26.3 darwin/arm64
- 3-node cluster on 127.0.0.1:9001/9002/9003 (all on one machine, loopback only)
- Concurrency: 32 closed-loop goroutines per run
- Key space: 1000 keys, value size: 128 bytes
- Warmup: 2s discarded before measurement window (10s)

### Throughput and latency

| Workload   | Throughput (ops/sec) | P50 (ms) | P95 (ms) | P99 (ms) |
|------------|---------------------|----------|----------|----------|
| write-only | 82                  | 418.66   | 608.99   | 656.38   |
| read-only  | 9,868               | 3.21     | 5.42     | 6.79     |
| mixed (70% get / 25% put / 5% CAS) | 270 | 117.10 | 229.87 | 287.82 |

### Failover time

**820 ms** — time from SIGKILL to the leader process until the first successful `Put` completes on the new leader (measured on the same loopback cluster).

### Interpretation

- **Writes are serialized through the Raft commit pipeline.** Each `Put` or `CAS` requires a full Raft round-trip: the leader appends the entry, sends `AppendEntries` to both followers synchronously, and waits for a quorum ACK before applying. On this single-machine cluster the Raft pipeline commits ~80 writes/sec. This is expected for a correctness-first implementation (50ms tick, 150ms heartbeat interval, synchronous per-follower dispatch). A production system would batch proposals and pipeline AppendEntries asynchronously.
- **Reads are fast** because linearizable `Get` uses ReadIndex (leader confirms quorum via heartbeat, then reads locally without a log append), giving ~10k ops/sec at sub-5ms P95.
- **Mixed workload latency** is dominated by the write fraction (30% of ops) queuing behind the commit pipeline, raising P50 to ~117ms.
- **Failover** (820ms) covers Raft election timeout (10–20 ticks × 50ms = 500–1000ms randomized) plus the client's leader-discovery retry. No leader-hint persistence across failover; the client retries all known endpoints.

These numbers are from a loopback cluster, not a LAN or WAN. Network latency dominates real deployments.

## Reproducing

```
make bench
```

This builds the bench binary, starts a 3-node local cluster, runs all three workloads, then tears down cleanly.

For the failover sub-benchmark:

```
./scripts/cluster-up.sh
sleep 3
LEADER=$(bin/distkvctl status | grep '^leader:' | awk '{print $2}')
PID=$(cat data/${LEADER}.pid)
# Build pids string from all nodes
PIDS=$(for n in n1 n2 n3; do echo -n "${n}=$(cat data/${n}.pid),"; done | sed 's/,$//') 
bin/distkv-bench --failover --pids "$PIDS"
./scripts/cluster-down.sh --clean
```

## Architecture

```
pkg/client        — leader-aware gRPC client with transparent retry + redirect
cmd/distkvd       — daemon: one Raft node + gRPC server (KV + Raft RPCs)
cmd/distkvctl     — one-shot CLI (put / get / del / cas / status)
cmd/distkv-bench  — throughput/latency/failover benchmark
internal/raft     — Raft consensus core (election, replication, snapshots, membership)
internal/server   — RaftNode driver (ticker, apply loop, Propose, LinearizableGet)
internal/store    — in-memory KV state machine
internal/raftstore— bbolt-backed durable log + HardState + snapshot storage
internal/transport— gRPC Raft transport
internal/chaos    — fault-injection harness (partitions, drops, delays, crashes)
internal/lincheck — WGL linearizability checker
internal/bench    — benchmark harness (log-scale histogram, closed-loop runner)
```
