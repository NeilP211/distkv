// Package websim runs an in-process DistKV cluster for the live web showcase.
//
// It mirrors the construction recipe of internal/chaos.Cluster — N
// server.RaftNodes driven in real time over one fault-injecting
// simnet.Network — but adds the surface the browser front-end needs: an
// all-node Snapshot, per-node log views, leader-aware KV operations, speed
// presets, a reset, and a forwarded RPC observer tap. It deliberately does NOT
// reuse internal/chaos so the verified linearizability path stays untouched.
package websim

import (
	"fmt"
	"sync"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/server"
	"github.com/NeilP211/distkv/internal/simnet"
)

// Election/heartbeat timeouts in Tick units (same ratios as the chaos
// harness): a 6–12 tick election timeout with a 2 tick heartbeat settles
// elections in a handful of ticks. The wall-clock pace is set by the Speed.
const (
	electionMin = 6
	electionMax = 12
	heartbeat   = 2
)

// Speed selects the wall-clock tick interval, trading realism for watchability.
type Speed int

const (
	// SpeedNormal is the demo default: RPCs are paced for human viewing.
	SpeedNormal Speed = iota
	// SpeedSlow exaggerates the pacing so every heartbeat is easy to follow.
	SpeedSlow
	// SpeedFast approaches the chaos-harness pace.
	SpeedFast
)

func (s Speed) tick() time.Duration {
	switch s {
	case SpeedSlow:
		return 250 * time.Millisecond
	case SpeedFast:
		return 60 * time.Millisecond
	default:
		return 150 * time.Millisecond
	}
}

func (s Speed) String() string {
	switch s {
	case SpeedSlow:
		return "slow"
	case SpeedFast:
		return "fast"
	default:
		return "normal"
	}
}

// ParseSpeed maps a UI string to a Speed, defaulting to SpeedNormal.
func ParseSpeed(s string) Speed {
	switch s {
	case "slow":
		return SpeedSlow
	case "fast":
		return SpeedFast
	default:
		return SpeedNormal
	}
}

// NodeState is one node's state in a ClusterState snapshot.
type NodeState struct {
	ID        string `json:"id"`
	Role      string `json:"role"`
	Term      uint64 `json:"term"`
	Leader    string `json:"leader"`
	Commit    uint64 `json:"commit"`
	LastIndex uint64 `json:"lastIndex"`
	Up        bool   `json:"up"`
}

// ClusterState is a point-in-time snapshot of the whole cluster, JSON-encoded
// to the browser as a "state" frame.
type ClusterState struct {
	Nodes      []NodeState `json:"nodes"`
	Partitions [][]string  `json:"partitions"` // nil = fully connected
	Drop       float64     `json:"drop"`
	Speed      string      `json:"speed"`
}

// node bundles a member's live RaftNode with the MemStorage that outlives it,
// so a restart rebuilds a fresh RaftNode over the same durable state.
type node struct {
	rn      *server.RaftNode
	storage *raft.MemStorage
}

// Cluster is an N-node in-process DistKV cluster over a fault-injecting simnet.
// All exported methods are safe for concurrent use.
type Cluster struct {
	mu    sync.Mutex
	net   *simnet.Network
	ids   []raft.NodeID
	nodes map[raft.NodeID]*node

	// Mirrored fault state, surfaced via Snapshot.
	down  map[raft.NodeID]bool
	parts [][]raft.NodeID
	drop  float64

	speed Speed
	seed  int64
	obs   func(simnet.MessageEvent)
}

// New builds and starts an n-node cluster seeded with seed, paced at speed.
// obs, if non-nil, is installed as the simnet RPC observer (forwarded on every
// network rebuild). It panics if n < 1.
func New(n int, seed int64, speed Speed, obs func(simnet.MessageEvent)) *Cluster {
	if n < 1 {
		panic("websim: New needs at least 1 node")
	}
	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
	}
	c := &Cluster{
		ids:   ids,
		speed: speed,
		seed:  seed,
		obs:   obs,
	}
	c.build()
	return c
}

// build constructs a fresh network and node set over the current ids/speed,
// installs the observer, registers every node, and starts them. The caller
// must hold no expectation of prior nodes; build replaces them wholesale.
func (c *Cluster) build() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.net = simnet.NewNetwork(c.seed)
	if c.obs != nil {
		c.net.SetObserver(c.obs)
	}
	c.nodes = make(map[raft.NodeID]*node, len(c.ids))
	c.down = make(map[raft.NodeID]bool)
	c.parts = nil
	c.drop = 0
	for _, id := range c.ids {
		st := raft.NewMemStorage()
		rn := c.buildNode(id, st)
		c.nodes[id] = &node{rn: rn, storage: st}
		c.net.Register(id, rn.Step)
	}
	for _, nd := range c.nodes {
		nd.rn.Start()
	}
}

// buildNode constructs (but does not start) a RaftNode for id over storage st,
// wired to this cluster's network at the current speed. Caller holds c.mu.
func (c *Cluster) buildNode(id raft.NodeID, st *raft.MemStorage) *server.RaftNode {
	rn, err := server.NewRaftNode(server.RaftNodeConfig{
		Raft: raft.Config{
			ID:                 id,
			Peers:              c.ids,
			Storage:            st,
			Transport:          c.net.Node(id),
			ElectionTimeoutMin: electionMin,
			ElectionTimeoutMax: electionMax,
			HeartbeatInterval:  heartbeat,
		},
		TickInterval: c.speed.tick(),
	})
	if err != nil {
		panic("websim: NewRaftNode: " + err.Error())
	}
	return rn
}

// IDs returns the cluster's node ids as strings. The result is a fresh slice.
func (c *Cluster) IDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.ids))
	for i, id := range c.ids {
		out[i] = string(id)
	}
	return out
}

// Stop stops every node cleanly. It is idempotent.
func (c *Cluster) Stop() {
	c.mu.Lock()
	nodes := make([]*node, 0, len(c.nodes))
	for _, nd := range c.nodes {
		nodes = append(nodes, nd)
	}
	c.mu.Unlock()
	for _, nd := range nodes {
		nd.rn.Stop()
	}
}

// Snapshot returns a consistent view of every node's Raft state plus the
// current fault configuration.
func (c *Cluster) Snapshot() ClusterState {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodes := make([]NodeState, len(c.ids))
	for i, id := range c.ids {
		nd := c.nodes[id]
		s := nd.rn.Status()
		nodes[i] = NodeState{
			ID:        string(id),
			Role:      s.Role,
			Term:      s.Term,
			Leader:    string(s.Leader),
			Commit:    s.CommitIndex,
			LastIndex: nd.rn.LastIndex(),
			Up:        !c.down[id],
		}
	}

	var parts [][]string
	if c.parts != nil {
		parts = make([][]string, len(c.parts))
		for i, g := range c.parts {
			grp := make([]string, len(g))
			for j, id := range g {
				grp[j] = string(id)
			}
			parts[i] = grp
		}
	}

	return ClusterState{Nodes: nodes, Partitions: parts, Drop: c.drop, Speed: c.speed.String()}
}

// --- Fault injection: track mirrored state, then delegate to the simnet. ---

// CrashNode makes id unreachable while retaining its in-memory state.
func (c *Cluster) CrashNode(id string) {
	c.mu.Lock()
	c.down[raft.NodeID(id)] = true
	c.mu.Unlock()
	c.net.Crash(raft.NodeID(id))
}

// RecoverNode restores a crashed id to the network.
func (c *Cluster) RecoverNode(id string) {
	c.mu.Lock()
	delete(c.down, raft.NodeID(id))
	c.mu.Unlock()
	c.net.Recover(raft.NodeID(id))
}

// RestartNode models a genuine crash-restart: it stops id's RaftNode, builds a
// fresh one over the SAME storage (recovering committed state), re-registers
// it, and starts it. A restarted node is considered up.
func (c *Cluster) RestartNode(id string) {
	nid := raft.NodeID(id)
	c.mu.Lock()
	nd := c.nodes[nid]
	c.mu.Unlock()
	if nd == nil {
		return
	}
	nd.rn.Stop()
	fresh := c.buildNodeLocked(nid, nd.storage)

	c.mu.Lock()
	nd.rn = fresh
	delete(c.down, nid)
	c.mu.Unlock()

	c.net.Recover(nid)
	c.net.Register(nid, fresh.Step)
	fresh.Start()
}

// buildNodeLocked builds a node without holding c.mu (buildNode requires the
// lock for c.net/c.ids/c.speed reads; those are stable between rebuilds, so we
// briefly take the lock here).
func (c *Cluster) buildNodeLocked(id raft.NodeID, st *raft.MemStorage) *server.RaftNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buildNode(id, st)
}

// Partition installs a network partition from groups of node-id strings; an
// empty groups argument heals.
func (c *Cluster) Partition(groups [][]string) {
	if len(groups) == 0 {
		c.Heal()
		return
	}
	parts := make([][]raft.NodeID, len(groups))
	flat := make([][]raft.NodeID, len(groups))
	for i, g := range groups {
		ids := make([]raft.NodeID, len(g))
		for j, s := range g {
			ids[j] = raft.NodeID(s)
		}
		parts[i] = ids
		flat[i] = ids
	}
	c.mu.Lock()
	c.parts = parts
	c.mu.Unlock()
	c.net.Partition(flat...)
}

// Heal removes any active partition.
func (c *Cluster) Heal() {
	c.mu.Lock()
	c.parts = nil
	c.mu.Unlock()
	c.net.Heal()
}

// SetDrop sets the probability [0,1] that a message is dropped.
func (c *Cluster) SetDrop(rate float64) {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	c.mu.Lock()
	c.drop = rate
	c.mu.Unlock()
	c.net.SetDrop(rate)
}

// SetSpeed rebuilds every node at a new wall-clock tick interval over the same
// storage, preserving committed state. Faults (crashes/partitions) are
// cleared, since the network is rebuilt.
func (c *Cluster) SetSpeed(speed Speed) {
	c.Stop()
	c.mu.Lock()
	c.speed = speed
	c.mu.Unlock()
	c.build()
}

// Reset stops the cluster and rebuilds it from scratch with a new seed and
// empty state — a clean slate for the demo.
func (c *Cluster) Reset() {
	c.Stop()
	c.mu.Lock()
	c.seed++
	c.mu.Unlock()
	c.build()
}
