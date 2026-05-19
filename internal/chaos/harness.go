// Package chaos provides a deterministic chaos-testing harness for DistKV.
//
// A Cluster runs N server.RaftNodes in real time over a single
// simnet.Network, so faults — partitions, drops, delays, crashes,
// restarts — can be injected into the live consensus protocol.  The
// companion Workload drives closed-loop clients that retry every operation
// until it has a definitive outcome, recording a clean lincheck.History the
// linearizability checker can verify.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/server"
	"github.com/NeilP211/distkv/internal/simnet"
	"github.com/NeilP211/distkv/internal/store"
)

// chaosTick is the wall-clock period between Raft ticks for every node in a
// chaos Cluster.  Election timeouts below are expressed in ticks, so a 20ms
// tick with a 6-12 tick election timeout settles elections in 120-240ms —
// fast enough for short CI scenarios, slow enough to be free of jitter.
const chaosTick = 20 * time.Millisecond

const (
	chaosElectionMin = 6
	chaosElectionMax = 12
	chaosHeartbeat   = 2
)

// node bundles a single cluster member: its RaftNode driver plus the
// MemStorage backing it.  The MemStorage outlives any individual RaftNode so
// a RestartNode can rebuild a fresh RaftNode over the same durable state,
// modelling a genuine crash-restart with state recovery.
type node struct {
	rn      *server.RaftNode
	storage *raft.MemStorage
}

// Cluster is an N-node DistKV cluster running over a fault-injecting simnet.
// All exported methods are safe for concurrent use.
type Cluster struct {
	net *simnet.Network
	ids []raft.NodeID

	mu    sync.Mutex
	nodes map[raft.NodeID]*node
}

// NewCluster builds and starts an n-node cluster over a simnet seeded with
// seed.  Every node shares the one Network so faults are injectable.
func NewCluster(n int, seed int64) *Cluster {
	if n < 1 {
		panic("chaos: NewCluster needs at least 1 node")
	}
	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
	}
	c := &Cluster{
		net:   simnet.NewNetwork(seed),
		ids:   ids,
		nodes: make(map[raft.NodeID]*node, n),
	}
	for _, id := range ids {
		st := raft.NewMemStorage()
		rn := c.buildNode(id, st)
		c.nodes[id] = &node{rn: rn, storage: st}
		c.net.Register(id, rn.Step)
	}
	for _, nd := range c.nodes {
		nd.rn.Start()
	}
	return c
}

// buildNode constructs a RaftNode for id over storage st, wired to this
// cluster's simnet transport.  It does not Start the node.
func (c *Cluster) buildNode(id raft.NodeID, st *raft.MemStorage) *server.RaftNode {
	rn, err := server.NewRaftNode(server.RaftNodeConfig{
		Raft: raft.Config{
			ID:                 id,
			Peers:              c.ids,
			Storage:            st,
			Transport:          c.net.Node(id),
			ElectionTimeoutMin: chaosElectionMin,
			ElectionTimeoutMax: chaosElectionMax,
			HeartbeatInterval:  chaosHeartbeat,
		},
		TickInterval: chaosTick,
	})
	if err != nil {
		panic("chaos: NewRaftNode: " + err.Error())
	}
	return rn
}

// IDs returns the cluster's node ids.  The returned slice must not be
// mutated.
func (c *Cluster) IDs() []raft.NodeID { return c.ids }

// Stop stops every node cleanly.  It is idempotent.
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

// node returns the bookkeeping struct for id under the cluster lock.
func (c *Cluster) node(id raft.NodeID) *node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// snapshotNodes returns a copy of the current id->node map.
func (c *Cluster) snapshotNodes() map[raft.NodeID]*node {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[raft.NodeID]*node, len(c.nodes))
	for id, nd := range c.nodes {
		out[id] = nd
	}
	return out
}

// Leader reports the cluster's current leader as seen by any node's Status,
// returning false when no leader is known yet.  It requires that at least
// one node both claims the Leader role and is named as leader, so a stale
// belief during an election does not produce a phantom leader.
func (c *Cluster) Leader() (raft.NodeID, bool) {
	nodes := c.snapshotNodes()
	for id, nd := range nodes {
		s := nd.rn.Status()
		if s.Role == "Leader" && s.Leader == id {
			return id, true
		}
	}
	return "", false
}

// WaitLeader polls until a single stable leader exists (one node claims the
// Leader role and every other node either agrees or has no opinion yet),
// returning that leader id.  It fails with an error on timeout.
func (c *Cluster) WaitLeader(timeout time.Duration) (raft.NodeID, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id, ok := c.stableLeader(); ok {
			return id, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return "", fmt.Errorf("chaos: no stable leader within %s", timeout)
}

// stableLeader returns the leader id when exactly one node claims the Leader
// role and no other node claims it.
func (c *Cluster) stableLeader() (raft.NodeID, bool) {
	nodes := c.snapshotNodes()
	var leader raft.NodeID
	count := 0
	for id, nd := range nodes {
		if nd.rn.Status().Role == "Leader" {
			leader = id
			count++
		}
	}
	if count != 1 {
		return "", false
	}
	return leader, true
}

// --- Fault injection: thin delegations to the simnet. ---

// Partition installs a network partition; each group is a set of node ids
// that can reach one another.  A later Heal restores full connectivity.
func (c *Cluster) Partition(groups ...[]raft.NodeID) { c.net.Partition(groups...) }

// Heal removes any active partition.
func (c *Cluster) Heal() { c.net.Heal() }

// SetDrop sets the probability [0,1] that a message is dropped.
func (c *Cluster) SetDrop(rate float64) { c.net.SetDrop(rate) }

// SetDelay configures a random per-message delivery delay in [min,max].
func (c *Cluster) SetDelay(min, max time.Duration) { c.net.SetDelay(min, max) }

// CrashNode makes id unreachable while retaining its in-memory state
// (a process pause).  RecoverNode undoes it.
func (c *Cluster) CrashNode(id raft.NodeID) { c.net.Crash(id) }

// RecoverNode restores a CrashNode'd id to the network.
func (c *Cluster) RecoverNode(id raft.NodeID) { c.net.Recover(id) }

// RestartNode models a genuine crash-restart: it stops id's RaftNode, builds
// a fresh RaftNode over the SAME MemStorage (so committed state is recovered
// from durable storage exactly as a real restart would), re-registers it
// with the simnet, and starts it.
func (c *Cluster) RestartNode(id raft.NodeID) {
	c.mu.Lock()
	nd := c.nodes[id]
	c.mu.Unlock()
	if nd == nil {
		panic("chaos: RestartNode of unknown node " + id)
	}
	nd.rn.Stop()
	fresh := c.buildNode(id, nd.storage)

	c.mu.Lock()
	nd.rn = fresh
	c.mu.Unlock()

	c.net.Register(id, fresh.Step)
	fresh.Start()
}

// --- Client operations used by the workload. ---

// proposeOn issues cmd as a proposal against node id.  It returns the apply
// result, raft.ErrNotLeader on a non-leader, or any transient error.
func (c *Cluster) proposeOn(ctx context.Context, id raft.NodeID, cmd store.Command) (string, error) {
	nd := c.node(id)
	if nd == nil {
		return "", errors.New("chaos: unknown node " + string(id))
	}
	c.mu.Lock()
	rn := nd.rn
	c.mu.Unlock()
	return rn.Propose(ctx, cmd)
}

// getOn issues a linearizable read of key against node id.
func (c *Cluster) getOn(ctx context.Context, id raft.NodeID, key string) (string, bool, error) {
	nd := c.node(id)
	if nd == nil {
		return "", false, errors.New("chaos: unknown node " + string(id))
	}
	c.mu.Lock()
	rn := nd.rn
	c.mu.Unlock()
	return rn.LinearizableGet(ctx, key)
}

// leaderHint returns a node id that some node currently believes is the
// leader, or "" if none is known.  Clients use it to re-target retries.
func (c *Cluster) leaderHint() raft.NodeID {
	nodes := c.snapshotNodes()
	for _, nd := range nodes {
		if l := nd.rn.Status().Leader; l != "" {
			return l
		}
	}
	return ""
}
