package raft_test

import (
	"fmt"
	"testing"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/simnet"
)

// cluster is a set of Nodes wired together over a simnet.Network, used to
// drive Raft tests purely by calling Tick.
type cluster struct {
	t    *testing.T
	net  *simnet.Network
	ids  []raft.NodeID
	node map[raft.NodeID]*raft.Node
}

// newCluster builds an n-node cluster over a fresh simnet.Network.  Each node
// uses its own MemStorage.
func newCluster(t *testing.T, n int, seed int64) *cluster {
	t.Helper()
	return newClusterWith(t, n, seed, nil)
}

// newClusterWith builds an n-node cluster, using storages[id] when provided.
func newClusterWith(t *testing.T, n int, seed int64, storages map[raft.NodeID]raft.Storage) *cluster {
	t.Helper()
	ids := make([]raft.NodeID, n)
	for i := 0; i < n; i++ {
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
	}
	net := simnet.NewNetwork(seed)
	c := &cluster{t: t, net: net, ids: ids, node: make(map[raft.NodeID]*raft.Node)}
	for _, id := range ids {
		var st raft.Storage
		if storages != nil && storages[id] != nil {
			st = storages[id]
		} else {
			st = raft.NewMemStorage()
		}
		cfg := raft.Config{
			ID:                 id,
			Peers:              ids,
			Storage:            st,
			Transport:          net.Node(id),
			ElectionTimeoutMin: 10,
			ElectionTimeoutMax: 20,
			HeartbeatInterval:  3,
		}
		nd, err := raft.NewNode(cfg)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		c.node[id] = nd
	}
	// Register handlers after all nodes exist so Step is always routable.
	for _, id := range ids {
		nd := c.node[id]
		net.Register(id, nd.Step)
	}
	return c
}

// tickAll advances every node by one tick.
func (c *cluster) tickAll() {
	for _, id := range c.ids {
		c.node[id].Tick()
	}
}

// runTicks advances the whole cluster by count ticks.
func (c *cluster) runTicks(count int) {
	for i := 0; i < count; i++ {
		c.tickAll()
	}
}

// leaders returns the IDs of every node that currently believes it is leader.
func (c *cluster) leaders() []raft.NodeID {
	var ls []raft.NodeID
	for _, id := range c.ids {
		if c.node[id].Role() == raft.Leader {
			ls = append(ls, id)
		}
	}
	return ls
}

// waitOneLeader runs ticks until exactly one stable leader exists.
func (c *cluster) waitOneLeader(maxTicks int) raft.NodeID {
	c.t.Helper()
	for i := 0; i < maxTicks; i++ {
		c.tickAll()
		ls := c.leaders()
		if len(ls) == 1 {
			stable := true
			lead := ls[0]
			for j := 0; j < 5; j++ {
				c.tickAll()
				ls2 := c.leaders()
				if len(ls2) != 1 || ls2[0] != lead {
					stable = false
					break
				}
			}
			if stable {
				return lead
			}
		}
	}
	c.t.Fatalf("no single stable leader after %d ticks (leaders=%v)", maxTicks, c.leaders())
	return ""
}

// countingTransport records every Send call so tests can assert that a
// single-node cluster never touches the transport.
type countingTransport struct{ sends int }

func (ct *countingTransport) Send(to raft.NodeID, msg raft.Message) (raft.Message, error) {
	ct.sends++
	return raft.Message{}, nil
}

// newSingleNode builds a one-member raft.Node ("solo") with the given
// transport.  The cluster's only member is the node itself.
func newSingleNode(t *testing.T, tr raft.Transport) *raft.Node {
	t.Helper()
	cfg := raft.Config{
		ID:                 "solo",
		Peers:              []raft.NodeID{"solo"},
		Storage:            raft.NewMemStorage(),
		Transport:          tr,
		ElectionTimeoutMin: 5,
		ElectionTimeoutMax: 5,
		HeartbeatInterval:  3,
	}
	nd, err := raft.NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode(solo): %v", err)
	}
	return nd
}

func TestSingleNodeSelfElects(t *testing.T) {
	ct := &countingTransport{}
	nd := newSingleNode(t, ct)

	if nd.Role() != raft.Follower {
		t.Fatalf("initial role = %v, want Follower", nd.Role())
	}

	// Drive only by Tick(); the node must become Leader once the election
	// timeout (5 ticks) elapses, with no peer responses.
	for i := 0; i < 5; i++ {
		nd.Tick()
	}

	if nd.Role() != raft.Leader {
		t.Fatalf("role after election timeout = %v, want Leader", nd.Role())
	}
	if nd.Leader() != "solo" {
		t.Fatalf("Leader() = %q, want \"solo\"", nd.Leader())
	}
	if ct.sends != 0 {
		t.Fatalf("transport Send count = %d, want 0 (single-node needs no transport)", ct.sends)
	}
}

func TestElectionThreeNodesOneLeader(t *testing.T) {
	c := newCluster(t, 3, 1)
	leader := c.waitOneLeader(200)

	term := c.node[leader].Term()
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if c.node[id].Role() != raft.Follower {
			t.Fatalf("%s role = %v, want Follower", id, c.node[id].Role())
		}
		if c.node[id].Term() != term {
			t.Fatalf("%s term = %d, want %d", id, c.node[id].Term(), term)
		}
	}
}

func TestElectionFiveNodesOneLeader(t *testing.T) {
	c := newCluster(t, 5, 42)
	leader := c.waitOneLeader(300)
	if leader == "" {
		t.Fatal("no leader")
	}
	if got := len(c.leaders()); got != 1 {
		t.Fatalf("leader count = %d, want 1", got)
	}
}

func TestElectionIsolatedNodeCannotLead(t *testing.T) {
	c := newCluster(t, 3, 7)
	// Isolate n1 before any election.
	c.net.Isolate("n1")

	// Run many ticks; n1 must never become a stable leader.
	for i := 0; i < 300; i++ {
		c.tickAll()
		if c.node["n1"].Role() == raft.Leader {
			t.Fatalf("isolated n1 became leader at tick %d", i)
		}
	}
	// The majority side (n2,n3) should have a leader.
	major := 0
	for _, id := range []raft.NodeID{"n2", "n3"} {
		if c.node[id].Role() == raft.Leader {
			major++
		}
	}
	if major != 1 {
		t.Fatalf("majority side leader count = %d, want 1", major)
	}

	// Heal and confirm the cluster converges to exactly one leader.
	c.net.Recover("n1")
	c.waitOneLeader(300)
}

func TestElectionLeaderStepsDownOnHigherTerm(t *testing.T) {
	c := newCluster(t, 3, 3)
	leader := c.waitOneLeader(200)
	oldTerm := c.node[leader].Term()

	// Deliver a message from a fictitious higher term.
	resp := c.node[leader].Step(raft.Message{
		Type: raft.MsgAppendEntries,
		From: "n_ghost",
		To:   leader,
		Term: oldTerm + 5,
	})
	if c.node[leader].Role() != raft.Follower {
		t.Fatalf("leader role after higher-term msg = %v, want Follower", c.node[leader].Role())
	}
	if c.node[leader].Term() != oldTerm+5 {
		t.Fatalf("term after higher-term msg = %d, want %d", c.node[leader].Term(), oldTerm+5)
	}
	if !resp.Success {
		t.Fatalf("expected Success heartbeat reply, got %+v", resp)
	}
}
