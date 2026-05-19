package raft_test

import (
	"testing"

	"github.com/NeilP211/distkv/internal/raft"
)

func TestConfirmLeadership_ThreeNodeLeaderConfirms(t *testing.T) {
	c := newCluster(t, 3, 1)
	leader := c.waitOneLeader(200)

	readIdx, ok := c.node[leader].ConfirmLeadership()
	if !ok {
		t.Fatalf("ConfirmLeadership on healthy leader %s: ok=false, want true", leader)
	}
	if readIdx != c.node[leader].CommitIndex() {
		t.Fatalf("readIndex = %d, want commitIndex %d", readIdx, c.node[leader].CommitIndex())
	}
}

func TestConfirmLeadership_NonLeaderRejected(t *testing.T) {
	c := newCluster(t, 3, 1)
	leader := c.waitOneLeader(200)

	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if _, ok := c.node[id].ConfirmLeadership(); ok {
			t.Fatalf("ConfirmLeadership on follower %s: ok=true, want false", id)
		}
	}
}

func TestConfirmLeadership_PartitionedLeaderRejected(t *testing.T) {
	c := newCluster(t, 3, 1)
	leader := c.waitOneLeader(200)

	// Isolate the leader from the other two nodes: its heartbeat round can
	// no longer reach a majority.
	var others []raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			others = append(others, id)
		}
	}
	c.net.Partition([]raft.NodeID{leader}, others)

	// The deposed/partitioned leader must NOT confirm leadership.
	if _, ok := c.node[leader].ConfirmLeadership(); ok {
		t.Fatal("ConfirmLeadership on partitioned leader: ok=true, want false (stale-read risk)")
	}
}

func TestConfirmLeadership_SingleNodeLeaderConfirms(t *testing.T) {
	ct := &countingTransport{}
	nd := newSingleNode(t, ct)
	for i := 0; i < 5; i++ {
		nd.Tick()
	}
	if nd.Role() != raft.Leader {
		t.Fatalf("single node role = %v, want Leader", nd.Role())
	}

	readIdx, ok := nd.ConfirmLeadership()
	if !ok {
		t.Fatal("ConfirmLeadership on single-node leader: ok=false, want true")
	}
	if readIdx != nd.CommitIndex() {
		t.Fatalf("readIndex = %d, want commitIndex %d", readIdx, nd.CommitIndex())
	}
	if ct.sends != 0 {
		t.Fatalf("single-node ConfirmLeadership sent %d messages, want 0", ct.sends)
	}
}
