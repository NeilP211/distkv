package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/simnet"
	"github.com/NeilP211/distkv/internal/store"
)

// readCluster bundles a 3-node RaftNode cluster wired over simnet plus the
// network handle so a test can install partitions.
type readCluster struct {
	net   *simnet.Network
	nodes map[raft.NodeID]*RaftNode
	ids   []raft.NodeID
}

func startReadCluster(t *testing.T) *readCluster {
	t.Helper()
	ids := []raft.NodeID{"n1", "n2", "n3"}
	net := simnet.NewNetwork(1)
	c := &readCluster{net: net, nodes: make(map[raft.NodeID]*RaftNode), ids: ids}
	for _, id := range ids {
		rn := newRaftNode(t, id, ids, net.Node(id))
		c.nodes[id] = rn
		net.Register(id, rn.Step)
	}
	for _, rn := range c.nodes {
		rn.Start()
	}
	t.Cleanup(func() {
		for _, rn := range c.nodes {
			rn.Stop()
		}
	})
	return c
}

func (c *readCluster) leader(t *testing.T) *RaftNode {
	t.Helper()
	return waitForLeader(t, 5*time.Second, c.nodes)
}

func TestLinearizableGet_ReadsCommittedWrite(t *testing.T) {
	c := startReadCluster(t)
	leader := c.leader(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, store.Command{Op: store.OpPut, Key: "k", Value: "v"}); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	v, found, err := leader.LinearizableGet(ctx, "k")
	if err != nil {
		t.Fatalf("LinearizableGet: %v", err)
	}
	if !found || v != "v" {
		t.Fatalf("LinearizableGet(k) = %q,%v want v,true", v, found)
	}
}

func TestLinearizableGet_FollowerReturnsErrNotLeader(t *testing.T) {
	c := startReadCluster(t)
	leader := c.leader(t)

	var follower *RaftNode
	for _, rn := range c.nodes {
		if rn != leader {
			follower = rn
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := follower.LinearizableGet(ctx, "k")
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("LinearizableGet on follower err = %v, want raft.ErrNotLeader", err)
	}
}

func TestLinearizableGet_PartitionedLeaderRejectsStaleRead(t *testing.T) {
	c := startReadCluster(t)
	leader := c.leader(t)

	// Commit a value while the cluster is healthy.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, store.Command{Op: store.OpPut, Key: "k", Value: "v"}); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	// Partition the leader away from the other two nodes.
	leaderID := leader.Status().ID
	var others []raft.NodeID
	for _, id := range c.ids {
		if id != leaderID {
			others = append(others, id)
		}
	}
	c.net.Partition([]raft.NodeID{leaderID}, others)

	// The partitioned node may still believe it is leader for a short
	// window.  Whenever it does, LinearizableGet MUST refuse rather than
	// return stale data: it cannot reach a majority for the heartbeat round.
	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer rcancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _, err := leader.LinearizableGet(rctx, "k")
		switch {
		case errors.Is(err, ErrLeadershipLost):
			// Correct: read refused because leadership could not be confirmed.
			return
		case errors.Is(err, raft.ErrNotLeader):
			// Also correct: the node has stepped down to a follower.
			return
		case err != nil:
			t.Fatalf("unexpected error from partitioned leader: %v", err)
		default:
			t.Fatal("partitioned leader served a read (stale-data risk), want ErrLeadershipLost")
		}
	}
}
