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

// tickInterval is kept short so tests run quickly while remaining well above
// scheduling jitter; election timeouts below are expressed in ticks.
const testTick = 10 * time.Millisecond

// newRaftNode builds a RaftNode with an in-memory raft store over the given
// transport.  Election timeouts are randomized between 10 and 20 ticks and the
// heartbeat fires every 3 ticks.
func newRaftNode(t *testing.T, id raft.NodeID, peers []raft.NodeID, tr raft.Transport) *RaftNode {
	t.Helper()
	rn, err := NewRaftNode(RaftNodeConfig{
		Raft: raft.Config{
			ID:                 id,
			Peers:              peers,
			Storage:            raft.NewMemStorage(),
			Transport:          tr,
			ElectionTimeoutMin: 10,
			ElectionTimeoutMax: 20,
			HeartbeatInterval:  3,
		},
		TickInterval: testTick,
	})
	if err != nil {
		t.Fatalf("NewRaftNode(%s): %v", id, err)
	}
	return rn
}

// waitFor polls cond until it returns true or the deadline elapses.
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, desc)
}

// waitForLeader returns the single elected leader among the nodes, failing if
// none (or more than one) is elected within the timeout.
func waitForLeader(t *testing.T, timeout time.Duration, nodes map[raft.NodeID]*RaftNode) *RaftNode {
	t.Helper()
	var leader *RaftNode
	waitFor(t, timeout, "a leader to be elected", func() bool {
		count := 0
		leader = nil
		for _, rn := range nodes {
			if rn.Status().Role == "Leader" {
				count++
				leader = rn
			}
		}
		return count == 1
	})
	return leader
}

func TestRaftNode_SingleNodeProposeApplies(t *testing.T) {
	rn := newRaftNode(t, "n1", []raft.NodeID{"n1"}, nopTransport{})
	rn.Start()
	defer rn.Stop()

	waitFor(t, 3*time.Second, "single node to become leader", func() bool {
		return rn.Status().Role == "Leader"
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := rn.Propose(ctx, store.Command{Op: store.OpPut, Key: "k", Value: "v"})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if got != "v" {
		t.Fatalf("Propose result = %q, want %q", got, "v")
	}
	if v, ok := rn.LocalGet("k"); !ok || v != "v" {
		t.Fatalf("LocalGet(k) = %q,%v want v,true", v, ok)
	}
}

func TestRaftNode_ClusterReplicates(t *testing.T) {
	net := simnet.NewNetwork(1)
	ids := []raft.NodeID{"n1", "n2", "n3"}
	nodes := make(map[raft.NodeID]*RaftNode)
	for _, id := range ids {
		rn := newRaftNode(t, id, ids, net.Node(id))
		nodes[id] = rn
		net.Register(id, rn.Step)
	}
	for _, rn := range nodes {
		rn.Start()
	}
	defer func() {
		for _, rn := range nodes {
			rn.Stop()
		}
	}()

	leader := waitForLeader(t, 5*time.Second, nodes)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, store.Command{Op: store.OpPut, Key: "color", Value: "blue"}); err != nil {
		t.Fatalf("Propose on leader: %v", err)
	}

	// The value must replicate and apply on every node.
	for id, rn := range nodes {
		rn := rn
		waitFor(t, 3*time.Second, "node "+string(id)+" to apply", func() bool {
			v, ok := rn.LocalGet("color")
			return ok && v == "blue"
		})
	}
}

func TestRaftNode_ProposeOnFollowerReturnsErrNotLeader(t *testing.T) {
	net := simnet.NewNetwork(2)
	ids := []raft.NodeID{"n1", "n2", "n3"}
	nodes := make(map[raft.NodeID]*RaftNode)
	for _, id := range ids {
		rn := newRaftNode(t, id, ids, net.Node(id))
		nodes[id] = rn
		net.Register(id, rn.Step)
	}
	for _, rn := range nodes {
		rn.Start()
	}
	defer func() {
		for _, rn := range nodes {
			rn.Stop()
		}
	}()

	leader := waitForLeader(t, 5*time.Second, nodes)

	var follower *RaftNode
	for _, rn := range nodes {
		if rn != leader {
			follower = rn
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := follower.Propose(ctx, store.Command{Op: store.OpPut, Key: "k", Value: "v"})
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("Propose on follower err = %v, want raft.ErrNotLeader", err)
	}
}

func TestRaftNode_ProposeContextCancellation(t *testing.T) {
	// A single-node cluster that never starts: the node is leader-eligible
	// but with Start never called nothing commits, so Propose blocks until
	// the context is cancelled.
	rn := newRaftNode(t, "n1", []raft.NodeID{"n1"}, nopTransport{})
	rn.Start()
	defer rn.Stop()

	waitFor(t, 3*time.Second, "node to become leader", func() bool {
		return rn.Status().Role == "Leader"
	})

	// Stop the apply loop so the proposal can never be acked, then propose
	// with a short-lived context.
	rn.Stop()

	// After Stop, Propose should return promptly (node stopped).
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := rn.Propose(ctx, store.Command{Op: store.OpPut, Key: "k", Value: "v"})
	if err == nil {
		t.Fatal("Propose after Stop: expected error, got nil")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Propose took %s, expected prompt return", elapsed)
	}
}

func TestRaftNode_ProposeAlreadyCancelledContext(t *testing.T) {
	// A proposal made with an already-cancelled context must return promptly
	// rather than block waiting for the apply loop.
	rn := newRaftNode(t, "n1", []raft.NodeID{"n1"}, nopTransport{})
	rn.Start()
	defer rn.Stop()
	waitFor(t, 3*time.Second, "leader", func() bool { return rn.Status().Role == "Leader" })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	start := time.Now()
	_, err := rn.Propose(ctx, store.Command{Op: store.OpPut, Key: "k", Value: "v"})
	if !errors.Is(err, context.Canceled) {
		// It is possible the entry applied before the select observed the
		// cancelled context; both outcomes are acceptable.
		if err != nil {
			t.Fatalf("Propose with cancelled ctx err = %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Propose took %s, expected prompt return", elapsed)
	}
}

func TestRaftNode_StopIdempotent(t *testing.T) {
	rn := newRaftNode(t, "n1", []raft.NodeID{"n1"}, nopTransport{})
	rn.Start()
	rn.Stop()
	rn.Stop() // must not panic or block
}

// nopTransport is a Transport for single-node clusters: it is never called
// because the node has no peers, but Config requires a non-nil Transport.
type nopTransport struct{}

func (nopTransport) Send(raft.NodeID, raft.Message) (raft.Message, error) {
	return raft.Message{}, errors.New("nopTransport: no peers")
}
