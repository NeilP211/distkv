package server

import (
	"context"
	"testing"
	"time"

	"github.com/NeilP211/distkv/api"
	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/simnet"
)

// kvCluster bundles a RaftNode and its KVService for each cluster member.
type kvCluster struct {
	nodes map[raft.NodeID]*RaftNode
	svc   map[raft.NodeID]*KVService
}

// startKVCluster builds an n-node cluster of RaftNodes wired over simnet, each
// fronted by a KVService, and starts the raft drivers.
func startKVCluster(t *testing.T, ids []raft.NodeID) *kvCluster {
	t.Helper()
	net := simnet.NewNetwork(1)
	c := &kvCluster{
		nodes: make(map[raft.NodeID]*RaftNode),
		svc:   make(map[raft.NodeID]*KVService),
	}
	for _, id := range ids {
		rn := newRaftNode(t, id, ids, net.Node(id))
		c.nodes[id] = rn
		net.Register(id, rn.Step)
	}
	for _, id := range ids {
		c.svc[id] = NewKVService(c.nodes[id], nil)
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

// leader returns the RaftNode id currently reporting itself as leader.
func (c *kvCluster) leaderID(t *testing.T) raft.NodeID {
	t.Helper()
	var id raft.NodeID
	waitFor(t, 5*time.Second, "a single leader", func() bool {
		count := 0
		id = ""
		for nid, rn := range c.nodes {
			if rn.Status().Role == "Leader" {
				count++
				id = nid
			}
		}
		return count == 1
	})
	return id
}

func TestKVService_WriteToFollowerReturnsLeaderHint(t *testing.T) {
	ids := []raft.NodeID{"n1", "n2", "n3"}
	c := startKVCluster(t, ids)
	leaderID := c.leaderID(t)

	var followerID raft.NodeID
	for _, id := range ids {
		if id != leaderID {
			followerID = id
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.svc[followerID].Put(ctx, &api.PutReq{Key: "k", Value: []byte("v")})
	if err != nil {
		t.Fatalf("Put on follower returned gRPC error: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("Put on follower reported success, want not-leader failure")
	}
	// The hint must be a real cluster member.  It usually equals leaderID,
	// but during a re-election the follower's known leader may briefly lag,
	// so we only require it to name some member (and not the follower).
	hint := resp.GetLeaderHint()
	if hint == "" {
		t.Fatal("Put on follower: empty leader_hint, want non-empty")
	}
	isMember := false
	for _, id := range ids {
		if string(id) == hint {
			isMember = true
		}
	}
	if !isMember {
		t.Fatalf("leader_hint %q is not a cluster member", hint)
	}
}

func TestKVService_WriteToLeaderSucceedsAndGetReads(t *testing.T) {
	ids := []raft.NodeID{"n1", "n2", "n3"}
	c := startKVCluster(t, ids)
	leaderID := c.leaderID(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	putResp, err := c.svc[leaderID].Put(ctx, &api.PutReq{Key: "color", Value: []byte("blue")})
	if err != nil {
		t.Fatalf("Put on leader: %v", err)
	}
	if !putResp.GetSuccess() {
		t.Fatalf("Put on leader not successful, leader_hint=%q", putResp.GetLeaderHint())
	}

	getResp, err := c.svc[leaderID].Get(ctx, &api.GetReq{Key: "color"})
	if err != nil {
		t.Fatalf("Get on leader: %v", err)
	}
	if !getResp.GetFound() || string(getResp.GetValue()) != "blue" {
		t.Fatalf("Get on leader = %q,found=%v want blue,true",
			getResp.GetValue(), getResp.GetFound())
	}
}

func TestKVService_StatusReportsExactlyOneLeader(t *testing.T) {
	ids := []raft.NodeID{"n1", "n2", "n3"}
	c := startKVCluster(t, ids)
	c.leaderID(t) // ensure a leader has settled

	ctx := context.Background()
	leaders := 0
	for _, id := range ids {
		resp, err := c.svc[id].Status(ctx, &api.StatusReq{})
		if err != nil {
			t.Fatalf("Status(%s): %v", id, err)
		}
		if len(resp.GetMemberIds()) != len(ids) {
			t.Fatalf("Status(%s) member count = %d, want %d",
				id, len(resp.GetMemberIds()), len(ids))
		}
		if resp.GetRole() == "Leader" {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("Status across cluster reported %d leaders, want 1", leaders)
	}
}

func TestKVService_CASMismatchIsNormalFailure(t *testing.T) {
	ids := []raft.NodeID{"n1", "n2", "n3"}
	c := startKVCluster(t, ids)
	leaderID := c.leaderID(t)
	svc := c.svc[leaderID]

	// callCtx returns a fresh, generously-bounded context per RPC so a slow
	// sequence of commits cannot exhaust a single shared deadline.
	callCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 3*time.Second)
	}

	// Seed a value, then attempt a CAS with the wrong expected value.
	ctx1, c1 := callCtx()
	defer c1()
	if _, err := svc.Put(ctx1, &api.PutReq{Key: "k", Value: []byte("one")}); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	ctx2, c2 := callCtx()
	defer c2()
	resp, err := svc.CAS(ctx2, &api.CASReq{
		Key:         "k",
		ExpectValue: []byte("WRONG"),
		NewValue:    []byte("two"),
	})
	if err != nil {
		t.Fatalf("CAS mismatch returned gRPC error: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("CAS with wrong expected value reported success")
	}
	if resp.GetLeaderHint() != "" {
		t.Fatalf("CAS mismatch carried leader_hint %q, want empty", resp.GetLeaderHint())
	}

	// A correct CAS must succeed.
	ctx3, c3 := callCtx()
	defer c3()
	ok, err := svc.CAS(ctx3, &api.CASReq{
		Key:         "k",
		ExpectValue: []byte("one"),
		NewValue:    []byte("two"),
	})
	if err != nil {
		t.Fatalf("correct CAS: %v", err)
	}
	if !ok.GetSuccess() {
		t.Fatal("correct CAS reported failure")
	}
}

func TestKVService_AddRemoveNodeUnimplemented(t *testing.T) {
	c := startKVCluster(t, []raft.NodeID{"n1"})
	svc := c.svc["n1"]
	if _, err := svc.AddNode(context.Background(), &api.AddNodeReq{}); err == nil {
		t.Fatal("AddNode: expected Unimplemented error")
	}
	if _, err := svc.RemoveNode(context.Background(), &api.RemoveNodeReq{}); err == nil {
		t.Fatal("RemoveNode: expected Unimplemented error")
	}
}
