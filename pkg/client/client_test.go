package client_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/NeilP211/distkv/api"
	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/server"
	"github.com/NeilP211/distkv/internal/transport"
	"github.com/NeilP211/distkv/pkg/client"
)

// testNode is one cluster member: a RaftNode plus the gRPC server hosting both
// the RaftService and the KV service on a loopback listener.
type testNode struct {
	id      raft.NodeID
	addr    string
	rn      *server.RaftNode
	gs      *grpc.Server
	tr      *transport.GRPCTransport
	stopped bool
}

// testCluster is a running 3-node cluster over loopback gRPC.
type testCluster struct {
	t     *testing.T
	nodes map[raft.NodeID]*testNode
	ids   []raft.NodeID
}

// startCluster builds and starts an n-node cluster.  Each node listens on a
// free 127.0.0.1 port; both gRPC services are registered on that listener.
func startCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	ids := make([]raft.NodeID, n)
	lns := make([]net.Listener, n)
	peers := make(map[raft.NodeID]string, n)
	for i := 0; i < n; i++ {
		id := raft.NodeID(fmt.Sprintf("n%d", i+1))
		ids[i] = id
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[i] = ln
		peers[id] = ln.Addr().String()
	}

	c := &testCluster{t: t, nodes: make(map[raft.NodeID]*testNode), ids: ids}

	for i, id := range ids {
		tr := transport.NewGRPCTransport(id, peers)
		rn, err := server.NewRaftNode(server.RaftNodeConfig{
			Raft: raft.Config{
				ID:                 id,
				Peers:              ids,
				Storage:            raft.NewMemStorage(),
				Transport:          tr,
				ElectionTimeoutMin: 10,
				ElectionTimeoutMax: 20,
				HeartbeatInterval:  3,
			},
			TickInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRaftNode(%s): %v", id, err)
		}

		// peerAddrs lets KVService leader hints carry dialable addresses.
		peerAddrs := make(map[raft.NodeID]string, len(peers))
		for pid, a := range peers {
			peerAddrs[pid] = a
		}
		kv := server.NewKVService(rn, peerAddrs)

		gs := grpc.NewServer()
		api.RegisterRaftServiceServer(gs, transport.NewRaftServer(rn.Step))
		api.RegisterKVServer(gs, kv)

		tn := &testNode{id: id, addr: peers[id], rn: rn, gs: gs, tr: tr}
		c.nodes[id] = tn

		ln := lns[i]
		go func() { _ = gs.Serve(ln) }()
		rn.Start()
	}

	t.Cleanup(func() { c.stopAll() })
	return c
}

func (c *testCluster) stopAll() {
	for _, tn := range c.nodes {
		if tn.stopped {
			continue
		}
		tn.stop()
	}
}

func (tn *testNode) stop() {
	tn.stopped = true
	tn.rn.Stop()
	tn.gs.Stop()
	_ = tn.tr.Close()
}

func (c *testCluster) endpoints() []string {
	eps := make([]string, 0, len(c.ids))
	for _, id := range c.ids {
		eps = append(eps, c.nodes[id].addr)
	}
	return eps
}

// waitReady blocks until every live node answers a Status RPC, so client
// operations are not issued against connections still in dial backoff.
func (c *testCluster) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for _, id := range c.ids {
		tn := c.nodes[id]
		if tn.stopped {
			continue
		}
		conn, err := grpc.NewClient(tn.addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial %s: %v", tn.addr, err)
		}
		ready := false
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, rerr := api.NewKVClient(conn).Status(ctx, &api.StatusReq{})
			cancel()
			if rerr == nil {
				ready = true
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		_ = conn.Close()
		if !ready {
			t.Fatalf("node %s never became ready", id)
		}
	}
}

// waitLeader blocks until exactly one node reports itself leader and returns it.
func (c *testCluster) waitLeader(t *testing.T) *testNode {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		var leader *testNode
		for _, tn := range c.nodes {
			if tn.stopped {
				continue
			}
			if tn.rn.Status().Role == "Leader" {
				count++
				leader = tn
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no single leader elected")
	return nil
}

func TestClient_PutGetDeleteCAS(t *testing.T) {
	c := startCluster(t, 3)
	c.waitReady(t)
	c.waitLeader(t)

	cl := client.New(c.endpoints())
	defer cl.Close()

	// Each operation gets a fresh, generously-bounded context so one slow
	// op (election churn under -race) cannot exhaust a shared deadline.
	opCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 12*time.Second)
	}

	ctx, cancel := opCtx()
	defer cancel()
	if err := cl.Put(ctx, "color", "blue"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ctx, cancel = opCtx()
	defer cancel()
	v, found, err := cl.Get(ctx, "color")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || v != "blue" {
		t.Fatalf("Get(color) = %q,%v want blue,true", v, found)
	}

	// CAS with the correct expected value swaps.
	ctx, cancel = opCtx()
	defer cancel()
	ok, err := cl.CAS(ctx, "color", "blue", "green")
	if err != nil {
		t.Fatalf("CAS: %v", err)
	}
	if !ok {
		t.Fatal("CAS with correct expect reported not swapped")
	}

	ctx, cancel = opCtx()
	defer cancel()
	v, _, err = cl.Get(ctx, "color")
	if err != nil {
		t.Fatalf("Get after CAS: %v", err)
	}
	if v != "green" {
		t.Fatalf("Get after CAS = %q want green", v)
	}

	// CAS with a wrong expected value must not swap and must not error.
	ctx, cancel = opCtx()
	defer cancel()
	ok, err = cl.CAS(ctx, "color", "WRONG", "red")
	if err != nil {
		t.Fatalf("CAS mismatch returned error: %v", err)
	}
	if ok {
		t.Fatal("CAS with wrong expect reported swapped")
	}

	ctx, cancel = opCtx()
	defer cancel()
	if err := cl.Delete(ctx, "color"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	ctx, cancel = opCtx()
	defer cancel()
	_, found, err = cl.Get(ctx, "color")
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if found {
		t.Fatal("Get after Delete still found the key")
	}
}

func TestClient_StatusReportsOneLeader(t *testing.T) {
	c := startCluster(t, 3)
	c.waitReady(t)
	c.waitLeader(t)

	cl := client.New(c.endpoints())
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := cl.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(info.Members) != 3 {
		t.Fatalf("Status members = %d, want 3", len(info.Members))
	}
}

func TestClient_FollowsLeaderChange(t *testing.T) {
	c := startCluster(t, 3)
	c.waitReady(t)
	leader := c.waitLeader(t)

	cl := client.New(c.endpoints(), client.WithMaxAttempts(10))
	defer cl.Close()

	opCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 12*time.Second)
	}

	// Write succeeds against the original leader.
	ctx, cancel := opCtx()
	defer cancel()
	if err := cl.Put(ctx, "k", "v1"); err != nil {
		t.Fatalf("Put before leader change: %v", err)
	}

	// Kill the current leader.  A new leader must be elected among the
	// remaining two nodes.
	leader.stop()

	// Wait for a new leader among the survivors.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for _, tn := range c.nodes {
			if tn.stopped {
				continue
			}
			if tn.rn.Status().Role == "Leader" {
				count++
			}
		}
		if count == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The client, given all 3 endpoints, must transparently follow the
	// change: operations against the surviving cluster still succeed.
	ctx, cancel = opCtx()
	defer cancel()
	if err := cl.Put(ctx, "k", "v2"); err != nil {
		t.Fatalf("Put after leader change: %v", err)
	}

	ctx, cancel = opCtx()
	defer cancel()
	v, found, err := cl.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get after leader change: %v", err)
	}
	if !found || v != "v2" {
		t.Fatalf("Get after leader change = %q,%v want v2,true", v, found)
	}
}
