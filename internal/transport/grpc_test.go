package transport_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/NeilP211/distkv/api"
	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/server"
	"github.com/NeilP211/distkv/internal/store"
	"github.com/NeilP211/distkv/internal/transport"
)

const testTick = 10 * time.Millisecond

// grpcPeer bundles a RaftNode, its gRPC server, and its transport so the test
// can wire and tear them down together.
type grpcPeer struct {
	id   raft.NodeID
	node *server.RaftNode
	tr   *transport.GRPCTransport
	srv  *grpc.Server
	lis  net.Listener
}

// startGRPCCluster builds an n-node cluster wired with GRPCTransport over
// loopback TCP.  Listeners are bound first (to learn the free ports), then the
// nodes are constructed, servers registered, and everything started.
func startGRPCCluster(t *testing.T, ids []raft.NodeID) map[raft.NodeID]*grpcPeer {
	t.Helper()

	// Bind a listener per node first so peer addresses are known up front.
	addrs := make(map[raft.NodeID]string, len(ids))
	listeners := make(map[raft.NodeID]net.Listener, len(ids))
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen for %s: %v", id, err)
		}
		listeners[id] = lis
		addrs[id] = lis.Addr().String()
	}

	peers := make(map[raft.NodeID]*grpcPeer, len(ids))
	for _, id := range ids {
		tr := transport.NewGRPCTransport(id, addrs)
		node, err := server.NewRaftNode(server.RaftNodeConfig{
			Raft: raft.Config{
				ID:                 id,
				Peers:              ids,
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
		srv := grpc.NewServer()
		api.RegisterRaftServiceServer(srv, transport.NewRaftServer(node.Step))
		peers[id] = &grpcPeer{id: id, node: node, tr: tr, srv: srv, lis: listeners[id]}
	}

	// Start serving, then start the raft drivers.
	for _, p := range peers {
		p := p
		go func() { _ = p.srv.Serve(p.lis) }()
	}
	for _, p := range peers {
		p.node.Start()
	}

	t.Cleanup(func() {
		for _, p := range peers {
			p.node.Stop()
			p.srv.Stop()
			_ = p.tr.Close()
		}
	})
	return peers
}

// waitFor polls cond until true or the deadline elapses.
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

func TestGRPCTransport_ElectAndReplicate(t *testing.T) {
	ids := []raft.NodeID{"n1", "n2"}
	peers := startGRPCCluster(t, ids)

	// A leader must emerge.
	waitFor(t, 5*time.Second, "a leader to be elected", func() bool {
		for _, p := range peers {
			if p.node.Status().Role == "Leader" {
				return true
			}
		}
		return false
	})

	// Propose against whichever node is currently the leader, retrying on a
	// leadership change (a 2-node cluster can re-elect under timing jitter).
	proposed := false
	waitFor(t, 8*time.Second, "a write to commit on the leader", func() bool {
		var leader *grpcPeer
		for _, p := range peers {
			if p.node.Status().Role == "Leader" {
				leader = p
			}
		}
		if leader == nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := leader.node.Propose(ctx, store.Command{Op: store.OpPut, Key: "k", Value: "v"})
		if err == nil {
			proposed = true
			return true
		}
		return false
	})
	if !proposed {
		t.Fatal("no successful Propose")
	}

	// The write must replicate to every node.
	for id, p := range peers {
		p := p
		waitFor(t, 3*time.Second, "node "+string(id)+" to apply", func() bool {
			v, ok := p.node.LocalGet("k")
			return ok && v == "v"
		})
	}
}

func TestGRPCTransport_SendToUnknownPeerIsUnreachable(t *testing.T) {
	tr := transport.NewGRPCTransport("n1", map[raft.NodeID]string{})
	defer tr.Close()

	_, err := tr.Send("ghost", raft.Message{Type: raft.MsgRequestVote})
	if err != transport.ErrUnreachable {
		t.Fatalf("Send to unknown peer err = %v, want ErrUnreachable", err)
	}
}

func TestGRPCTransport_SendToDeadPeerIsUnreachable(t *testing.T) {
	// Bind a port, then close it so nothing is listening.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	tr := transport.NewGRPCTransport("n1", map[raft.NodeID]string{"n2": addr})
	defer tr.Close()

	start := time.Now()
	_, err = tr.Send("n2", raft.Message{Type: raft.MsgRequestVote})
	if err != transport.ErrUnreachable {
		t.Fatalf("Send to dead peer err = %v, want ErrUnreachable", err)
	}
	// The short per-call timeout must bound the stall.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Send to dead peer took %s, expected prompt failure", elapsed)
	}
}
