package transport

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/NeilP211/distkv/api"
	"github.com/NeilP211/distkv/internal/raft"
)

// callTimeout bounds a single Raft RPC.  It is deliberately short: a Raft
// message that cannot be delivered quickly is better treated as dropped (the
// algorithm tolerates lost messages and retries on the next heartbeat) than
// allowed to stall the sending node's Tick/Step path.
const callTimeout = 150 * time.Millisecond

// GRPCTransport is a raft.Transport that carries Raft messages over gRPC.  It
// lazily dials peers, caches the connections, and is safe for concurrent use.
type GRPCTransport struct {
	self  raft.NodeID
	peers map[raft.NodeID]string // node id -> host:port

	mu     sync.Mutex
	conns  map[raft.NodeID]*grpc.ClientConn
	closed bool
}

// NewGRPCTransport constructs a GRPCTransport.  peers maps each peer's node id
// to its host:port address; the self entry, if present, is never dialed.
func NewGRPCTransport(self raft.NodeID, peers map[raft.NodeID]string) *GRPCTransport {
	p := make(map[raft.NodeID]string, len(peers))
	for id, addr := range peers {
		p[id] = addr
	}
	return &GRPCTransport{
		self:  self,
		peers: p,
		conns: make(map[raft.NodeID]*grpc.ClientConn),
	}
}

// Send delivers msg to the peer identified by to and returns its response.
// Any dial or RPC failure — including an unknown peer or a timeout — is
// reported as ErrUnreachable so the Raft core treats it as a dropped message.
func (t *GRPCTransport) Send(to raft.NodeID, msg raft.Message) (raft.Message, error) {
	conn, err := t.connFor(to)
	if err != nil {
		return raft.Message{}, ErrUnreachable
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	resp, err := api.NewRaftServiceClient(conn).Step(ctx, ToProto(msg))
	if err != nil {
		return raft.Message{}, ErrUnreachable
	}
	return FromProto(resp), nil
}

// connFor returns a cached client connection for the peer, dialing one lazily
// on first use.  Dialing with grpc.NewClient does not block, so a connection
// to an unreachable peer is created here and fails later inside the RPC.
func (t *GRPCTransport) connFor(to raft.NodeID) (*grpc.ClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, ErrUnreachable
	}
	if c, ok := t.conns[to]; ok {
		return c, nil
	}

	addr, ok := t.peers[to]
	if !ok {
		return nil, ErrUnreachable
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[to] = conn
	return conn, nil
}

// Close closes every cached client connection.  It is idempotent.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	for id, c := range t.conns {
		_ = c.Close()
		delete(t.conns, id)
	}
	return nil
}

// RaftServer is the gRPC server side of the Raft transport.  Its Step method
// delivers an inbound message to a local handler — in Phase 6 this is
// RaftNode.Step — and returns the handler's response.
type RaftServer struct {
	api.UnimplementedRaftServiceServer
	step func(raft.Message) raft.Message
}

// NewRaftServer wraps a step handler as a RaftServiceServer suitable for
// registration with grpc.Server via api.RegisterRaftServiceServer.
func NewRaftServer(step func(raft.Message) raft.Message) *RaftServer {
	return &RaftServer{step: step}
}

// Step implements the RaftService gRPC method: it converts the wire message,
// dispatches it to the local handler, and returns the converted response.
func (s *RaftServer) Step(_ context.Context, in *api.Message) (*api.Message, error) {
	resp := s.step(FromProto(in))
	return ToProto(resp), nil
}
