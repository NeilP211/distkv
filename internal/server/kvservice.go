package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/NeilP211/distkv/api"
	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/store"
)

// KVService implements the client-facing KV gRPC service on top of a RaftNode.
//
// Leader-routing scheme: write RPCs (Put/Delete/CAS) and Get must run on the
// leader.  When this node is not the leader the RPC returns a NORMAL response
// (no gRPC error) with success=false and leader_hint set to the current
// leader's address (or its node id if no address mapping is known, or "" if
// the leader is currently unknown).  This matches the leader_hint field on the
// proto response messages, lets a client redirect without inspecting gRPC
// status details, and reserves gRPC error codes for genuine faults.  A CAS
// mismatch is likewise a normal response with success=false — it is an
// application outcome, not a routing failure, and carries no leader_hint.
type KVService struct {
	api.UnimplementedKVServer

	node *RaftNode
	// peerAddrs maps node id -> host:port so a not-leader response can hand
	// the client a dialable address.  It may be nil/partial; a missing entry
	// falls back to the bare node id string.
	peerAddrs map[raft.NodeID]string
}

// NewKVService constructs a KVService backed by node.  peerAddrs maps each
// member's node id to its client-facing address and is used to populate
// leader hints; it may be nil.
func NewKVService(node *RaftNode, peerAddrs map[raft.NodeID]string) *KVService {
	addrs := make(map[raft.NodeID]string, len(peerAddrs))
	for id, a := range peerAddrs {
		addrs[id] = a
	}
	return &KVService{node: node, peerAddrs: addrs}
}

// leaderHint returns the current leader's address (or node id, or "") for
// inclusion in a not-leader response.
func (s *KVService) leaderHint() string {
	leader := s.node.Status().Leader
	if leader == "" {
		return ""
	}
	if addr, ok := s.peerAddrs[leader]; ok {
		return addr
	}
	return string(leader)
}

// Put applies a put command through Raft.
func (s *KVService) Put(ctx context.Context, req *api.PutReq) (*api.PutResp, error) {
	_, err := s.node.Propose(ctx, store.Command{
		Op:    store.OpPut,
		Key:   req.GetKey(),
		Value: string(req.GetValue()),
	})
	if errors.Is(err, raft.ErrNotLeader) {
		return &api.PutResp{Success: false, LeaderHint: s.leaderHint()}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "put failed: %v", err)
	}
	return &api.PutResp{Success: true}, nil
}

// Delete applies a delete command through Raft.
func (s *KVService) Delete(ctx context.Context, req *api.DeleteReq) (*api.DeleteResp, error) {
	_, err := s.node.Propose(ctx, store.Command{
		Op:  store.OpDelete,
		Key: req.GetKey(),
	})
	if errors.Is(err, raft.ErrNotLeader) {
		return &api.DeleteResp{Success: false, LeaderHint: s.leaderHint()}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete failed: %v", err)
	}
	return &api.DeleteResp{Success: true}, nil
}

// CAS applies a compare-and-swap command through Raft.  A CAS mismatch is
// reported as a normal response with success=false (an application outcome,
// not a leader-routing failure).
func (s *KVService) CAS(ctx context.Context, req *api.CASReq) (*api.CASResp, error) {
	_, err := s.node.Propose(ctx, store.Command{
		Op:          store.OpCAS,
		Key:         req.GetKey(),
		Value:       string(req.GetNewValue()),
		ExpectValue: string(req.GetExpectValue()),
	})
	if errors.Is(err, raft.ErrNotLeader) {
		return &api.CASResp{Success: false, LeaderHint: s.leaderHint()}, nil
	}
	if errors.Is(err, store.ErrCASMismatch) {
		// Expected value did not match: a normal failed CAS, not a routing
		// error and not an RPC fault.
		return &api.CASResp{Success: false}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cas failed: %v", err)
	}
	return &api.CASResp{Success: true}, nil
}

// Get retrieves the value for a key.
//
// Phase 6b: replace with ReadIndex.  For now, only the leader serves reads —
// reading from its local state machine after confirming leadership.  A
// non-leader returns found=false; the GetResp proto has no leader_hint field,
// so a client detects "not leader here" by the absent value and should call
// Status to learn the leader.
func (s *KVService) Get(_ context.Context, req *api.GetReq) (*api.GetResp, error) {
	if s.node.Status().Role != raft.Leader.String() {
		// Not the leader: refuse to serve a possibly-stale read.
		return &api.GetResp{Found: false}, nil
	}
	v, ok := s.node.LocalGet(req.GetKey())
	return &api.GetResp{Value: []byte(v), Found: ok}, nil
}

// Status returns the current cluster status.
func (s *KVService) Status(_ context.Context, _ *api.StatusReq) (*api.StatusResp, error) {
	st := s.node.Status()
	members := make([]string, len(st.Members))
	for i, m := range st.Members {
		members[i] = string(m)
	}
	return &api.StatusResp{
		LeaderId:    string(st.Leader),
		Term:        st.Term,
		Role:        st.Role,
		CommitIndex: st.CommitIndex,
		MemberIds:   members,
	}, nil
}

// AddNode is not yet implemented.
func (s *KVService) AddNode(_ context.Context, _ *api.AddNodeReq) (*api.AddNodeResp, error) {
	// Phase 8: joint-consensus membership.
	return nil, status.Error(codes.Unimplemented, "AddNode not implemented")
}

// RemoveNode is not yet implemented.
func (s *KVService) RemoveNode(_ context.Context, _ *api.RemoveNodeReq) (*api.RemoveNodeResp, error) {
	// Phase 8: joint-consensus membership.
	return nil, status.Error(codes.Unimplemented, "RemoveNode not implemented")
}
