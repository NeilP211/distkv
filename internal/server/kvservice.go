package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/NeilP211/distkv/api"
	"github.com/NeilP211/distkv/internal/metrics"
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
		metrics.KVRequest("put", "notleader")
		return &api.PutResp{Success: false, LeaderHint: s.leaderHint()}, nil
	}
	if err != nil {
		metrics.KVRequest("put", "error")
		return nil, status.Errorf(codes.Internal, "put failed: %v", err)
	}
	metrics.KVRequest("put", "ok")
	return &api.PutResp{Success: true}, nil
}

// Delete applies a delete command through Raft.
func (s *KVService) Delete(ctx context.Context, req *api.DeleteReq) (*api.DeleteResp, error) {
	_, err := s.node.Propose(ctx, store.Command{
		Op:  store.OpDelete,
		Key: req.GetKey(),
	})
	if errors.Is(err, raft.ErrNotLeader) {
		metrics.KVRequest("delete", "notleader")
		return &api.DeleteResp{Success: false, LeaderHint: s.leaderHint()}, nil
	}
	if err != nil {
		metrics.KVRequest("delete", "error")
		return nil, status.Errorf(codes.Internal, "delete failed: %v", err)
	}
	metrics.KVRequest("delete", "ok")
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
		metrics.KVRequest("cas", "notleader")
		return &api.CASResp{Success: false, LeaderHint: s.leaderHint()}, nil
	}
	if errors.Is(err, store.ErrCASMismatch) {
		// Expected value did not match: a normal failed CAS, not a routing
		// error and not an RPC fault.
		metrics.KVRequest("cas", "error")
		return &api.CASResp{Success: false}, nil
	}
	if err != nil {
		metrics.KVRequest("cas", "error")
		return nil, status.Errorf(codes.Internal, "cas failed: %v", err)
	}
	metrics.KVRequest("cas", "ok")
	return &api.CASResp{Success: true}, nil
}

// Get retrieves the value for a key with a linearizable read (ReadIndex).
//
// Only the leader serves reads, and only after confirming via a heartbeat
// round that it still holds leadership — so a deposed leader cannot return
// stale data.  A non-leader returns a normal response with found=false; the
// GetResp proto has no leader_hint field, so a client detects "not leader
// here" by the absent value and should call Status to learn the leader.  A
// leader that loses leadership mid-read returns codes.Unavailable so the
// client retries against another node.
func (s *KVService) Get(ctx context.Context, req *api.GetReq) (*api.GetResp, error) {
	v, found, err := s.node.LinearizableGet(ctx, req.GetKey())
	if errors.Is(err, raft.ErrNotLeader) {
		// Not the leader: refuse to serve a possibly-stale read.
		metrics.KVRequest("get", "notleader")
		return &api.GetResp{Found: false}, nil
	}
	if errors.Is(err, ErrLeadershipLost) {
		metrics.KVRequest("get", "unavailable")
		return nil, status.Error(codes.Unavailable, "leadership lost during read")
	}
	if err != nil {
		metrics.KVRequest("get", "error")
		return nil, status.Errorf(codes.Internal, "get failed: %v", err)
	}
	metrics.KVRequest("get", "ok")
	return &api.GetResp{Value: []byte(v), Found: found}, nil
}

// Status returns the current cluster status.
func (s *KVService) Status(_ context.Context, _ *api.StatusReq) (*api.StatusResp, error) {
	st := s.node.Status()
	members := make([]string, len(st.Members))
	for i, m := range st.Members {
		members[i] = string(m)
	}
	metrics.KVRequest("status", "ok")
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
