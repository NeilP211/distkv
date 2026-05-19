package server

import (
	"context"
	"errors"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
)

// ErrLeadershipLost is returned by LinearizableGet when the node was the
// leader but could not confirm it still holds leadership via a heartbeat
// round to a majority — for example a leader partitioned away from the rest
// of the cluster.  The caller maps this to a retryable gRPC Unavailable: the
// read was refused (NOT served with stale data) and may succeed elsewhere.
var ErrLeadershipLost = errors.New("server: leadership lost during read")

// readIndexPoll is how often LinearizableGet checks whether the apply loop
// has caught up to the captured readIndex.
const readIndexPoll = 2 * time.Millisecond

// LinearizableGet performs a linearizable read of key using the ReadIndex
// protocol:
//
//  1. The node must be the leader; otherwise it returns raft.ErrNotLeader.
//  2. It confirms it still holds leadership by exchanging a heartbeat round
//     with a majority (raft.Node.ConfirmLeadership).  If that fails it
//     returns ErrLeadershipLost rather than serving a possibly-stale value.
//  3. It waits until the apply loop has applied every entry up to the
//     readIndex captured in step 2, bounded by ctx.
//  4. It reads key from the local state machine and returns the result.
func (rn *RaftNode) LinearizableGet(ctx context.Context, key string) (value string, found bool, err error) {
	if rn.node.Role() != raft.Leader {
		return "", false, raft.ErrNotLeader
	}

	readIndex, ok := rn.node.ConfirmLeadership()
	if !ok {
		return "", false, ErrLeadershipLost
	}

	// Wait for the state machine to catch up to readIndex.  Nudge the apply
	// loop so committed-but-unapplied entries drain promptly.
	rn.signalApply()
	for rn.appliedIndex() < readIndex {
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case <-time.After(readIndexPoll):
			rn.signalApply()
		}
	}

	v, f := rn.sm.Get(key)
	return v, f, nil
}
