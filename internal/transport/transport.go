// Package transport defines the abstract RPC interface used by the Raft core
// to send messages to other cluster members.  Concrete implementations
// include the gRPC transport (Phase 6) and the in-process network simulator
// (internal/simnet, Phase 3).
package transport

import (
	"errors"

	"github.com/NeilP211/distkv/internal/raft"
)

// ErrUnreachable is returned by Transport.Send when the destination node
// cannot be contacted (partitioned away, crashed, or the message was
// randomly dropped by fault injection).
var ErrUnreachable = errors.New("transport: destination unreachable")

// Transport is the interface that the Raft core uses to deliver a message to
// another cluster member and receive the synchronous response.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Transport interface {
	// Send delivers msg to the node identified by to and returns the
	// response message produced by the recipient's Step function.
	// If the destination is unreachable, Send returns ErrUnreachable.
	Send(to raft.NodeID, msg raft.Message) (raft.Message, error)
}
