// Package simnet provides an in-process, fault-injecting network simulator
// for deterministic Raft testing.  All registered nodes share a single
// *Network; each node communicates through its own transport.Transport handle
// returned by Network.Node.  All randomness is driven by a seeded rand.Rand
// so a given seed reproduces identical fault patterns across runs.
package simnet

import (
	"math/rand"
	"sync"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/transport"
)

// Network is an in-process simulated network that routes messages between
// registered nodes and supports deterministic fault injection.
//
// All exported methods are safe for concurrent use.
type Network struct {
	mu sync.Mutex

	rng *rand.Rand

	// handlers maps a NodeID to its registered Step function.
	handlers map[raft.NodeID]func(raft.Message) raft.Message

	// crashed/isolated nodes never send or receive.
	down map[raft.NodeID]bool

	// partition holds the current partition configuration.
	// Each entry is a set of NodeIDs that can communicate with each other.
	// When nil, no partition is active (all nodes can reach all nodes).
	partition []map[raft.NodeID]bool

	// dropRate is the probability [0,1] that a message is dropped.
	dropRate float64

	// delay configuration (zero means no delay).
	minDelay time.Duration
	maxDelay time.Duration
}

// NewNetwork creates a new Network whose random-number generator is seeded
// with seed, guaranteeing deterministic fault injection for a given seed.
func NewNetwork(seed int64) *Network {
	return &Network{
		handlers: make(map[raft.NodeID]func(raft.Message) raft.Message),
		down:     make(map[raft.NodeID]bool),
		rng:      rand.New(rand.NewSource(seed)), //nolint:gosec
	}
}

// Register associates a Step handler with a node id.  The handler is invoked
// synchronously by Send when a message arrives for id.  Registering the same
// id twice replaces the previous handler.
func (n *Network) Register(id raft.NodeID, handler func(raft.Message) raft.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = handler
}

// Node returns a Transport that, when used to Send, pretends to originate
// from id.  The returned handle holds no state of its own; all routing and
// fault injection state lives in the parent Network.
func (n *Network) Node(id raft.NodeID) transport.Transport {
	return &nodeTransport{net: n, id: id}
}

// Partition installs a network partition.  Each group is a slice of NodeIDs
// that can communicate with one another; nodes in different groups cannot.
// Calling Partition replaces any previously installed partition.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partition = make([]map[raft.NodeID]bool, len(groups))
	for i, g := range groups {
		s := make(map[raft.NodeID]bool, len(g))
		for _, id := range g {
			s[id] = true
		}
		n.partition[i] = s
	}
}

// Heal removes all active partitions, restoring full connectivity
// (subject to drop rate and individual node crashes/isolations).
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partition = nil
}

// SetDrop sets the probability that any given message is randomly dropped.
// rate must be in [0, 1].  A rate of 1.0 drops every message.
func (n *Network) SetDrop(rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = rate
}

// SetDelay configures a random per-message delivery delay drawn uniformly
// from [min, max].  Both values are applied synchronously inside Send
// (the goroutine calling Send sleeps for the chosen duration).
func (n *Network) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minDelay = min
	n.maxDelay = max
}

// Isolate marks id as isolated: it cannot send to or receive from any peer.
// Use Recover to restore connectivity.
func (n *Network) Isolate(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = true
}

// Crash marks id as crashed: it cannot send to or receive from any peer.
// Semantically identical to Isolate; both are undone by Recover.
func (n *Network) Crash(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = true
}

// Recover removes a crash or isolation previously set for id, allowing it to
// send and receive messages again.
func (n *Network) Recover(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.down, id)
}

// send is the internal implementation of Send.  It is called by nodeTransport
// and must be called with n.mu NOT held (it acquires the lock internally
// to read state and advance the rng).
func (n *Network) send(from raft.NodeID, to raft.NodeID, msg raft.Message) (raft.Message, error) {
	n.mu.Lock()

	// Check if source or destination is down.
	if n.down[from] || n.down[to] {
		n.mu.Unlock()
		return raft.Message{}, transport.ErrUnreachable
	}

	// Check partition.
	if n.partition != nil && !n.sameGroup(from, to) {
		n.mu.Unlock()
		return raft.Message{}, transport.ErrUnreachable
	}

	// Check drop rate.
	dropped := n.dropRate > 0 && n.rng.Float64() < n.dropRate

	// Sample delay.
	var delay time.Duration
	if n.maxDelay > n.minDelay {
		delay = n.minDelay + time.Duration(n.rng.Int63n(int64(n.maxDelay-n.minDelay)))
	} else if n.maxDelay > 0 {
		delay = n.minDelay
	}

	// Retrieve the destination handler.
	handler, ok := n.handlers[to]

	n.mu.Unlock()

	if dropped {
		return raft.Message{}, transport.ErrUnreachable
	}

	if !ok {
		// No handler registered — treat as unreachable.
		return raft.Message{}, transport.ErrUnreachable
	}

	// Apply delay outside the lock.
	if delay > 0 {
		time.Sleep(delay)
	}

	resp := handler(msg)
	return resp, nil
}

// sameGroup reports whether from and to are in the same partition group.
// Must be called with n.mu held.
func (n *Network) sameGroup(from, to raft.NodeID) bool {
	for _, g := range n.partition {
		if g[from] && g[to] {
			return true
		}
	}
	return false
}

// nodeTransport is the per-node Transport handle returned by Network.Node.
type nodeTransport struct {
	net *Network
	id  raft.NodeID
}

// Send implements transport.Transport.  It delivers msg to the node
// identified by to via the parent Network's routing and fault-injection logic.
func (t *nodeTransport) Send(to raft.NodeID, msg raft.Message) (raft.Message, error) {
	return t.net.send(t.id, to, msg)
}
