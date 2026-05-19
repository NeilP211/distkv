// Package server wires the Raft core (internal/raft) into a runnable node:
// the RaftNode driver owns the real-time ticker and the apply loop, and the
// KVService exposes the client-facing gRPC API on top of it.
package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"sync"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/store"
)

// applyPoll is how often the apply loop wakes to drain newly committed
// entries.  Proposals also signal the loop directly so latency does not
// depend on this interval; it is only a safety net for entries committed
// by background replication (heartbeat acks).
const applyPoll = 5 * time.Millisecond

// maxPendingResults bounds the buffer of apply results awaiting a racing
// Propose to claim them.  The buffer only needs to cover the brief window
// between a Propose committing an entry and that same Propose registering its
// waiter, so a small cap is ample; the lowest indices are evicted past it.
const maxPendingResults = 64

// RaftNodeConfig configures a RaftNode: the embedded raft.Config plus the
// real-time tick duration that drives raft.Node.Tick.
type RaftNodeConfig struct {
	// Raft is the configuration passed straight to raft.NewNode.
	Raft raft.Config
	// TickInterval is the wall-clock period between raft.Node.Tick calls.
	// Election/heartbeat timeouts in Raft.Config are expressed in Ticks, so
	// the effective timeouts are TickInterval multiplied by those counts.
	TickInterval time.Duration
}

// proposalResult carries the outcome of applying a proposed command back to
// the goroutine blocked in Propose.
type proposalResult struct {
	value string
	err   error
}

// RaftNode drives a raft.Node in real time and applies committed entries to a
// store.Store state machine.  It owns two goroutines started by Start: a
// ticker and an apply loop.  All exported methods are safe for concurrent use.
type RaftNode struct {
	node    *raft.Node
	sm      *store.Store
	members []raft.NodeID // cluster members, captured at construction

	tickInterval time.Duration

	mu       sync.Mutex
	started  bool
	stopped  bool
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	applyNow chan struct{} // signalled by Propose to wake the apply loop

	// waiters maps a log index to the channel awaiting that entry's apply
	// result.  Guarded by mu.
	waiters map[uint64]chan proposalResult

	// pending buffers apply results for entries that were applied before
	// their Propose call had a chance to register a waiter.  raft.Propose
	// can commit an entry synchronously (and the apply loop can then drain
	// it) in the window between node.Propose returning and Propose
	// registering its waiter; without this buffer that result would be lost
	// and Propose would block forever.  Guarded by mu; entries are removed
	// once claimed by Propose or when the node stops.
	pending map[uint64]proposalResult

	// applied is the highest log index whose entry has been applied to the
	// state machine.  Guarded by mu; read by LinearizableGet to know when a
	// ReadIndex point has been reached.
	applied uint64
}

// NewRaftNode constructs a RaftNode and its underlying raft.Node.  It does NOT
// start any goroutines; call Start for that.
func NewRaftNode(cfg RaftNodeConfig) (*RaftNode, error) {
	if cfg.TickInterval <= 0 {
		return nil, errors.New("server: TickInterval must be positive")
	}
	node, err := raft.NewNode(cfg.Raft)
	if err != nil {
		return nil, err
	}
	members := make([]raft.NodeID, len(cfg.Raft.Peers))
	copy(members, cfg.Raft.Peers)
	return &RaftNode{
		node:         node,
		sm:           store.New(),
		members:      members,
		tickInterval: cfg.TickInterval,
		applyNow:     make(chan struct{}, 1),
		waiters:      make(map[uint64]chan proposalResult),
		pending:      make(map[uint64]proposalResult),
	}, nil
}

// Start launches the ticker and apply-loop goroutines.  It is safe to call
// once; subsequent calls are no-ops.
func (rn *RaftNode) Start() {
	rn.mu.Lock()
	if rn.started || rn.stopped {
		rn.mu.Unlock()
		return
	}
	rn.started = true
	ctx, cancel := context.WithCancel(context.Background())
	rn.cancel = cancel
	rn.mu.Unlock()

	rn.wg.Add(2)
	go rn.tickLoop(ctx)
	go rn.applyLoop(ctx)
}

// Stop cleanly stops the ticker and apply-loop goroutines.  It is idempotent
// and blocks until both goroutines have exited.
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return
	}
	rn.stopped = true
	cancel := rn.cancel
	rn.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	rn.wg.Wait()

	// Fail any still-blocked proposal waiters so callers unblock promptly,
	// and drop buffered results no Propose will ever claim.
	rn.mu.Lock()
	for idx, ch := range rn.waiters {
		close(ch)
		delete(rn.waiters, idx)
	}
	for idx := range rn.pending {
		delete(rn.pending, idx)
	}
	rn.mu.Unlock()
}

// tickLoop calls raft.Node.Tick on a fixed wall-clock cadence.
func (rn *RaftNode) tickLoop(ctx context.Context) {
	defer rn.wg.Done()
	t := time.NewTicker(rn.tickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rn.node.Tick()
			// A Tick may have advanced commitIndex (heartbeat acks, or a
			// single-node leader self-committing); nudge the apply loop so
			// it drains promptly.
			rn.signalApply()
		}
	}
}

// applyLoop drains committed-not-applied entries and applies them to the
// state machine, delivering results to any registered proposal waiters.
func (rn *RaftNode) applyLoop(ctx context.Context) {
	defer rn.wg.Done()
	t := time.NewTicker(applyPoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			rn.drainReady() // final drain so committed entries are not lost
			return
		case <-t.C:
			rn.drainReady()
		case <-rn.applyNow:
			rn.drainReady()
		}
	}
}

// drainReady applies every committed-but-unapplied entry exactly once and
// advances the applied-index watermark.
func (rn *RaftNode) drainReady() {
	for _, e := range rn.node.Ready() {
		rn.applyEntry(e)
		rn.mu.Lock()
		if e.Index > rn.applied {
			rn.applied = e.Index
		}
		rn.mu.Unlock()
	}
}

// appliedIndex returns the highest log index applied to the state machine.
func (rn *RaftNode) appliedIndex() uint64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.applied
}

// applyEntry applies a single log entry to the state machine and signals the
// proposal waiter (if any) registered for the entry's index.
func (rn *RaftNode) applyEntry(e raft.LogEntry) {
	if e.Type != raft.EntryNormal {
		// EntryNoop / EntryConfChange carry no application command.
		return
	}
	cmd, decErr := decodeCommand(e.Data)
	var res proposalResult
	if decErr != nil {
		res.err = decErr
	} else {
		res.value, res.err = rn.sm.Apply(cmd)
	}

	rn.mu.Lock()
	ch, ok := rn.waiters[e.Index]
	if ok {
		delete(rn.waiters, e.Index)
	} else {
		// No waiter yet: either the local Propose that produced this entry
		// has not finished registering, or this entry was replicated from
		// another leader (no local Propose at all).  Buffer the result so a
		// racing Propose can claim it, but cap the buffer and evict the
		// lowest-index stale entries so a follower applying a long stream
		// of replicated entries cannot leak memory.
		rn.pending[e.Index] = res
		for len(rn.pending) > maxPendingResults {
			var oldest uint64
			first := true
			for idx := range rn.pending {
				if first || idx < oldest {
					oldest, first = idx, false
				}
			}
			delete(rn.pending, oldest)
		}
	}
	rn.mu.Unlock()

	if ok {
		ch <- res
		close(ch)
	}
}

// signalApply wakes the apply loop without blocking; the buffered channel
// coalesces multiple signals.
func (rn *RaftNode) signalApply() {
	select {
	case rn.applyNow <- struct{}{}:
	default:
	}
}

// Propose encodes cmd, appends it to the Raft log via the leader, and blocks
// until the apply loop has applied that entry (returning its result) or ctx is
// done.  It returns raft.ErrNotLeader if this node is not the leader.
func (rn *RaftNode) Propose(ctx context.Context, cmd store.Command) (string, error) {
	data, err := encodeCommand(cmd)
	if err != nil {
		return "", err
	}

	idx, err := rn.node.Propose(data)
	if err != nil {
		return "", err
	}

	ch := make(chan proposalResult, 1)
	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return "", errors.New("server: RaftNode stopped")
	}
	// The apply loop may already have applied this entry in the window
	// between node.Propose returning and now (raft.Propose can commit
	// synchronously).  If so the result is buffered in pending; claim it
	// directly rather than registering a waiter that would never fire.
	if res, ok := rn.pending[idx]; ok {
		delete(rn.pending, idx)
		rn.mu.Unlock()
		return res.value, res.err
	}
	rn.waiters[idx] = ch
	rn.mu.Unlock()

	// raft.Propose may already have committed the entry (a single-node
	// leader is its own majority); nudge the apply loop so it drains
	// without waiting for the next applyPoll tick.
	rn.signalApply()

	defer func() {
		// Clean up the waiter and any unclaimed buffered result on every
		// exit path.
		rn.mu.Lock()
		delete(rn.waiters, idx)
		delete(rn.pending, idx)
		rn.mu.Unlock()
	}()

	select {
	case res, ok := <-ch:
		if !ok {
			// Channel closed without a value: node stopped mid-proposal.
			return "", errors.New("server: RaftNode stopped before apply")
		}
		return res.value, res.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// LocalGet reads a key directly from the local state machine.  It is NOT
// linearizable (Phase 6b adds ReadIndex); it exists for tests and local
// inspection.
func (rn *RaftNode) LocalGet(key string) (string, bool) {
	return rn.sm.Get(key)
}

// Step delivers an inbound Raft message to the underlying node and returns the
// response.  It is the entry point a transport's server side wires to.
func (rn *RaftNode) Step(msg raft.Message) raft.Message {
	resp := rn.node.Step(msg)
	// A Step may have advanced commitIndex; nudge the apply loop.
	rn.signalApply()
	return resp
}

// Status is a snapshot of the RaftNode's Raft state.
type Status struct {
	// ID is this node's identifier.
	ID raft.NodeID
	// Role is the node's current role ("Leader"/"Follower"/"Candidate").
	Role string
	// Term is the current election term.
	Term uint64
	// Leader is the currently known leader ("" if unknown).
	Leader raft.NodeID
	// CommitIndex is the highest log index known to be committed.
	CommitIndex uint64
	// Members lists every cluster member, including self.
	Members []raft.NodeID
}

// Status returns a snapshot of the node's current Raft state.
func (rn *RaftNode) Status() Status {
	return Status{
		ID:          rn.node.ID(),
		Role:        rn.node.Role().String(),
		Term:        rn.node.Term(),
		Leader:      rn.node.Leader(),
		CommitIndex: rn.node.CommitIndex(),
		Members:     rn.members,
	}
}

// encodeCommand serializes a store.Command with gob — the same codec the
// store package uses for its WAL records, ensuring round-trip consistency.
func encodeCommand(cmd store.Command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeCommand is the inverse of encodeCommand.
func decodeCommand(data []byte) (store.Command, error) {
	var cmd store.Command
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd); err != nil {
		return store.Command{}, err
	}
	return cmd, nil
}
