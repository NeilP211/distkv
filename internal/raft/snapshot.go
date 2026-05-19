package raft

import (
	"errors"
	"fmt"
)

// ErrSnapshotOutOfRange is returned by CompactTo when the requested snapshot
// index is greater than the last applied index — a node must not snapshot
// state it has not yet applied.
var ErrSnapshotOutOfRange = errors.New("raft: snapshot index exceeds lastApplied")

// CompactTo records a state-machine snapshot covering log indices up to and
// including index, then compacts the log so every entry at or below index is
// discarded.  After it returns, Storage.FirstIndex advances to index+1.
//
// term must be the term of the log entry at index; the caller (the server
// layer) obtains it via TermOf.  data is the opaque serialised state-machine
// image (store.Store.Snapshot()).
//
// Validation:
//   - index must not exceed lastApplied: a node cannot snapshot state it has
//     not applied.  Violating this returns ErrSnapshotOutOfRange.
//   - If index is already covered by an existing snapshot (index < FirstIndex,
//     i.e. index <= snapshotIndex) CompactTo is a no-op and returns nil — the
//     log point has already been compacted away, so re-recording it is
//     harmless and need not be an error.
//
// CompactTo acquires the node mutex.
func (n *Node) CompactTo(index, term uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index > n.lastApplied {
		return fmt.Errorf("%w: index=%d lastApplied=%d", ErrSnapshotOutOfRange, index, n.lastApplied)
	}

	first, err := n.storage.FirstIndex()
	if err != nil {
		return err
	}
	if index < first {
		// Already compacted past this point; nothing to do.
		return nil
	}

	snap := Snapshot{Index: index, Term: term, Data: data, Conf: n.clusterConfig.clone()}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		return err
	}
	// SaveSnapshot already advances FirstIndex (via snapIndex); Compact then
	// physically discards the now-subsumed log entries.  Compact is a no-op on
	// stores (MemStorage) that discard during SaveSnapshot, so calling both is
	// safe and keeps BoltStorage's on-disk log trimmed.
	if err := n.storage.Compact(index); err != nil {
		return err
	}
	return nil
}

// TermOf returns the term of the log entry (or snapshot point) at index.  It
// is the accessor the server layer uses to obtain the term argument for
// CompactTo.  It returns an error if the index has been compacted away or is
// beyond the end of the log.
func (n *Node) TermOf(index uint64) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.term(index)
}

// FirstIndex returns the index of the first log entry the node can still serve
// (1 + the snapshot index, or 1 if there is no snapshot).
func (n *Node) FirstIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	first, err := n.storage.FirstIndex()
	if err != nil {
		panic("raft: storage.FirstIndex failed: " + err.Error())
	}
	return first
}

// PendingSnapshot returns a snapshot that was just installed via
// MsgInstallSnapshot and has not yet been handed to the state machine, and
// true.  It returns (Snapshot{}, false) when there is no such snapshot.  The
// snapshot is cleared on return, so each installed snapshot is delivered
// exactly once.  The server's apply loop calls this every cycle.
func (n *Node) PendingSnapshot() (Snapshot, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.hasPendingSnap {
		return Snapshot{}, false
	}
	snap := n.pendingSnap
	n.pendingSnap = Snapshot{}
	n.hasPendingSnap = false
	return snap, true
}

// buildInstallSnapshot constructs a MsgInstallSnapshot for peer carrying the
// node's current persisted snapshot.  Caller must hold the node mutex; the
// node must be the leader.  It returns nil if there is no snapshot to send.
func (n *Node) buildInstallSnapshot(peer NodeID) []outMsg {
	snap, err := n.storage.LoadSnapshot()
	if err != nil || snap.Index == 0 {
		// No snapshot available — cannot help this peer this round.
		return nil
	}
	cp := snap
	return []outMsg{{to: peer, msg: Message{
		Type:     MsgInstallSnapshot,
		From:     n.id,
		To:       peer,
		Term:     n.currentTerm,
		Snapshot: &cp,
	}}}
}

// handleInstallSnapshotResp processes a follower's reply to InstallSnapshot.
// On success it advances the peer's matchIndex/nextIndex to the snapshot
// index, then retries replication so the now-caught-up follower receives any
// post-snapshot entries.  Caller must hold the node mutex.
func (n *Node) handleInstallSnapshotResp(msg Message) []outMsg {
	if n.role != Leader || msg.Term != n.currentTerm {
		return nil
	}
	if !msg.Success {
		// The follower rejected the snapshot (stale term, or malformed);
		// nothing to advance.  A subsequent heartbeat retries.
		return nil
	}
	// ConflictIndex carries the snapshot index the follower installed (or
	// already had).  The follower is consistent with the leader up to it.
	matched := msg.ConflictIndex
	if matched > n.matchIndex[msg.From] {
		n.matchIndex[msg.From] = matched
	}
	if matched+1 > n.nextIndex[msg.From] {
		n.nextIndex[msg.From] = matched + 1
	}
	n.advanceCommit()
	// Send any entries appended after the snapshot point straight away.
	return n.buildReplication(msg.From)
}

// handleInstallSnapshot processes a MsgInstallSnapshot from a leader and
// returns the response.  Caller must hold the node mutex.
//
// Term checks mirror handleAppendEntries: a stale-term snapshot is rejected;
// a higher term has already demoted this node in Step.  If the snapshot is
// newer than the follower's committed state it is installed wholesale — saved
// to Storage, the conflicting log discarded, commitIndex/lastApplied advanced
// to snap.Index — and recorded for delivery to the state machine via
// PendingSnapshot.  A stale snapshot (snap.Index <= commitIndex) is accepted
// with Success=true but not re-installed.
func (n *Node) handleInstallSnapshot(msg Message) Message {
	resp := Message{
		Type: MsgInstallSnapshotResp,
		From: n.id,
		To:   msg.From,
		Term: n.currentTerm,
	}
	if msg.Term < n.currentTerm {
		resp.Success = false
		return resp
	}

	// A valid leader for this term: (re)become follower and record it.
	if n.role != Follower {
		n.becomeFollower(msg.Term, msg.From)
	} else {
		n.leaderID = msg.From
	}
	n.resetElectionTimeout()

	if msg.Snapshot == nil {
		// Malformed message; reject so the leader retries.
		resp.Success = false
		return resp
	}
	snap := *msg.Snapshot

	// MatchIndex hint: regardless of whether we install, after this RPC the
	// follower's log is consistent with the leader up to snap.Index.  We carry
	// it in ConflictIndex — the same field MsgAppendEntriesResp uses as its
	// highest-replicated-index hint, and which the transport layer already
	// serialises — so the leader can advance matchIndex/nextIndex.
	resp.ConflictIndex = snap.Index

	// Stale snapshot: the follower already has everything it covers.
	if snap.Index <= n.commitIndex {
		resp.Success = true
		return resp
	}

	// Install the snapshot wholesale.  SaveSnapshot persists it and (on
	// MemStorage) discards subsumed entries; Compact physically trims the log
	// on stores that keep entries after SaveSnapshot.  Any log entries that
	// survive Compact but conflict with the snapshot are discarded by reseting
	// the log: we Compact up to snap.Index, then drop anything left that the
	// snapshot's term does not vouch for.
	if err := n.storage.SaveSnapshot(snap); err != nil {
		panic("raft: SaveSnapshot during install failed: " + err.Error())
	}
	if err := n.storage.Compact(snap.Index); err != nil {
		panic("raft: Compact during install failed: " + err.Error())
	}
	// Discard any surviving log suffix that is not consistent with the
	// snapshot.  After Compact, FirstIndex == snap.Index+1.  An entry at
	// snap.Index+1.. is only safe to keep if the log is otherwise contiguous
	// from the snapshot; since the leader sent a snapshot precisely because
	// our log diverged or lagged, we conservatively drop the whole remaining
	// suffix so the follower catches up purely from the snapshot.
	last := n.log.lastIndex()
	if last > snap.Index {
		if err := n.storage.Compact(last); err != nil {
			panic("raft: log reset during install failed: " + err.Error())
		}
	}

	n.commitIndex = snap.Index
	n.lastApplied = snap.Index
	n.log.commitIndex = snap.Index

	// Restore cluster membership from the snapshot.  The snapshot supersedes
	// the follower's log entirely, so its recorded configuration becomes the
	// authoritative membership.  A snapshot produced before Phase 8 (or by a
	// store that cannot persist Conf) carries an empty Voters set; in that
	// case keep the existing configuration rather than wiping it.
	if len(snap.Conf.Voters) > 0 {
		n.clusterConfig = snap.Conf.clone()
	}

	// Record the snapshot for delivery to the state machine.  The most recent
	// install wins if a previous one was not yet consumed.
	n.pendingSnap = snap
	n.hasPendingSnap = true

	resp.Success = true
	return resp
}
