package raft

import "errors"

// ErrNotLeader is returned by Propose when the node is not the current leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Propose appends an application command to the leader's log and returns the
// index assigned to it.  It fails with ErrNotLeader on any non-leader node.
// Replication to followers happens on the next heartbeat Tick.
func (n *Node) Propose(data []byte) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	n.log.append([]LogEntry{{Term: n.currentTerm, Type: EntryNormal, Data: data}})
	idx := n.log.lastIndex()
	n.matchIndex[n.id] = idx
	// In a single-node cluster the leader alone is a majority, so the entry
	// is committed the instant it is appended.  advanceCommit counts the
	// leader's own matchIndex and still enforces the §5.4.2 current-term
	// rule, so it is a no-op for a multi-node leader that lacks peer acks.
	n.advanceCommit()
	out := n.buildAppendEntries()
	n.mu.Unlock()

	n.dispatch(out)
	return idx, nil
}

// buildAppendEntries collects a MsgAppendEntries for every peer based on its
// nextIndex.  Caller must hold the node mutex; the node must be the leader.
func (n *Node) buildAppendEntries() []outMsg {
	out := make([]outMsg, 0, len(n.peers)-1)
	last := n.log.lastIndex()
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		next := n.nextIndex[p]
		if next < 1 {
			next = 1
		}
		prevIndex := next - 1
		prevTerm, err := n.log.term(prevIndex)
		if err != nil {
			// prevIndex has been compacted away; a snapshot is needed.
			// Snapshot transfer is a Phase 5 concern — skip this peer.
			continue
		}
		var entries []LogEntry
		if next <= last {
			es, err := n.log.slice(next, last+1)
			if err != nil {
				continue
			}
			entries = es
		}
		out = append(out, outMsg{to: p, msg: Message{
			Type:         MsgAppendEntries,
			From:         n.id,
			To:           p,
			Term:         n.currentTerm,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			LeaderCommit: n.commitIndex,
			Entries:      entries,
		}})
	}
	return out
}

// handleAppendEntries processes a MsgAppendEntries from a leader and returns
// the response.  Caller must hold the node mutex.
func (n *Node) handleAppendEntries(msg Message) Message {
	resp := Message{
		Type: MsgAppendEntriesResp,
		From: n.id,
		To:   msg.From,
		Term: n.currentTerm,
	}
	// Reject AppendEntries from a stale leader.
	if msg.Term < n.currentTerm {
		resp.Success = false
		return resp
	}

	// A valid leader for this term: (re)become follower and record it.
	// (If msg.Term > currentTerm, Step already demoted us.)
	if n.role != Follower {
		n.becomeFollower(msg.Term, msg.From)
	} else {
		n.leaderID = msg.From
	}
	n.resetElectionTimeout()

	// Log-consistency check: we must have an entry at PrevLogIndex whose
	// term matches PrevLogTerm.
	last := n.log.lastIndex()
	if msg.PrevLogIndex > last {
		// Our log is too short.
		resp.Success = false
		resp.ConflictIndex = last + 1
		resp.ConflictTerm = 0
		return resp
	}
	prevTerm, err := n.log.term(msg.PrevLogIndex)
	if err != nil {
		// PrevLogIndex compacted away; treat as needing a shorter probe.
		resp.Success = false
		resp.ConflictIndex = last + 1
		resp.ConflictTerm = 0
		return resp
	}
	if prevTerm != msg.PrevLogTerm {
		// Term mismatch: report the conflicting term and the first
		// index this follower holds with that term, so the leader can
		// back nextIndex off a whole term at a time.
		resp.Success = false
		resp.ConflictTerm = prevTerm
		resp.ConflictIndex = n.firstIndexOfTerm(msg.PrevLogIndex, prevTerm)
		return resp
	}

	// Logs are consistent up to PrevLogIndex.  Append the new entries,
	// truncating only the genuinely conflicting suffix.
	if len(msg.Entries) > 0 {
		conflict := n.log.findConflict(msg.Entries)
		switch {
		case conflict == 0:
			// Every supplied entry is already present; nothing to do.
		default:
			// Append from the first conflicting entry onward.
			start := conflict - msg.Entries[0].Index
			n.log.append(msg.Entries[start:])
		}
	}

	// Advance the commit index.
	if msg.LeaderCommit > n.commitIndex {
		lastNew := msg.PrevLogIndex + uint64(len(msg.Entries))
		newCommit := msg.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.log.commitTo(newCommit)
		}
	}

	resp.Success = true
	// MatchIndex hint: the highest index now known consistent with leader.
	resp.ConflictIndex = msg.PrevLogIndex + uint64(len(msg.Entries))
	return resp
}

// firstIndexOfTerm scans backwards from idx and returns the first (lowest)
// index whose entry has the given term.  Caller must hold the node mutex.
func (n *Node) firstIndexOfTerm(idx, term uint64) uint64 {
	first := idx
	for i := idx; i >= 1; i-- {
		t, err := n.log.term(i)
		if err != nil || t != term {
			break
		}
		first = i
		if i == 1 {
			break
		}
	}
	return first
}

// handleAppendEntriesResp processes a follower's reply to AppendEntries.
// Caller must hold the node mutex.
func (n *Node) handleAppendEntriesResp(msg Message) []outMsg {
	if n.role != Leader || msg.Term != n.currentTerm {
		return nil
	}

	if msg.Success {
		// ConflictIndex carries the highest replicated index (the index
		// of the last entry the leader sent in this AppendEntries).
		matched := msg.ConflictIndex
		if matched > n.matchIndex[msg.From] {
			n.matchIndex[msg.From] = matched
		}
		if matched+1 > n.nextIndex[msg.From] {
			n.nextIndex[msg.From] = matched + 1
		}
		n.advanceCommit()
		return nil
	}

	// Failure: back nextIndex off using the conflict hint.
	next := n.nextIndex[msg.From]
	if msg.ConflictTerm == 0 {
		// Follower's log is too short (or compacted): jump straight to
		// the index it reported.
		next = msg.ConflictIndex
	} else {
		// If the leader has the conflicting term, retry from just past
		// the leader's last entry of that term; otherwise use the
		// follower's first index of that term.
		if li, ok := n.lastIndexOfTerm(msg.ConflictTerm); ok {
			next = li + 1
		} else {
			next = msg.ConflictIndex
		}
	}
	if next < 1 {
		next = 1
	}
	n.nextIndex[msg.From] = next
	// Retry replication to this peer immediately.
	return n.buildAppendEntriesFor(msg.From)
}

// lastIndexOfTerm returns the highest index in the leader's log whose entry
// has the given term, and whether such an entry exists.  Caller must hold the
// node mutex.
func (n *Node) lastIndexOfTerm(term uint64) (uint64, bool) {
	for i := n.log.lastIndex(); i >= 1; i-- {
		t, err := n.log.term(i)
		if err != nil {
			return 0, false
		}
		if t == term {
			return i, true
		}
		if t < term {
			// Terms are monotonic; no higher index can match.
			return 0, false
		}
		if i == 1 {
			break
		}
	}
	return 0, false
}

// buildAppendEntriesFor builds a single MsgAppendEntries for one peer.  Caller
// must hold the node mutex.
func (n *Node) buildAppendEntriesFor(peer NodeID) []outMsg {
	for _, o := range n.buildAppendEntries() {
		if o.to == peer {
			return []outMsg{o}
		}
	}
	return nil
}

// advanceCommit recomputes the leader's commit index: the highest index N that
// is replicated on a majority AND whose entry was created in the current term
// (§5.4.2 — a leader never commits prior-term entries directly).  Caller must
// hold the node mutex.
func (n *Node) advanceCommit() {
	last := n.log.lastIndex()
	for N := last; N > n.commitIndex; N-- {
		t, err := n.log.term(N)
		if err != nil || t != n.currentTerm {
			continue
		}
		count := 0
		for _, p := range n.peers {
			if n.matchIndex[p] >= N {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = N
			n.log.commitTo(N)
			return
		}
	}
}

// LogEntries returns the log entries in the half-open range [lo, hi).  It is
// a read-only accessor over the node's log, useful to tests and to Phase 6
// inspection; it returns nil if the range cannot be served.
func (n *Node) LogEntries(lo, hi uint64) []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	es, err := n.log.slice(lo, hi)
	if err != nil {
		return nil
	}
	return es
}

// Ready returns the committed-but-not-yet-applied log entries and advances the
// last-applied index past them.  Phase 6 feeds these into the state machine.
func (n *Node) Ready() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lastApplied >= n.commitIndex {
		return nil
	}
	entries, err := n.log.slice(n.lastApplied+1, n.commitIndex+1)
	if err != nil {
		panic("raft: Ready slice failed: " + err.Error())
	}
	n.lastApplied = n.commitIndex
	return entries
}
