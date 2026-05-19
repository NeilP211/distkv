package raft

import "errors"

// ErrNotLeader is returned by Propose when the node is not the current leader.
var ErrNotLeader = errors.New("raft: not the leader")

// ErrConfChangeInProgress is returned by ProposeConfChange when a membership
// change is already underway — the node is still in a joint configuration, and
// Raft permits only one membership change in flight at a time.
var ErrConfChangeInProgress = errors.New("raft: configuration change already in progress")

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

// ProposeConfChange begins a cluster-membership change by appending a joint
// configuration entry (an EntryConfChange carrying cc with Leave=false) to the
// leader's log.  It is leader-only (ErrNotLeader otherwise) and rejects a new
// change while one is still in flight — i.e. while the node's configuration is
// still joint — with ErrConfChangeInProgress.
//
// The node adopts the joint configuration immediately, on APPEND (§6), so the
// joint-consensus rules take effect before the entry commits.  The leader
// completes the change automatically: completeMembershipChange appends the
// final (joint-leaving) entry once the joint entry commits.
func (n *Node) ProposeConfChange(cc ConfChange) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	if n.clusterConfig.Joint {
		n.mu.Unlock()
		return 0, ErrConfChangeInProgress
	}
	cc.Leave = false
	entry := LogEntry{Term: n.currentTerm, Type: EntryConfChange, Data: cc.encode()}
	n.log.append([]LogEntry{entry})
	idx := n.log.lastIndex()
	// Adopt the membership change on append, then refresh per-peer progress
	// so a newly added member is replicated to immediately.
	n.applyConfEntry(LogEntry{Type: EntryConfChange, Data: cc.encode(), Index: idx, Term: n.currentTerm})
	n.matchIndex[n.id] = idx
	n.advanceCommit()
	out := n.buildAppendEntries()
	n.mu.Unlock()

	n.dispatch(out)
	return idx, nil
}

// completeMembershipChange drives the second half of a joint-consensus
// membership change.  When the leader observes that the joint-config entry has
// committed and the node is still leader and still joint, it appends the final
// (joint-leaving) EntryConfChange.  When the final configuration has committed
// and the leader is no longer a voter in it, the leader steps down — it must
// not keep leading a cluster it is not part of.  Caller must hold the node
// mutex; safe to call on a non-leader (it is then a no-op).
func (n *Node) completeMembershipChange() {
	if n.role != Leader {
		return
	}

	// Leader removed itself: once the final config commits, step down.
	if !n.clusterConfig.Joint {
		if !containsID(n.clusterConfig.Voters, n.id) && n.confEntryCommitted() {
			n.becomeFollower(n.currentTerm, "")
		}
		return
	}

	// Still joint: append the leaving entry once the joint entry has committed.
	jointIdx, jointCC, ok := n.lastConfEntry()
	if !ok || jointCC.Leave {
		return
	}
	if n.commitIndex < jointIdx {
		return
	}
	leave := ConfChange{Type: jointCC.Type, Node: jointCC.Node, Leave: true}
	entry := LogEntry{Term: n.currentTerm, Type: EntryConfChange, Data: leave.encode()}
	n.log.append([]LogEntry{entry})
	idx := n.log.lastIndex()
	n.applyConfEntry(LogEntry{Type: EntryConfChange, Data: leave.encode(), Index: idx, Term: n.currentTerm})
	n.matchIndex[n.id] = idx
	// Recompute commit (the leave entry may be immediately committable in a
	// small cluster) — but guard against unbounded recursion: advanceCommit
	// calls completeMembershipChange again, which will now find the config
	// already simple and only ever step the leader down.
	n.advanceCommit()
}

// lastConfEntry returns the highest-indexed EntryConfChange in the log that the
// node can still serve, its decoded ConfChange, and whether one was found.
// Caller must hold the node mutex.
func (n *Node) lastConfEntry() (uint64, ConfChange, bool) {
	first, err := n.storage.FirstIndex()
	if err != nil {
		return 0, ConfChange{}, false
	}
	for i := n.log.lastIndex(); i >= first; i-- {
		es, err := n.log.slice(i, i+1)
		if err != nil || len(es) != 1 {
			break
		}
		if es[0].Type == EntryConfChange {
			cc, err := decodeConfChange(es[0].Data)
			if err != nil {
				return 0, ConfChange{}, false
			}
			return i, cc, true
		}
		if i == 0 {
			break
		}
	}
	return 0, ConfChange{}, false
}

// confEntryCommitted reports whether the most recent EntryConfChange in the log
// has been committed.  Caller must hold the node mutex.
func (n *Node) confEntryCommitted() bool {
	idx, _, ok := n.lastConfEntry()
	return ok && n.commitIndex >= idx
}

// containsID reports whether ids contains id.
func containsID(ids []NodeID, id NodeID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// buildAppendEntries collects a replication message for every peer based on
// its nextIndex.  For a peer whose nextIndex still lies within the leader's
// log it is a MsgAppendEntries; for a peer that has fallen behind the leader's
// compacted log boundary it is a MsgInstallSnapshot.  Caller must hold the
// node mutex; the node must be the leader.
func (n *Node) buildAppendEntries() []outMsg {
	members := n.clusterConfig.allMembers()
	out := make([]outMsg, 0, len(members))
	for _, p := range members {
		if p == n.id {
			continue
		}
		out = append(out, n.buildReplication(p)...)
	}
	return out
}

// buildReplication builds the single replication message owed to one peer:
// MsgAppendEntries when the entries it needs are still in the log, or
// MsgInstallSnapshot when they have been compacted away.  Caller must hold the
// node mutex; the node must be the leader.  It returns nil if no message can
// be built for the peer this round.
func (n *Node) buildReplication(peer NodeID) []outMsg {
	first, err := n.storage.FirstIndex()
	if err != nil {
		panic("raft: storage.FirstIndex failed: " + err.Error())
	}
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	// The leader needs the entry at prevIndex (= next-1) to build a valid
	// AppendEntries.  If prevIndex precedes the first index the log can serve,
	// those entries are gone — the peer must be caught up with a snapshot.
	prevIndex := next - 1
	if prevIndex < first-1 {
		return n.buildInstallSnapshot(peer)
	}

	last := n.log.lastIndex()
	prevTerm, err := n.log.term(prevIndex)
	if err != nil {
		// prevIndex compacted away despite the bound check (a concurrent
		// compaction); fall back to a snapshot.
		return n.buildInstallSnapshot(peer)
	}
	var entries []LogEntry
	if next <= last {
		es, err := n.log.slice(next, last+1)
		if err != nil {
			return n.buildInstallSnapshot(peer)
		}
		entries = es
	}
	return []outMsg{{to: peer, msg: Message{
		Type:         MsgAppendEntries,
		From:         n.id,
		To:           peer,
		Term:         n.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: n.commitIndex,
		Entries:      entries,
	}}}
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
			appended := msg.Entries[start:]
			// A conflict means we truncated a divergent suffix.  Any
			// EntryConfChange in that suffix must be undone, so re-derive
			// the configuration by replay from durable state, then re-apply
			// the membership effect of the entries we just appended.
			truncated := conflict <= n.log.lastIndex()
			n.log.append(appended)
			if truncated {
				snap, err := n.storage.LoadSnapshot()
				if err != nil {
					panic("raft: LoadSnapshot during conf replay failed: " + err.Error())
				}
				if err := n.replayConfig(snap); err != nil {
					panic("raft: replayConfig after truncation failed: " + err.Error())
				}
			} else {
				// Pure extension: just adopt the new entries' membership.
				for _, e := range appended {
					n.applyConfEntry(e)
				}
			}
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
	// Retry replication to this peer immediately.  buildReplication switches
	// to MsgInstallSnapshot automatically if the backoff walked nextIndex
	// below the leader's compacted log boundary.
	return n.buildReplication(msg.From)
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

// advanceCommit recomputes the leader's commit index: the highest index N that
// is replicated on a quorum AND whose entry was created in the current term
// (§5.4.2 — a leader never commits prior-term entries directly).  "Quorum" is
// evaluated through the node's ClusterConfig, so while joint it requires an
// independent majority of both C_old and C_new.
//
// After advancing the commit index it calls completeMembershipChange, which
// drives the second (joint-leaving) step of a membership change and any
// leader step-down once the final configuration commits.  Caller must hold the
// node mutex.
func (n *Node) advanceCommit() {
	last := n.log.lastIndex()
	for N := last; N > n.commitIndex; N-- {
		t, err := n.log.term(N)
		if err != nil || t != n.currentTerm {
			continue
		}
		if n.clusterConfig.committed(n.matchIndex, n.id, last, N) {
			n.commitIndex = N
			n.log.commitTo(N)
			break
		}
	}
	n.completeMembershipChange()
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
