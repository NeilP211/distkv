package raft

// outMsg is a pending outbound message collected while the node mutex is held
// and dispatched after it is released.
type outMsg struct {
	to  NodeID
	msg Message
}

// Tick advances the node's logical clock by one unit.  A Follower or Candidate
// counts down its election timeout and starts an election when it expires; a
// Leader counts down its heartbeat interval and broadcasts AppendEntries.
//
// Tick acquires the node mutex, collects any outbound messages, releases the
// mutex, then dispatches them via the transport.
func (n *Node) Tick() {
	n.mu.Lock()
	var out []outMsg
	switch n.role {
	case Follower, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.becomeCandidate()
			// becomeCandidate may have already won the election in a
			// single-node cluster; in that case assert leadership with
			// heartbeats instead of soliciting votes.
			if n.role == Leader {
				out = n.buildAppendEntries()
			} else {
				out = n.buildRequestVotes()
			}
		}
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatInterval {
			n.heartbeatElapsed = 0
			out = n.buildAppendEntries()
		}
	}
	n.mu.Unlock()
	n.dispatch(out)
}

// dispatch sends each outbound message and feeds the response back into Step.
// It must be called with the node mutex NOT held.  A transport error simply
// means no response is processed for that peer.
func (n *Node) dispatch(out []outMsg) {
	if n.transport == nil {
		return
	}
	for _, o := range out {
		resp, err := n.transport.Send(o.to, o.msg)
		if err != nil {
			continue
		}
		if resp.Type == 0 && resp.From == "" {
			// Zero-value response (one-way message); nothing to feed back.
			continue
		}
		n.Step(resp)
	}
}

// buildRequestVotes collects a MsgRequestVote for every peer.  Caller must
// hold the node mutex.
func (n *Node) buildRequestVotes() []outMsg {
	members := n.clusterConfig.allMembers()
	out := make([]outMsg, 0, len(members))
	for _, p := range members {
		if p == n.id {
			continue
		}
		out = append(out, outMsg{to: p, msg: Message{
			Type:         MsgRequestVote,
			From:         n.id,
			To:           p,
			Term:         n.currentTerm,
			LastLogIndex: n.log.lastIndex(),
			LastLogTerm:  n.log.lastTerm(),
		}})
	}
	return out
}

// Step is the single entry point for every incoming Raft message.  For request
// messages it returns the response to send back; for response/one-way messages
// it returns a zero Message.  Any outbound messages produced as a side effect
// (e.g. heartbeats after winning an election) are dispatched internally.
func (n *Node) Step(msg Message) Message {
	n.mu.Lock()

	// A message from a higher term forces this node to a follower at that
	// term before the message is handled.
	if msg.Term > n.currentTerm {
		leader := NodeID("")
		if msg.Type == MsgAppendEntries || msg.Type == MsgInstallSnapshot {
			leader = msg.From
		}
		n.becomeFollower(msg.Term, leader)
	}

	var resp Message
	var out []outMsg
	switch msg.Type {
	case MsgRequestVote:
		resp = n.handleRequestVote(msg)
	case MsgRequestVoteResp:
		out = n.handleRequestVoteResp(msg)
	case MsgAppendEntries:
		resp = n.handleAppendEntries(msg)
	case MsgAppendEntriesResp:
		out = n.handleAppendEntriesResp(msg)
	case MsgInstallSnapshot:
		resp = n.handleInstallSnapshot(msg)
	case MsgInstallSnapshotResp:
		out = n.handleInstallSnapshotResp(msg)
	}
	n.mu.Unlock()

	n.dispatch(out)
	return resp
}

// handleRequestVote processes a MsgRequestVote and returns the response.
// Caller must hold the node mutex.
func (n *Node) handleRequestVote(msg Message) Message {
	resp := Message{
		Type: MsgRequestVoteResp,
		From: n.id,
		To:   msg.From,
		Term: n.currentTerm,
	}
	// Reject votes from a stale term.
	if msg.Term < n.currentTerm {
		resp.VoteGranted = false
		return resp
	}
	// Grant iff we have not voted for someone else this term and the
	// candidate's log is at least as up-to-date as ours (§5.4.1).
	canVote := n.votedFor == "" || n.votedFor == msg.From
	if canVote && n.log.isUpToDate(msg.LastLogIndex, msg.LastLogTerm) {
		n.votedFor = msg.From
		n.persistHardState()
		n.resetElectionTimeout()
		resp.VoteGranted = true
	}
	return resp
}

// handleRequestVoteResp tallies a vote and, on reaching a majority, promotes
// the node to Leader.  Caller must hold the node mutex.
func (n *Node) handleRequestVoteResp(msg Message) []outMsg {
	// Only relevant while still campaigning in the same term.
	if n.role != Candidate || msg.Term != n.currentTerm {
		return nil
	}
	if !msg.VoteGranted {
		return nil
	}
	n.votesGranted[msg.From] = true
	if n.maybeBecomeLeader() {
		// Immediately assert leadership with a round of heartbeats so
		// followers learn the new leader without waiting a full tick.
		return n.buildAppendEntries()
	}
	return nil
}
