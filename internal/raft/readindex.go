package raft

// ConfirmLeadership implements the heartbeat-confirmation step of the ReadIndex
// protocol for linearizable reads.
//
// If the node is not currently the Leader it returns (0, false): readIndex is
// only meaningful when ok is true and MUST be ignored otherwise.
//
// Otherwise it captures readIndex = commitIndex, then broadcasts a
// heartbeat-style MsgAppendEntries round to every peer and counts how many
// reply Success at the leader's current term.  ok is true only if the leader
// plus those acknowledging peers form a majority of the cluster — proving the
// node is still leader at the moment of the read.  A deposed leader, isolated
// from a majority, cannot collect those acks and so returns ok = false rather
// than serving a possibly-stale read.
//
// For a single-node cluster the leader alone is a majority, so it returns
// (commitIndex, true) without sending any messages.
//
// Concurrency: the heartbeat messages are built under n.mu and dispatched after
// the mutex is released — the same collect-then-send discipline used by Tick
// and Propose — so transport.Send is never called while holding n.mu.
func (n *Node) ConfirmLeadership() (readIndex uint64, ok bool) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, false
	}
	term := n.currentTerm
	readIndex = n.commitIndex
	quorum := n.quorum()
	// buildAppendEntries is the shared heartbeat/AppendEntries broadcast
	// helper; reusing it here avoids duplicating the per-peer construction
	// logic.  With no pending entries each message is a pure heartbeat.
	out := n.buildAppendEntries()
	n.mu.Unlock()

	// Single-node cluster: the leader alone is already a majority.
	if quorum <= 1 {
		return readIndex, true
	}

	// The leader counts toward the majority itself.
	acks := 1
	for _, o := range out {
		resp, err := n.sendForConfirm(o)
		if err != nil {
			continue
		}
		if resp.Type == MsgAppendEntriesResp && resp.Success && resp.Term == term {
			acks++
		}
		// Feed the response back into Step so a higher term still demotes
		// this node and matchIndex/commit progress is not lost.
		if resp.Type != 0 || resp.From != "" {
			n.Step(resp)
		}
	}
	return readIndex, acks >= quorum
}

// sendForConfirm dispatches a single heartbeat message via the transport.  It
// must be called with n.mu NOT held.
func (n *Node) sendForConfirm(o outMsg) (Message, error) {
	if n.transport == nil {
		return Message{}, ErrNotLeader
	}
	return n.transport.Send(o.to, o.msg)
}
