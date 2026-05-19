package raft

import (
	"encoding/json"
	"sort"
)

// ConfChangeType distinguishes the two kinds of single-node membership change
// that a joint-consensus transition can express.
type ConfChangeType int

const (
	// ConfAddNode adds a voting member to the cluster.
	ConfAddNode ConfChangeType = iota
	// ConfRemoveNode removes a voting member from the cluster.
	ConfRemoveNode
)

// ConfChange is the payload of an EntryConfChange log entry.
//
// A membership change is carried out as two EntryConfChange entries:
//
//   - the joint-entering entry (Leave == false) records the actual add/remove;
//     applying it moves the node into the joint configuration C_old,new.
//   - the joint-leaving entry (Leave == true) carries the same add/remove for
//     informational symmetry, but applying it simply collapses the joint
//     configuration to the final simple configuration C_new.
//
// The Leave flag is what lets a node, when it appends an EntryConfChange,
// decide whether to call enterJoint or leaveJoint without consulting any other
// state.
type ConfChange struct {
	// Type is the kind of change (add or remove).
	Type ConfChangeType `json:"type"`
	// Node is the member being added or removed.
	Node NodeID `json:"node"`
	// Leave is false for the joint-entering entry and true for the
	// joint-leaving entry.
	Leave bool `json:"leave"`
}

// encode serialises a ConfChange for storage in LogEntry.Data.
func (cc ConfChange) encode() []byte {
	b, err := json.Marshal(cc)
	if err != nil {
		panic("raft: ConfChange.encode failed: " + err.Error())
	}
	return b
}

// decodeConfChange deserialises a ConfChange previously produced by encode.
func decodeConfChange(data []byte) (ConfChange, error) {
	var cc ConfChange
	if err := json.Unmarshal(data, &cc); err != nil {
		return ConfChange{}, err
	}
	return cc, nil
}

// ClusterConfig is the active cluster-membership configuration of a node.
//
// When Joint is false the configuration is simple: Voters is the single active
// set and OldVoters is unused.  When Joint is true the configuration is the
// joint configuration C_old,new of Raft §6: Voters is C_new and OldVoters is
// C_old, and agreement requires an independent majority of EACH set.
type ClusterConfig struct {
	// Voters is the active voting set (C_new while joint).
	Voters []NodeID
	// OldVoters is C_old; meaningful only while Joint is true.
	OldVoters []NodeID
	// Joint reports whether the node is in a joint configuration.
	Joint bool
}

// majority reports whether the granted nodes form a strict majority of set.
func majority(set []NodeID, granted map[NodeID]bool) bool {
	if len(set) == 0 {
		// An empty voter set vacuously has agreement; this only arises for
		// OldVoters of a simple (non-joint) configuration, which is ignored.
		return true
	}
	count := 0
	for _, id := range set {
		if granted[id] {
			count++
		}
	}
	return count >= len(set)/2+1
}

// quorumReached reports whether votes constitute agreement: a majority of
// Voters and, when Joint, also an independent majority of OldVoters.
func (c ClusterConfig) quorumReached(votes map[NodeID]bool) bool {
	if !majority(c.Voters, votes) {
		return false
	}
	if c.Joint && !majority(c.OldVoters, votes) {
		return false
	}
	return true
}

// committed reports whether index n is replicated on enough members to be
// committed: a majority of Voters and, when Joint, also an independent
// majority of OldVoters.
//
// A follower counts as holding n when its matchIndex is at least n.  The
// leader is identified by leader and counts via leaderLast (the index of its
// own last log entry) regardless of any matchIndex entry, since a leader does
// not replicate to itself.
func (c ClusterConfig) committed(matchIndex map[NodeID]uint64, leader NodeID, leaderLast, n uint64) bool {
	holds := func(id NodeID) bool {
		if id == leader {
			return leaderLast >= n
		}
		return matchIndex[id] >= n
	}
	has := func(set []NodeID) bool {
		if len(set) == 0 {
			return true
		}
		count := 0
		for _, id := range set {
			if holds(id) {
				count++
			}
		}
		return count >= len(set)/2+1
	}
	if !has(c.Voters) {
		return false
	}
	if c.Joint && !has(c.OldVoters) {
		return false
	}
	return true
}

// allMembers returns the deduplicated, sorted union of Voters and OldVoters.
func (c ClusterConfig) allMembers() []NodeID {
	seen := make(map[NodeID]struct{}, len(c.Voters)+len(c.OldVoters))
	for _, id := range c.Voters {
		seen[id] = struct{}{}
	}
	for _, id := range c.OldVoters {
		seen[id] = struct{}{}
	}
	out := make([]NodeID, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// applyChange returns Voters with the add/remove of change applied.  Adding an
// existing member or removing an absent one is a no-op.  The result is a fresh
// sorted slice; the receiver is not mutated.
func applyChange(voters []NodeID, change ConfChange) []NodeID {
	out := make([]NodeID, 0, len(voters)+1)
	present := false
	for _, id := range voters {
		if id == change.Node {
			present = true
			if change.Type == ConfRemoveNode {
				continue
			}
		}
		out = append(out, id)
	}
	if change.Type == ConfAddNode && !present {
		out = append(out, change.Node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// enterJoint returns the joint configuration that results from applying change
// to the current (simple) configuration: OldVoters becomes the current Voters
// and Voters becomes the current Voters with change applied.
func (c ClusterConfig) enterJoint(change ConfChange) ClusterConfig {
	old := make([]NodeID, len(c.Voters))
	copy(old, c.Voters)
	sort.Slice(old, func(i, j int) bool { return old[i] < old[j] })
	return ClusterConfig{
		Voters:    applyChange(c.Voters, change),
		OldVoters: old,
		Joint:     true,
	}
}

// leaveJoint returns the simple final configuration C_new: Voters is kept and
// OldVoters is dropped.
func (c ClusterConfig) leaveJoint() ClusterConfig {
	voters := make([]NodeID, len(c.Voters))
	copy(voters, c.Voters)
	sort.Slice(voters, func(i, j int) bool { return voters[i] < voters[j] })
	return ClusterConfig{Voters: voters, OldVoters: nil, Joint: false}
}

// clone returns a deep copy of the configuration.
func (c ClusterConfig) clone() ClusterConfig {
	cp := ClusterConfig{Joint: c.Joint}
	if c.Voters != nil {
		cp.Voters = make([]NodeID, len(c.Voters))
		copy(cp.Voters, c.Voters)
	}
	if c.OldVoters != nil {
		cp.OldVoters = make([]NodeID, len(c.OldVoters))
		copy(cp.OldVoters, c.OldVoters)
	}
	return cp
}
