package raft

import "testing"

func testConfig(id NodeID, peers []NodeID, s Storage) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		Storage:            s,
		Transport:          nil,
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
	}
}

func newTestNode(t *testing.T, id NodeID, peers []NodeID) *Node {
	t.Helper()
	n, err := NewNode(testConfig(id, peers, NewMemStorage()))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestNewNodeStartsAsFollower(t *testing.T) {
	n := newTestNode(t, "n1", []NodeID{"n1", "n2", "n3"})
	if n.Role() != Follower {
		t.Fatalf("role = %v, want Follower", n.Role())
	}
	if n.Term() != 0 {
		t.Fatalf("term = %d, want 0", n.Term())
	}
	if n.electionTimeout < 10 || n.electionTimeout > 20 {
		t.Fatalf("electionTimeout = %d, out of [10,20]", n.electionTimeout)
	}
}

func TestNewNodeLoadsHardState(t *testing.T) {
	s := NewMemStorage()
	if err := s.SaveHardState(HardState{CurrentTerm: 5, VotedFor: "n2"}); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	n, err := NewNode(testConfig("n1", []NodeID{"n1", "n2", "n3"}, s))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if n.Term() != 5 {
		t.Fatalf("term = %d, want 5", n.Term())
	}
	if n.votedFor != "n2" {
		t.Fatalf("votedFor = %q, want n2", n.votedFor)
	}
}

func TestBecomeCandidate(t *testing.T) {
	n := newTestNode(t, "n1", []NodeID{"n1", "n2", "n3"})
	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()
	if n.Role() != Candidate {
		t.Fatalf("role = %v, want Candidate", n.Role())
	}
	if n.Term() != 1 {
		t.Fatalf("term = %d, want 1", n.Term())
	}
	if n.votedFor != "n1" {
		t.Fatalf("votedFor = %q, want n1 (self)", n.votedFor)
	}
}

func TestBecomeFollower(t *testing.T) {
	n := newTestNode(t, "n1", []NodeID{"n1", "n2", "n3"})
	n.mu.Lock()
	n.becomeCandidate()
	n.becomeFollower(7, "n3")
	n.mu.Unlock()
	if n.Role() != Follower {
		t.Fatalf("role = %v, want Follower", n.Role())
	}
	if n.Term() != 7 {
		t.Fatalf("term = %d, want 7", n.Term())
	}
	if n.Leader() != "n3" {
		t.Fatalf("leader = %q, want n3", n.Leader())
	}
}

func TestLastIndexAccessor(t *testing.T) {
	// A fresh node over empty storage has LastIndex 0.
	n := newTestNode(t, "n1", []NodeID{"n1", "n2", "n3"})
	if li := n.LastIndex(); li != 0 {
		t.Fatalf("LastIndex on empty log = %d, want 0", li)
	}

	// A node constructed over storage with three entries reports the last
	// entry's index.
	s := NewMemStorage()
	if err := s.AppendEntries([]LogEntry{ent(1, 1), ent(1, 2), ent(1, 3)}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	n2, err := NewNode(testConfig("n1", []NodeID{"n1", "n2", "n3"}, s))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if li := n2.LastIndex(); li != 3 {
		t.Fatalf("LastIndex = %d, want 3", li)
	}
}

func TestBecomeLeaderInitializesIndices(t *testing.T) {
	s := NewMemStorage()
	if err := s.AppendEntries([]LogEntry{ent(1, 1), ent(1, 2), ent(1, 3)}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	n, err := NewNode(testConfig("n1", []NodeID{"n1", "n2", "n3"}, s))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.mu.Lock()
	n.becomeCandidate()
	n.becomeLeader()
	n.mu.Unlock()

	if n.Role() != Leader {
		t.Fatalf("role = %v, want Leader", n.Role())
	}
	// becomeLeader appends a no-op, so lastIndex is now 4.
	if li := n.log.lastIndex(); li != 4 {
		t.Fatalf("lastIndex after becomeLeader = %d, want 4", li)
	}
	last, err := n.log.slice(4, 5)
	if err != nil || len(last) != 1 || last[0].Type != EntryNoop {
		t.Fatalf("last entry = %+v, %v, want EntryNoop", last, err)
	}
	for _, p := range []NodeID{"n2", "n3"} {
		if n.nextIndex[p] != 5 {
			t.Fatalf("nextIndex[%s] = %d, want 5 (lastIndex+1)", p, n.nextIndex[p])
		}
		if n.matchIndex[p] != 0 {
			t.Fatalf("matchIndex[%s] = %d, want 0", p, n.matchIndex[p])
		}
	}
}

func TestHardStatePersistedOnTermChange(t *testing.T) {
	s := NewMemStorage()
	n, err := NewNode(testConfig("n1", []NodeID{"n1", "n2", "n3"}, s))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.mu.Lock()
	n.becomeCandidate() // term -> 1, votedFor -> n1
	n.mu.Unlock()

	hs, err := s.LoadHardState()
	if err != nil {
		t.Fatalf("LoadHardState: %v", err)
	}
	if hs.CurrentTerm != 1 || hs.VotedFor != "n1" {
		t.Fatalf("persisted hardstate = %+v, want {1 n1}", hs)
	}

	n.mu.Lock()
	n.becomeFollower(9, "")
	n.mu.Unlock()
	hs, err = s.LoadHardState()
	if err != nil {
		t.Fatalf("LoadHardState: %v", err)
	}
	if hs.CurrentTerm != 9 || hs.VotedFor != "" {
		t.Fatalf("persisted hardstate = %+v, want {9 }", hs)
	}
}

func TestRoleString(t *testing.T) {
	for r, want := range map[Role]string{
		Follower:  "Follower",
		Candidate: "Candidate",
		Leader:    "Leader",
	} {
		if r.String() != want {
			t.Fatalf("Role(%d).String() = %q, want %q", r, r.String(), want)
		}
	}
}
