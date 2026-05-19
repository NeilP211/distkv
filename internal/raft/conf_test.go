package raft

import (
	"reflect"
	"testing"
)

// votesOf builds a votes map from a list of granting node IDs.
func votesOf(ids ...NodeID) map[NodeID]bool {
	m := make(map[NodeID]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func TestQuorumReachedSimple(t *testing.T) {
	cfg := ClusterConfig{Voters: []NodeID{"n1", "n2", "n3"}}
	tests := []struct {
		name  string
		votes map[NodeID]bool
		want  bool
	}{
		{"none", votesOf(), false},
		{"one", votesOf("n1"), false},
		{"two", votesOf("n1", "n2"), true},
		{"all", votesOf("n1", "n2", "n3"), true},
		{"extraneous-only", votesOf("n9"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cfg.quorumReached(tc.votes); got != tc.want {
				t.Fatalf("quorumReached(%v) = %v, want %v", tc.votes, got, tc.want)
			}
		})
	}
}

func TestQuorumReachedJointGrow(t *testing.T) {
	// Joint 3->5: OldVoters {n1,n2,n3} need 2, Voters {n1..n5} need 3.
	cfg := ClusterConfig{
		Voters:    []NodeID{"n1", "n2", "n3", "n4", "n5"},
		OldVoters: []NodeID{"n1", "n2", "n3"},
		Joint:     true,
	}
	tests := []struct {
		name  string
		votes map[NodeID]bool
		want  bool
	}{
		{"old-maj-only", votesOf("n1", "n2"), false},       // 2-of-old, 2-of-new -> fail new
		{"new-maj-only", votesOf("n3", "n4", "n5"), false}, // 1-of-old, 3-of-new -> fail old
		{"both", votesOf("n1", "n2", "n3"), true},          // 3-of-old, 3-of-new
		{"both-mixed", votesOf("n1", "n2", "n4"), true},    // 2-of-old, 3-of-new
		{"new-only-3", votesOf("n4", "n5", "n1"), false},   // 1-of-old, 3-of-new
		{"all", votesOf("n1", "n2", "n3", "n4", "n5"), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cfg.quorumReached(tc.votes); got != tc.want {
				t.Fatalf("quorumReached(%v) = %v, want %v", tc.votes, got, tc.want)
			}
		})
	}
}

func TestQuorumReachedJointShrink(t *testing.T) {
	// Joint 5->3: OldVoters {n1..n5} need 3, Voters {n1,n2,n3} need 2.
	cfg := ClusterConfig{
		Voters:    []NodeID{"n1", "n2", "n3"},
		OldVoters: []NodeID{"n1", "n2", "n3", "n4", "n5"},
		Joint:     true,
	}
	tests := []struct {
		name  string
		votes map[NodeID]bool
		want  bool
	}{
		{"old-maj-only", votesOf("n3", "n4", "n5"), false}, // 3-of-old, 1-of-new
		{"new-maj-only", votesOf("n1", "n2"), false},       // 2-of-old, 2-of-new -> fail old
		{"both", votesOf("n1", "n2", "n3"), true},          // 3-of-old, 3-of-new
		{"both-min", votesOf("n1", "n2", "n4"), true},      // 3-of-old, 2-of-new
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cfg.quorumReached(tc.votes); got != tc.want {
				t.Fatalf("quorumReached(%v) = %v, want %v", tc.votes, got, tc.want)
			}
		})
	}
}

func TestCommittedSimple(t *testing.T) {
	// 3 voters: n1 (leader), n2, n3. leaderLast counts for n1.
	cfg := ClusterConfig{Voters: []NodeID{"n1", "n2", "n3"}}
	tests := []struct {
		name       string
		match      map[NodeID]uint64
		leaderLast uint64
		n          uint64
		want       bool
	}{
		{"leader-only", map[NodeID]uint64{}, 5, 5, false},
		{"one-follower", map[NodeID]uint64{"n2": 5}, 5, 5, true},
		{"follower-behind", map[NodeID]uint64{"n2": 3}, 5, 5, false},
		{"both-followers", map[NodeID]uint64{"n2": 5, "n3": 5}, 5, 5, true},
		{"higher-match-ok", map[NodeID]uint64{"n2": 7}, 7, 5, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cfg.committed(tc.match, "n1", tc.leaderLast, tc.n); got != tc.want {
				t.Fatalf("committed(%v,n1,%d,%d) = %v, want %v",
					tc.match, tc.leaderLast, tc.n, got, tc.want)
			}
		})
	}
}

func TestCommittedJointGrow(t *testing.T) {
	// Joint 3->5: n1 is leader. Old needs 2-of-{n1,n2,n3}, New needs 3-of-{n1..n5}.
	cfg := ClusterConfig{
		Voters:    []NodeID{"n1", "n2", "n3", "n4", "n5"},
		OldVoters: []NodeID{"n1", "n2", "n3"},
		Joint:     true,
	}
	tests := []struct {
		name       string
		match      map[NodeID]uint64
		leaderLast uint64
		n          uint64
		want       bool
	}{
		// leader + n2: 2-of-old ok, 2-of-new fail.
		{"old-only", map[NodeID]uint64{"n2": 5}, 5, 5, false},
		// leader + n4 + n5: 1-of-old fail, 3-of-new ok.
		{"new-only", map[NodeID]uint64{"n4": 5, "n5": 5}, 5, 5, false},
		// leader + n2 + n3: 3-of-old ok, 3-of-new ok.
		{"both", map[NodeID]uint64{"n2": 5, "n3": 5}, 5, 5, true},
		// leader + n2 + n4: 2-of-old ok, 3-of-new ok.
		{"both-mixed", map[NodeID]uint64{"n2": 5, "n4": 5}, 5, 5, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cfg.committed(tc.match, "n1", tc.leaderLast, tc.n); got != tc.want {
				t.Fatalf("committed(%v,n1,%d,%d) = %v, want %v",
					tc.match, tc.leaderLast, tc.n, got, tc.want)
			}
		})
	}
}

func TestCommittedJointShrink(t *testing.T) {
	// Joint 5->3: n1 leader. Old needs 3-of-{n1..n5}, New needs 2-of-{n1,n2,n3}.
	cfg := ClusterConfig{
		Voters:    []NodeID{"n1", "n2", "n3"},
		OldVoters: []NodeID{"n1", "n2", "n3", "n4", "n5"},
		Joint:     true,
	}
	tests := []struct {
		name       string
		match      map[NodeID]uint64
		leaderLast uint64
		n          uint64
		want       bool
	}{
		// leader + n4 + n5: 3-of-old ok, 1-of-new fail.
		{"old-only", map[NodeID]uint64{"n4": 5, "n5": 5}, 5, 5, false},
		// leader + n2: 2-of-old fail, 2-of-new ok.
		{"new-only", map[NodeID]uint64{"n2": 5}, 5, 5, false},
		// leader + n2 + n3: 3-of-old ok, 3-of-new ok.
		{"both", map[NodeID]uint64{"n2": 5, "n3": 5}, 5, 5, true},
		// leader + n2 + n4: 3-of-old ok, 2-of-new ok.
		{"both-mixed", map[NodeID]uint64{"n2": 5, "n4": 5}, 5, 5, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cfg.committed(tc.match, "n1", tc.leaderLast, tc.n); got != tc.want {
				t.Fatalf("committed(%v,n1,%d,%d) = %v, want %v",
					tc.match, tc.leaderLast, tc.n, got, tc.want)
			}
		})
	}
}

func TestAllMembers(t *testing.T) {
	cfg := ClusterConfig{
		Voters:    []NodeID{"n3", "n1", "n2"},
		OldVoters: []NodeID{"n2", "n1", "n4"},
		Joint:     true,
	}
	got := cfg.allMembers()
	want := []NodeID{"n1", "n2", "n3", "n4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("allMembers() = %v, want %v", got, want)
	}

	simple := ClusterConfig{Voters: []NodeID{"n2", "n1"}}
	gotS := simple.allMembers()
	wantS := []NodeID{"n1", "n2"}
	if !reflect.DeepEqual(gotS, wantS) {
		t.Fatalf("allMembers() simple = %v, want %v", gotS, wantS)
	}
}

func TestEnterJointAdd(t *testing.T) {
	cfg := ClusterConfig{Voters: []NodeID{"n1", "n2", "n3"}}
	j := cfg.enterJoint(ConfChange{Type: ConfAddNode, Node: "n4"})
	if !j.Joint {
		t.Fatal("enterJoint result not Joint")
	}
	if !reflect.DeepEqual(j.OldVoters, []NodeID{"n1", "n2", "n3"}) {
		t.Fatalf("OldVoters = %v", j.OldVoters)
	}
	if !reflect.DeepEqual(j.Voters, []NodeID{"n1", "n2", "n3", "n4"}) {
		t.Fatalf("Voters = %v", j.Voters)
	}
	// Original config must not be mutated.
	if len(cfg.Voters) != 3 {
		t.Fatalf("original config mutated: %v", cfg.Voters)
	}
}

func TestEnterJointRemove(t *testing.T) {
	cfg := ClusterConfig{Voters: []NodeID{"n1", "n2", "n3", "n4", "n5"}}
	j := cfg.enterJoint(ConfChange{Type: ConfRemoveNode, Node: "n5"})
	if !j.Joint {
		t.Fatal("enterJoint result not Joint")
	}
	if !reflect.DeepEqual(j.OldVoters, []NodeID{"n1", "n2", "n3", "n4", "n5"}) {
		t.Fatalf("OldVoters = %v", j.OldVoters)
	}
	if !reflect.DeepEqual(j.Voters, []NodeID{"n1", "n2", "n3", "n4"}) {
		t.Fatalf("Voters = %v", j.Voters)
	}
}

func TestEnterJointAddIdempotent(t *testing.T) {
	cfg := ClusterConfig{Voters: []NodeID{"n1", "n2"}}
	j := cfg.enterJoint(ConfChange{Type: ConfAddNode, Node: "n2"})
	if !reflect.DeepEqual(j.Voters, []NodeID{"n1", "n2"}) {
		t.Fatalf("adding existing node changed Voters: %v", j.Voters)
	}
}

func TestLeaveJoint(t *testing.T) {
	cfg := ClusterConfig{
		Voters:    []NodeID{"n1", "n2", "n3", "n4"},
		OldVoters: []NodeID{"n1", "n2", "n3"},
		Joint:     true,
	}
	f := cfg.leaveJoint()
	if f.Joint {
		t.Fatal("leaveJoint result still Joint")
	}
	if f.OldVoters != nil {
		t.Fatalf("OldVoters not nil: %v", f.OldVoters)
	}
	if !reflect.DeepEqual(f.Voters, []NodeID{"n1", "n2", "n3", "n4"}) {
		t.Fatalf("Voters = %v", f.Voters)
	}
}

func TestConfChangeEncodeDecode(t *testing.T) {
	for _, cc := range []ConfChange{
		{Type: ConfAddNode, Node: "n4"},
		{Type: ConfRemoveNode, Node: "n2"},
		{Type: ConfAddNode, Node: "n4", Leave: true},
	} {
		data := cc.encode()
		got, err := decodeConfChange(data)
		if err != nil {
			t.Fatalf("decodeConfChange: %v", err)
		}
		if got != cc {
			t.Fatalf("round-trip: got %+v, want %+v", got, cc)
		}
	}
	if _, err := decodeConfChange([]byte("garbage")); err == nil {
		t.Fatal("decodeConfChange(garbage) should error")
	}
}
