package websim_test

import (
	"strings"
	"testing"

	"github.com/NeilP211/distkv/internal/websim"
)

func state(nodes ...websim.NodeState) websim.ClusterState {
	return websim.ClusterState{Nodes: nodes}
}

func node(id, role string, term, commit uint64, up bool) websim.NodeState {
	return websim.NodeState{ID: id, Role: role, Term: term, Commit: commit, Up: up}
}

func texts(evs []websim.Event) string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Text
	}
	return strings.Join(out, " | ")
}

func TestDiffEventsRoleTransitions(t *testing.T) {
	prev := state(node("n1", "Follower", 1, 0, true))
	cur := state(node("n1", "Candidate", 2, 0, true))
	evs := websim.DiffEvents(prev, cur)
	if len(evs) != 1 || evs[0].Text != "n1 → Candidate (term 2)" {
		t.Fatalf("got %q", texts(evs))
	}

	prev = cur
	cur = state(node("n1", "Leader", 2, 0, true))
	evs = websim.DiffEvents(prev, cur)
	if len(evs) != 1 || evs[0].Text != "n1 became Leader (term 2)" {
		t.Fatalf("got %q", texts(evs))
	}
}

func TestDiffEventsCrashRecover(t *testing.T) {
	prev := state(node("n2", "Follower", 1, 5, true))
	cur := state(node("n2", "Follower", 1, 5, false))
	evs := websim.DiffEvents(prev, cur)
	if len(evs) != 1 || evs[0].Text != "n2 crashed" || evs[0].Kind != "fault" {
		t.Fatalf("crash: got %q", texts(evs))
	}

	evs = websim.DiffEvents(cur, prev) // false -> true
	if len(evs) != 1 || evs[0].Text != "n2 recovered" {
		t.Fatalf("recover: got %q", texts(evs))
	}
}

func TestDiffEventsCommitAdvance(t *testing.T) {
	prev := state(node("n1", "Leader", 2, 3, true))
	cur := state(node("n1", "Leader", 2, 9, true))
	evs := websim.DiffEvents(prev, cur)
	if len(evs) != 1 || evs[0].Text != "commit index → 9" || evs[0].Kind != "commit" {
		t.Fatalf("got %q", texts(evs))
	}
}

func TestDiffEventsNoChange(t *testing.T) {
	s := state(node("n1", "Leader", 2, 9, true), node("n2", "Follower", 2, 9, true))
	if evs := websim.DiffEvents(s, s); len(evs) != 0 {
		t.Fatalf("identical snapshots should yield no events, got %q", texts(evs))
	}
}
