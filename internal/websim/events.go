package websim

import "fmt"

// Event is a single human-readable narration entry shown in the UI event log
// and streamed to the browser as an "event" frame. Seq is assigned by the
// streaming layer (DiffEvents leaves it zero).
type Event struct {
	Seq  int    `json:"seq"`
	Kind string `json:"kind"` // "role" | "commit" | "fault"
	Text string `json:"text"`
}

// DiffEvents compares a previous and current cluster snapshot and returns the
// notable changes as narration events: per-node role transitions, per-node
// crash/recover, and an advance of the cluster's high-water commit index.
// Identical snapshots produce no events.
func DiffEvents(prev, cur ClusterState) []Event {
	prevByID := make(map[string]NodeState, len(prev.Nodes))
	for _, n := range prev.Nodes {
		prevByID[n.ID] = n
	}

	var events []Event
	for _, n := range cur.Nodes {
		p, ok := prevByID[n.ID]
		if !ok {
			continue // node not present before (e.g. first snapshot)
		}
		if p.Role != n.Role {
			events = append(events, Event{Kind: "role", Text: roleText(n.ID, n.Role, n.Term)})
		}
		if p.Up != n.Up {
			verb := "recovered"
			if !n.Up {
				verb = "crashed"
			}
			events = append(events, Event{Kind: "fault", Text: fmt.Sprintf("%s %s", n.ID, verb)})
		}
	}

	if pc, cc := maxCommit(prev), maxCommit(cur); cc > pc {
		events = append(events, Event{Kind: "commit", Text: fmt.Sprintf("commit index → %d", cc)})
	}
	return events
}

// roleText renders a role transition for a node.
func roleText(id, role string, term uint64) string {
	if role == "Leader" {
		return fmt.Sprintf("%s became Leader (term %d)", id, term)
	}
	return fmt.Sprintf("%s → %s (term %d)", id, role, term)
}

// maxCommit returns the highest commit index across all nodes in a snapshot.
func maxCommit(s ClusterState) uint64 {
	var m uint64
	for _, n := range s.Nodes {
		if n.Commit > m {
			m = n.Commit
		}
	}
	return m
}
