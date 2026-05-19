package raft_test

import (
	"bytes"
	"testing"

	"github.com/NeilP211/distkv/internal/raft"
)

// logEntries returns every entry in a node's log via repeated Ready-style
// inspection.  Since Ready advances lastApplied, this collects the log by
// reading committed entries; for full-log assertions we instead inspect
// commitIndex and the storage.
func nodeCommit(c *cluster, id raft.NodeID) uint64 {
	return c.node[id].CommitIndex()
}

func TestProposeReplicatesAndCommits(t *testing.T) {
	c := newCluster(t, 3, 11)
	leader := c.waitOneLeader(200)

	idx, err := c.node[leader].Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	// Run enough ticks for replication + commit advancement to propagate.
	c.runTicks(40)

	for _, id := range c.ids {
		if got := nodeCommit(c, id); got < idx {
			t.Fatalf("%s commitIndex = %d, want >= %d", id, got, idx)
		}
	}

	// Every node's Ready yields the proposed entry exactly once.
	for _, id := range c.ids {
		var found int
		for _, e := range c.node[id].Ready() {
			if e.Type == raft.EntryNormal && bytes.Equal(e.Data, []byte("hello")) {
				found++
			}
		}
		if found != 1 {
			t.Fatalf("%s Ready yielded entry %d times, want 1", id, found)
		}
		// A second Ready call yields nothing new.
		if extra := c.node[id].Ready(); len(extra) != 0 {
			t.Fatalf("%s second Ready returned %d entries, want 0", id, len(extra))
		}
	}
}

func TestSingleNodeProposeCommitsImmediately(t *testing.T) {
	ct := &countingTransport{}
	nd := newSingleNode(t, ct)

	// Tick the solo node to leadership.
	for i := 0; i < 5; i++ {
		nd.Tick()
	}
	if nd.Role() != raft.Leader {
		t.Fatalf("role = %v, want Leader", nd.Role())
	}

	// Drain the no-op entry committed on becoming leader so Ready below
	// reports only the proposed entry.
	beforeCommit := nd.CommitIndex()
	_ = nd.Ready()

	idx, err := nd.Propose([]byte("solo-value"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	// The leader alone is a majority, so the entry commits immediately —
	// without any transport activity.
	if got := nd.CommitIndex(); got < idx {
		t.Fatalf("commitIndex = %d after Propose at idx %d, want >= %d", got, idx, idx)
	}
	if got := nd.CommitIndex(); got <= beforeCommit {
		t.Fatalf("commitIndex did not advance past %d (got %d)", beforeCommit, got)
	}

	// Ready must yield the proposed entry exactly once.
	ready := nd.Ready()
	found := 0
	for _, e := range ready {
		if e.Index == idx {
			if e.Type != raft.EntryNormal || !bytes.Equal(e.Data, []byte("solo-value")) {
				t.Fatalf("Ready entry@%d = %+v, want EntryNormal solo-value", idx, e)
			}
			found++
		}
	}
	if found != 1 {
		t.Fatalf("Ready yielded proposed entry %d times, want 1", found)
	}
	if extra := nd.Ready(); len(extra) != 0 {
		t.Fatalf("second Ready returned %d entries, want 0", len(extra))
	}

	if ct.sends != 0 {
		t.Fatalf("transport Send count = %d, want 0 (single-node needs no transport)", ct.sends)
	}
}

func TestProposeOnNonLeaderFails(t *testing.T) {
	c := newCluster(t, 3, 5)
	leader := c.waitOneLeader(200)
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if _, err := c.node[id].Propose([]byte("x")); err != raft.ErrNotLeader {
			t.Fatalf("Propose on follower %s err = %v, want ErrNotLeader", id, err)
		}
	}
}

func TestDivergentFollowerLogRepaired(t *testing.T) {
	// Pre-seed n3 with a bogus divergent tail at a high term so it must be
	// truncated and overwritten by the leader's log.
	bogus := raft.NewMemStorage()
	if err := bogus.AppendEntries([]raft.LogEntry{
		{Term: 4, Index: 1, Type: raft.EntryNormal, Data: []byte("bogus1")},
		{Term: 4, Index: 2, Type: raft.EntryNormal, Data: []byte("bogus2")},
		{Term: 4, Index: 3, Type: raft.EntryNormal, Data: []byte("bogus3")},
	}); err != nil {
		t.Fatalf("seed bogus: %v", err)
	}
	storages := map[raft.NodeID]raft.Storage{"n3": bogus}
	c := newClusterWith(t, 3, 9, storages)

	leader := c.waitOneLeader(300)
	// The leader must be n1 or n2 (n3's high term could let it win; if so
	// the test still exercises convergence, just from the other side).
	idx, err := c.node[leader].Propose([]byte("real"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	c.runTicks(80)

	// All nodes converge: same commit index covering the proposal, and the
	// committed entry at idx is identical everywhere.
	want := entryAt(t, c, leader, idx)
	for _, id := range c.ids {
		if nodeCommit(c, id) < idx {
			t.Fatalf("%s commitIndex = %d, want >= %d", id, nodeCommit(c, id), idx)
		}
		got := entryAt(t, c, id, idx)
		if got.Term != want.Term || !bytes.Equal(got.Data, want.Data) {
			t.Fatalf("%s entry@%d = %+v, want %+v", id, idx, got, want)
		}
	}
}

func TestEntryNotCommittedWithoutMajority(t *testing.T) {
	c := newCluster(t, 5, 17)
	leader := c.waitOneLeader(300)

	// Partition the leader together with exactly one follower (a minority
	// of 2) away from the other three.
	var others []raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			others = append(others, id)
		}
	}
	minorityPeer := others[0]
	majority := others[1:]

	c.net.Partition(
		[]raft.NodeID{leader, minorityPeer},
		majority,
	)

	// Propose on the (now minority) leader; it must NOT commit.
	idx, err := c.node[leader].Propose([]byte("doomed"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	c.runTicks(60)
	if nodeCommit(c, leader) >= idx {
		t.Fatalf("minority leader committed index %d (commit=%d), must not",
			idx, nodeCommit(c, leader))
	}

	// Heal: the cluster reconciles. A leader emerges and commit advances on
	// a majority. The doomed entry may survive or be overwritten depending
	// on which side wins; either way the cluster converges.
	c.net.Heal()
	newLeader := c.waitOneLeader(400)
	// Drive a fresh proposal so a current-term entry commits everywhere.
	fidx, err := c.node[newLeader].Propose([]byte("after-heal"))
	if err != nil {
		t.Fatalf("Propose after heal: %v", err)
	}
	c.runTicks(80)
	for _, id := range c.ids {
		if nodeCommit(c, id) < fidx {
			t.Fatalf("%s commitIndex = %d, want >= %d after heal", id, nodeCommit(c, id), fidx)
		}
	}
}

// TestPriorTermCommitRule exercises §5.4.2: an entry from a prior term is only
// marked committed once a current-term entry above it is committed.  We seed a
// 3-node cluster where every node already holds an uncommitted entry from an
// old term (term 2).  No node has it committed.  After an election the new
// leader appends a no-op in its current term; once that no-op replicates to a
// majority, BOTH the no-op and the prior-term entry become committed together.
func TestPriorTermCommitRule(t *testing.T) {
	seed := func() raft.Storage {
		s := raft.NewMemStorage()
		if err := s.AppendEntries([]raft.LogEntry{
			{Term: 2, Index: 1, Type: raft.EntryNormal, Data: []byte("old")},
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		return s
	}
	storages := map[raft.NodeID]raft.Storage{
		"n1": seed(), "n2": seed(), "n3": seed(),
	}
	c := newClusterWith(t, 3, 4, storages)

	// Before any election, the prior-term entry is uncommitted everywhere.
	for _, id := range c.ids {
		if nodeCommit(c, id) != 0 {
			t.Fatalf("%s commitIndex = %d before election, want 0", id, nodeCommit(c, id))
		}
	}

	c.waitOneLeader(300)
	c.runTicks(40)

	// becomeLeader appended a no-op at index 2 in the leader's current
	// term. Once it commits, the prior-term entry at index 1 is committed
	// transitively. Both nodes must now show commitIndex >= 2.
	for _, id := range c.ids {
		if nodeCommit(c, id) < 2 {
			t.Fatalf("%s commitIndex = %d, want >= 2 (no-op commit drags prior term)",
				id, nodeCommit(c, id))
		}
		// The committed entry at index 1 is the prior-term entry.
		e := entryAt(t, c, id, 1)
		if e.Term != 2 || !bytes.Equal(e.Data, []byte("old")) {
			t.Fatalf("%s entry@1 = %+v, want prior-term old entry", id, e)
		}
	}
}

// entryAt returns the log entry at index idx on node id, failing the test if
// it cannot be read.
func entryAt(t *testing.T, c *cluster, id raft.NodeID, idx uint64) raft.LogEntry {
	t.Helper()
	es := c.node[id].LogEntries(idx, idx+1)
	if len(es) != 1 {
		t.Fatalf("%s has no entry at index %d", id, idx)
	}
	return es[0]
}
