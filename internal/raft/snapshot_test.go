package raft_test

import (
	"testing"

	"github.com/NeilP211/distkv/internal/raft"
)

// proposeAndCommit proposes data via the cluster leader and ticks until the
// entry is committed on every node, returning the entry's index.
func proposeAndCommit(t *testing.T, c *cluster, leader raft.NodeID, data []byte) uint64 {
	t.Helper()
	idx, err := c.node[leader].Propose(data)
	if err != nil {
		t.Fatalf("Propose(%q): %v", data, err)
	}
	c.runTicks(40)
	for _, id := range c.ids {
		if ci := c.node[id].CommitIndex(); ci < idx {
			t.Fatalf("%s commitIndex=%d, want >= %d", id, ci, idx)
		}
	}
	return idx
}

// compactNodes drains Ready() and calls CompactTo(index) on each listed node,
// so that whichever of them later becomes leader serves a compacted log.
func compactNodes(t *testing.T, c *cluster, index uint64, data []byte, ids ...raft.NodeID) {
	t.Helper()
	for _, id := range ids {
		nd := c.node[id]
		nd.Ready() // advance lastApplied to commitIndex
		term, err := nd.TermOf(index)
		if err != nil {
			t.Fatalf("%s TermOf(%d): %v", id, index, err)
		}
		if err := nd.CompactTo(index, term, data); err != nil {
			t.Fatalf("%s CompactTo(%d): %v", id, index, err)
		}
		if nd.FirstIndex() != index+1 {
			t.Fatalf("%s FirstIndex=%d after compact, want %d", id, nd.FirstIndex(), index+1)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────
// TestCompactToAdvancesFirstIndex
//
// CompactTo records a snapshot and compacts the log: FirstIndex must advance
// past the snapshot index, and Term(snapshotIndex) must still resolve (the
// snapshot vouches for its own index's term).
// ──────────────────────────────────────────────────────────────────────────
func TestCompactToAdvancesFirstIndex(t *testing.T) {
	c := newCluster(t, 3, 7)
	leader := c.waitOneLeader(300)

	var lastIdx uint64
	for _, d := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")} {
		lastIdx = proposeAndCommit(t, c, leader, d)
	}

	nd := c.node[leader]
	// Drain so lastApplied catches up to commitIndex (CompactTo requires
	// index <= lastApplied).
	nd.Ready()

	wantTerm, err := nd.TermOf(lastIdx)
	if err != nil {
		t.Fatalf("TermOf(%d): %v", lastIdx, err)
	}

	if err := nd.CompactTo(lastIdx, wantTerm, []byte("state")); err != nil {
		t.Fatalf("CompactTo(%d): %v", lastIdx, err)
	}

	if got := nd.FirstIndex(); got != lastIdx+1 {
		t.Fatalf("FirstIndex()=%d, want %d", got, lastIdx+1)
	}
	// Term at the snapshot index must still resolve.
	gotTerm, err := nd.TermOf(lastIdx)
	if err != nil {
		t.Fatalf("TermOf(%d) after compaction: %v", lastIdx, err)
	}
	if gotTerm != wantTerm {
		t.Fatalf("TermOf(%d)=%d after compaction, want %d", lastIdx, gotTerm, wantTerm)
	}
	// An index strictly inside the compacted range must now be gone.
	if _, err := nd.TermOf(lastIdx - 1); err == nil {
		t.Fatalf("TermOf(%d) succeeded, want compacted error", lastIdx-1)
	}
}

// TestCompactToRejectsUnappliedIndex: CompactTo must refuse to snapshot past
// what the node has applied.
func TestCompactToRejectsUnappliedIndex(t *testing.T) {
	c := newCluster(t, 3, 11)
	leader := c.waitOneLeader(300)
	lastIdx := proposeAndCommit(t, c, leader, []byte("x"))

	nd := c.node[leader]
	// Do NOT drain Ready(): lastApplied is still behind commitIndex.
	if err := nd.CompactTo(lastIdx, 1, []byte("state")); err == nil {
		t.Fatalf("CompactTo past lastApplied succeeded, want ErrSnapshotOutOfRange")
	}
}

// TestCompactToAlreadyCompactedIsNoop: snapshotting a point already covered by
// an earlier snapshot is a harmless no-op.
func TestCompactToAlreadyCompactedIsNoop(t *testing.T) {
	c := newCluster(t, 3, 13)
	leader := c.waitOneLeader(300)
	var lastIdx uint64
	for _, d := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		lastIdx = proposeAndCommit(t, c, leader, d)
	}
	nd := c.node[leader]
	nd.Ready()
	term, _ := nd.TermOf(lastIdx)
	if err := nd.CompactTo(lastIdx, term, []byte("s1")); err != nil {
		t.Fatalf("first CompactTo: %v", err)
	}
	// Re-compacting an earlier (already-compacted) index is a no-op.
	if err := nd.CompactTo(lastIdx-1, 1, []byte("s0")); err != nil {
		t.Fatalf("re-CompactTo earlier index: %v", err)
	}
	if got := nd.FirstIndex(); got != lastIdx+1 {
		t.Fatalf("FirstIndex()=%d after no-op recompact, want %d", got, lastIdx+1)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// TestLeaderSendsInstallSnapshotToLaggingFollower
//
// Drive a 3-node cluster, isolate one follower, commit enough entries that the
// leader compacts past the follower's log, then heal the follower.  The leader
// must catch it up via MsgInstallSnapshot rather than MsgAppendEntries; the
// follower must end with an identical committed log state.
// ──────────────────────────────────────────────────────────────────────────
func TestLeaderSendsInstallSnapshotToLaggingFollower(t *testing.T) {
	c := newCluster(t, 3, 23)
	leader := c.waitOneLeader(300)

	// Identify a follower to isolate.
	var lagger raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			lagger = id
			break
		}
	}

	// Isolate the lagger so it cannot replicate.
	c.net.Isolate(lagger)

	// Commit a batch of entries with only the remaining two nodes (still a
	// majority).
	var lastIdx uint64
	for i := 0; i < 8; i++ {
		idx, err := c.node[leader].Propose([]byte{byte('A' + i)})
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		lastIdx = idx
		c.runTicks(10)
	}
	c.runTicks(40)

	// Compact every connected node's log past the lagger's position, so
	// whichever of them serves as leader after the lagger rejoins (its
	// election timer may have advanced during isolation) has a compacted log.
	var connected []raft.NodeID
	for _, id := range c.ids {
		if id != lagger {
			connected = append(connected, id)
		}
	}
	compactNodes(t, c, lastIdx, []byte("snapshot-state"), connected...)

	// Heal the lagger.  It is now far behind the compacted log boundary, so
	// the leader must use InstallSnapshot.
	c.net.Recover(lagger)
	c.runTicks(200)

	// The lagger must have caught up: its commitIndex reaches lastIdx.
	if ci := c.node[lagger].CommitIndex(); ci < lastIdx {
		t.Fatalf("lagger %s commitIndex=%d after recovery, want >= %d", lagger, ci, lastIdx)
	}
	// And it must have installed the snapshot, advancing its own FirstIndex.
	if fi := c.node[lagger].FirstIndex(); fi <= 1 {
		t.Fatalf("lagger %s FirstIndex=%d, want > 1 (snapshot installed)", lagger, fi)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// TestFollowerReportsInstalledSnapshotViaPendingSnapshot
//
// A follower that installs a snapshot must surface it exactly once through
// PendingSnapshot.
// ──────────────────────────────────────────────────────────────────────────
func TestFollowerReportsInstalledSnapshotViaPendingSnapshot(t *testing.T) {
	c := newCluster(t, 3, 31)
	leader := c.waitOneLeader(300)

	var lagger raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			lagger = id
			break
		}
	}
	c.net.Isolate(lagger)

	var lastIdx uint64
	for i := 0; i < 6; i++ {
		idx, err := c.node[leader].Propose([]byte{byte('a' + i)})
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		lastIdx = idx
		c.runTicks(10)
	}
	c.runTicks(40)

	want := []byte("the-snapshot-bytes")
	var connected []raft.NodeID
	for _, id := range c.ids {
		if id != lagger {
			connected = append(connected, id)
		}
	}
	compactNodes(t, c, lastIdx, want, connected...)

	c.net.Recover(lagger)
	c.runTicks(200)

	snap, ok := c.node[lagger].PendingSnapshot()
	if !ok {
		t.Fatalf("lagger %s reported no pending snapshot", lagger)
	}
	if snap.Index != lastIdx {
		t.Fatalf("pending snapshot Index=%d, want %d", snap.Index, lastIdx)
	}
	if string(snap.Data) != string(want) {
		t.Fatalf("pending snapshot Data=%q, want %q", snap.Data, want)
	}
	// Exactly once: a second call must report nothing.
	if _, ok := c.node[lagger].PendingSnapshot(); ok {
		t.Fatalf("PendingSnapshot returned a snapshot twice")
	}
}

// ──────────────────────────────────────────────────────────────────────────
// TestNewNodeFromSnapshotInitializesAppliedAndCommit
//
// A node constructed over a Storage that already holds a snapshot at index N
// must start with commitIndex and lastApplied both equal to N — otherwise
// Ready() would try to slice compacted entries.
// ──────────────────────────────────────────────────────────────────────────
func TestNewNodeFromSnapshotInitializesAppliedAndCommit(t *testing.T) {
	st := raft.NewMemStorage()
	// Seed a log and a snapshot at index 5.
	entries := make([]raft.LogEntry, 5)
	for i := range entries {
		entries[i] = raft.LogEntry{Term: 1, Index: uint64(i + 1), Type: raft.EntryNormal}
	}
	if err := st.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := st.SaveSnapshot(raft.Snapshot{Index: 5, Term: 1, Data: []byte("s")}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := st.Compact(5); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	nd, err := raft.NewNode(raft.Config{
		ID:                 "solo",
		Peers:              []raft.NodeID{"solo"},
		Storage:            st,
		Transport:          &countingTransport{},
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if got := nd.CommitIndex(); got != 5 {
		t.Fatalf("CommitIndex()=%d, want 5", got)
	}
	// Ready() must not panic slicing compacted entries; with lastApplied==5
	// and commitIndex==5 it simply returns nil.
	if ready := nd.Ready(); ready != nil {
		t.Fatalf("Ready()=%v, want nil (nothing unapplied)", ready)
	}
}
