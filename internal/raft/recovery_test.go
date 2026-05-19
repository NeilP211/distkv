package raft_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/raftstore"
	"github.com/NeilP211/distkv/internal/simnet"
)

// ──────────────────────────────────────────────────────────────────────────
// helpers
// ──────────────────────────────────────────────────────────────────────────

// boltStorages opens a fresh BoltStorage for each node ID in ids, rooted
// under dir.  Returns storages map and a cleanup func that closes all stores.
func boltStorages(t *testing.T, ids []raft.NodeID, dir string) (map[raft.NodeID]*raftstore.BoltStorage, func()) {
	t.Helper()
	stores := make(map[raft.NodeID]*raftstore.BoltStorage, len(ids))
	for _, id := range ids {
		path := filepath.Join(dir, fmt.Sprintf("%s.db", id))
		s, err := raftstore.Open(path)
		if err != nil {
			t.Fatalf("Open(%s): %v", id, err)
		}
		stores[id] = s
	}
	cleanup := func() {
		for _, s := range stores {
			_ = s.Close()
		}
	}
	return stores, cleanup
}

// asRaftStorage converts a map[NodeID]*BoltStorage to map[NodeID]raft.Storage.
func asRaftStorage(m map[raft.NodeID]*raftstore.BoltStorage) map[raft.NodeID]raft.Storage {
	out := make(map[raft.NodeID]raft.Storage, len(m))
	for id, s := range m {
		out[id] = s
	}
	return out
}

// ──────────────────────────────────────────────────────────────────────────
// TestRecoveryPersistsLogAndTerm
//
// 3-node cluster over BoltStorage.  Drive to a stable leader, propose several
// entries, wait for them to commit on every node.  Then simulate a crash+restart
// of one follower: close its BoltStorage, open a fresh one from the same path,
// build a new raft.Node, re-wire to simnet.  Verify:
//
//  1. After restart, the restarted node's persisted log and term are intact.
//  2. The cluster re-elects a leader (or keeps the existing one) and continues
//     to commit new proposals.
//  3. Every entry committed before the crash is still present after.
// ──────────────────────────────────────────────────────────────────────────

func TestRecoveryPersistsLogAndTerm(t *testing.T) {
	dir := t.TempDir()
	peers := []raft.NodeID{"n1", "n2", "n3"}

	// ── Phase 1: initial cluster ─────────────────────────────────────────
	stores, closeAll := boltStorages(t, peers, dir)
	defer closeAll()

	c := newClusterWith(t, 3, 42, asRaftStorage(stores))
	leader := c.waitOneLeader(300)

	// Propose several entries and wait for full replication.
	preCrashEntries := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}
	var lastPreCrashIdx uint64
	for _, data := range preCrashEntries {
		idx, err := c.node[leader].Propose(data)
		if err != nil {
			t.Fatalf("Propose(%q): %v", data, err)
		}
		lastPreCrashIdx = idx
	}
	// Tick long enough for full replication and commit on all nodes.
	c.runTicks(100)
	for _, id := range peers {
		if ci := c.node[id].CommitIndex(); ci < lastPreCrashIdx {
			t.Fatalf("%s commitIndex = %d before crash, want >= %d", id, ci, lastPreCrashIdx)
		}
	}

	// Record the committed entries at the leader so we can compare post-restart.
	preEntries := c.node[leader].LogEntries(1, lastPreCrashIdx+1)
	if uint64(len(preEntries)) != lastPreCrashIdx {
		t.Fatalf("leader has %d entries, want %d", len(preEntries), lastPreCrashIdx)
	}

	// ── Phase 2: crash + restart one follower ────────────────────────────
	// Pick a follower (not the leader) to crash.
	var crashNode raft.NodeID
	for _, id := range peers {
		if id != leader {
			crashNode = id
			break
		}
	}

	// Capture the persisted term before closing.
	termBeforeCrash := c.node[crashNode].Term()

	// Close the BoltStorage for crashNode.  (The raft.Node is abandoned in-place;
	// the simnet handler will be replaced when we re-register below.)
	if err := stores[crashNode].Close(); err != nil {
		t.Fatalf("Close(%s): %v", crashNode, err)
	}
	// Mark the node as crashed in the simnet so it stops receiving messages.
	c.net.Crash(crashNode)

	// ── Phase 3: reopen storage and create fresh Node ────────────────────
	path := filepath.Join(dir, fmt.Sprintf("%s.db", crashNode))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("db file for %s missing after crash: %v", crashNode, err)
	}
	newStore, err := raftstore.Open(path)
	if err != nil {
		t.Fatalf("reopen %s: %v", crashNode, err)
	}
	t.Cleanup(func() { _ = newStore.Close() })

	// Verify persisted term is intact.
	hs, err := newStore.LoadHardState()
	if err != nil {
		t.Fatalf("LoadHardState after reopen: %v", err)
	}
	if hs.CurrentTerm < termBeforeCrash {
		t.Fatalf("persisted term %d < pre-crash term %d", hs.CurrentTerm, termBeforeCrash)
	}

	// Verify the pre-crash log entries are intact in the store.
	for i := uint64(1); i <= lastPreCrashIdx; i++ {
		got, err := newStore.Entries(i, i+1)
		if err != nil {
			t.Fatalf("Entries(%d,%d) after reopen: %v", i, i+1, err)
		}
		if len(got) != 1 {
			t.Fatalf("Entries(%d) = %d entries, want 1", i, len(got))
		}
		want := preEntries[i-1]
		if got[0].Term != want.Term || got[0].Index != want.Index {
			t.Fatalf("entry@%d = {Term:%d,Index:%d}, want {Term:%d,Index:%d}",
				i, got[0].Term, got[0].Index, want.Term, want.Index)
		}
		// EntryNormal data must match; skip EntryNoop (Data is nil).
		if want.Type == raft.EntryNormal && !bytes.Equal(got[0].Data, want.Data) {
			t.Fatalf("entry@%d data = %q, want %q", i, got[0].Data, want.Data)
		}
	}

	// Build a fresh Node over the recovered storage.
	newNode, err := raft.NewNode(raft.Config{
		ID:                 crashNode,
		Peers:              peers,
		Storage:            newStore,
		Transport:          c.net.Node(crashNode),
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
	})
	if err != nil {
		t.Fatalf("NewNode after reopen: %v", err)
	}
	// Replace the node in the cluster and re-register its Step handler.
	c.node[crashNode] = newNode
	c.net.Register(crashNode, newNode.Step)
	// Bring the node back online.
	c.net.Recover(crashNode)

	// ── Phase 4: cluster re-stabilises and new proposals commit ──────────
	// Allow time for the restarted node to catch up and for a leader to assert.
	c.runTicks(200)

	// Verify the cluster still has a single leader.
	_ = c.waitOneLeader(200)

	// Propose a new entry post-restart and confirm it commits on all nodes.
	newLeader := c.waitOneLeader(200)
	postIdx, err := c.node[newLeader].Propose([]byte("post-crash"))
	if err != nil {
		t.Fatalf("Propose after restart: %v", err)
	}
	c.runTicks(100)
	for _, id := range peers {
		if ci := c.node[id].CommitIndex(); ci < postIdx {
			t.Fatalf("%s commitIndex = %d after restart, want >= %d", id, ci, postIdx)
		}
	}

	// ── Phase 5: zero data loss — pre-crash committed entries intact ──────
	// Every entry that was committed before the crash must still be present
	// on the restarted node.
	recoveredEntries := c.node[crashNode].LogEntries(1, lastPreCrashIdx+1)
	if uint64(len(recoveredEntries)) != lastPreCrashIdx {
		t.Fatalf("restarted node has %d pre-crash entries, want %d",
			len(recoveredEntries), lastPreCrashIdx)
	}
	for i, e := range recoveredEntries {
		ref := preEntries[i]
		if e.Term != ref.Term || e.Index != ref.Index {
			t.Fatalf("recovered entry[%d] = {T:%d,I:%d}, want {T:%d,I:%d}",
				i, e.Term, e.Index, ref.Term, ref.Index)
		}
		if ref.Type == raft.EntryNormal && !bytes.Equal(e.Data, ref.Data) {
			t.Fatalf("recovered entry[%d].Data = %q, want %q", i, e.Data, ref.Data)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────
// TestRecoveryAllNodesCrash
//
// All 3 nodes crash simultaneously.  Re-open all storages from the same
// paths, build fresh Nodes, re-wire, and confirm the cluster elects a leader
// and commits a new proposal — verifying that durable state is sufficient to
// resume operation after a total cluster restart.
// ──────────────────────────────────────────────────────────────────────────

func TestRecoveryAllNodesCrash(t *testing.T) {
	dir := t.TempDir()
	peers := []raft.NodeID{"n1", "n2", "n3"}

	// ── Phase 1: run a cluster, commit some entries ──────────────────────
	stores, _ := boltStorages(t, peers, dir)
	// NOTE: we intentionally do NOT defer closeAll here; we close manually below.

	net1 := simnet.NewNetwork(99)
	nodes1 := make(map[raft.NodeID]*raft.Node, 3)
	for _, id := range peers {
		cfg := raft.Config{
			ID:                 id,
			Peers:              peers,
			Storage:            stores[id],
			Transport:          net1.Node(id),
			ElectionTimeoutMin: 10,
			ElectionTimeoutMax: 20,
			HeartbeatInterval:  3,
		}
		nd, err := raft.NewNode(cfg)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		nodes1[id] = nd
	}
	for _, id := range peers {
		nd := nodes1[id]
		net1.Register(id, nd.Step)
	}

	// Find a leader and commit entries.
	c1 := &cluster{t: t, net: net1, ids: peers, node: nodes1}
	leader := c1.waitOneLeader(300)
	var lastIdx uint64
	for _, payload := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		idx, err := c1.node[leader].Propose(payload)
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		lastIdx = idx
	}
	c1.runTicks(100)
	for _, id := range peers {
		if ci := c1.node[id].CommitIndex(); ci < lastIdx {
			t.Fatalf("%s commitIndex = %d, want >= %d", id, ci, lastIdx)
		}
	}

	// Close ALL storages — simulate total cluster crash.
	for _, s := range stores {
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// ── Phase 2: restart everything from disk ────────────────────────────
	stores2 := make(map[raft.NodeID]*raftstore.BoltStorage, 3)
	for _, id := range peers {
		path := filepath.Join(dir, fmt.Sprintf("%s.db", id))
		s, err := raftstore.Open(path)
		if err != nil {
			t.Fatalf("reopen(%s): %v", id, err)
		}
		stores2[id] = s
	}
	t.Cleanup(func() {
		for _, s := range stores2 {
			_ = s.Close()
		}
	})

	net2 := simnet.NewNetwork(99)
	nodes2 := make(map[raft.NodeID]*raft.Node, 3)
	for _, id := range peers {
		cfg := raft.Config{
			ID:                 id,
			Peers:              peers,
			Storage:            stores2[id],
			Transport:          net2.Node(id),
			ElectionTimeoutMin: 10,
			ElectionTimeoutMax: 20,
			HeartbeatInterval:  3,
		}
		nd, err := raft.NewNode(cfg)
		if err != nil {
			t.Fatalf("NewNode after restart (%s): %v", id, err)
		}
		nodes2[id] = nd
	}
	for _, id := range peers {
		nd := nodes2[id]
		net2.Register(id, nd.Step)
	}

	c2 := &cluster{t: t, net: net2, ids: peers, node: nodes2}
	newLeader := c2.waitOneLeader(400)

	// Post-restart proposal must commit everywhere.
	postIdx, err := c2.node[newLeader].Propose([]byte("post-restart"))
	if err != nil {
		t.Fatalf("Propose after total restart: %v", err)
	}
	c2.runTicks(100)
	for _, id := range peers {
		if ci := c2.node[id].CommitIndex(); ci < postIdx {
			t.Fatalf("%s commitIndex = %d post-restart, want >= %d", id, ci, postIdx)
		}
	}
}
