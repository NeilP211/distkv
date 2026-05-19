package server

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/raftstore"
	"github.com/NeilP211/distkv/internal/simnet"
	"github.com/NeilP211/distkv/internal/store"
)

// kvPut builds a Put command for key=value.
func kvPut(key, value string) store.Command {
	return store.Command{Op: store.OpPut, Key: key, Value: value}
}

// newRaftNodeWith builds a RaftNode over the supplied raft.Storage and
// transport, with a configurable snapshot threshold.  It is the persistent /
// restartable counterpart of newRaftNode.
func newRaftNodeWith(t *testing.T, id raft.NodeID, peers []raft.NodeID, tr raft.Transport, st raft.Storage, threshold uint64) *RaftNode {
	t.Helper()
	rn, err := NewRaftNode(RaftNodeConfig{
		Raft: raft.Config{
			ID:                 id,
			Peers:              peers,
			Storage:            st,
			Transport:          tr,
			ElectionTimeoutMin: 10,
			ElectionTimeoutMax: 20,
			HeartbeatInterval:  3,
		},
		TickInterval:      testTick,
		SnapshotThreshold: threshold,
	})
	if err != nil {
		t.Fatalf("NewRaftNode(%s): %v", id, err)
	}
	return rn
}

// openBolt opens a BoltStorage for id under dir, registering Close cleanup.
func openBolt(t *testing.T, dir string, id raft.NodeID) *raftstore.BoltStorage {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("%s.db", id))
	st, err := raftstore.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", id, err)
	}
	return st
}

// proposeN proposes count Put commands (k0..k{count-1}), blocking on each until
// applied.  It tolerates leadership changes among nodes: on ErrNotLeader it
// re-discovers the current leader and retries the same key.
func proposeN(t *testing.T, nodes map[raft.NodeID]*RaftNode, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		deadline := time.Now().Add(5 * time.Second)
		for {
			leader := currentLeader(nodes)
			if leader == nil {
				if time.Now().After(deadline) {
					t.Fatalf("Propose(%s): no leader available", k)
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := leader.Propose(ctx, kvPut(k, v))
			cancel()
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Propose(%s): %v", k, err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// currentLeader returns whichever node currently reports itself leader, or nil.
func currentLeader(nodes map[raft.NodeID]*RaftNode) *RaftNode {
	for _, rn := range nodes {
		if rn.Status().Role == "Leader" {
			return rn
		}
	}
	return nil
}

// assertHasAll verifies rn's state machine holds k0..k{count-1} with the
// expected values.
func assertHasAll(t *testing.T, rn *RaftNode, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		k := fmt.Sprintf("k%d", i)
		want := fmt.Sprintf("v%d", i)
		if got, ok := rn.LocalGet(k); !ok || got != want {
			t.Fatalf("LocalGet(%s)=%q,%v want %q,true", k, got, ok, want)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────
// TestRaftNode_AutomaticSnapshotCompactsLog
//
// A 3-node cluster with a small SnapshotThreshold: proposing many entries must
// trigger automatic snapshotting, advancing every node's FirstIndex past 1.
// ──────────────────────────────────────────────────────────────────────────
func TestRaftNode_AutomaticSnapshotCompactsLog(t *testing.T) {
	net := simnet.NewNetwork(5)
	ids := []raft.NodeID{"n1", "n2", "n3"}
	nodes := make(map[raft.NodeID]*RaftNode)
	for _, id := range ids {
		rn := newRaftNodeWith(t, id, ids, net.Node(id), raft.NewMemStorage(), 10)
		nodes[id] = rn
		net.Register(id, rn.Step)
	}
	for _, rn := range nodes {
		rn.Start()
	}
	defer func() {
		for _, rn := range nodes {
			rn.Stop()
		}
	}()

	waitForLeader(t, 5*time.Second, nodes)
	proposeN(t, nodes, 40)

	// Every node must eventually compact its log: FirstIndex advances past 1.
	for id, rn := range nodes {
		rn := rn
		waitFor(t, 5*time.Second, "node "+string(id)+" to compact its log", func() bool {
			return rn.node.FirstIndex() > 1
		})
	}
	// And every node still holds the full applied state.
	for _, rn := range nodes {
		waitFor(t, 3*time.Second, "node to apply all 40 entries", func() bool {
			_, ok := rn.LocalGet("k39")
			return ok
		})
		assertHasAll(t, rn, 40)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// TestRaftNode_LaggingNodeCatchesUpViaInstallSnapshot
//
// A 3-node cluster; one node is isolated while the others commit and compact
// far past it.  When the isolated node rejoins it must catch up via
// InstallSnapshot and end with identical state.
// ──────────────────────────────────────────────────────────────────────────
func TestRaftNode_LaggingNodeCatchesUpViaInstallSnapshot(t *testing.T) {
	net := simnet.NewNetwork(8)
	ids := []raft.NodeID{"n1", "n2", "n3"}
	nodes := make(map[raft.NodeID]*RaftNode)
	for _, id := range ids {
		rn := newRaftNodeWith(t, id, ids, net.Node(id), raft.NewMemStorage(), 10)
		nodes[id] = rn
		net.Register(id, rn.Step)
	}
	for _, rn := range nodes {
		rn.Start()
	}
	defer func() {
		for _, rn := range nodes {
			rn.Stop()
		}
	}()

	leader := waitForLeader(t, 5*time.Second, nodes)

	// Pick a follower to isolate.
	var lagger raft.NodeID
	for _, id := range ids {
		if nodes[id] != leader {
			lagger = id
			break
		}
	}
	net.Isolate(lagger)

	// Commit a large batch with the remaining majority.
	survivors := make(map[raft.NodeID]*RaftNode)
	for _, id := range ids {
		if id != lagger {
			survivors[id] = nodes[id]
		}
	}
	proposeN(t, survivors, 60)

	// Wait for the connected nodes to compact.
	for _, id := range ids {
		if id == lagger {
			continue
		}
		rn := nodes[id]
		waitFor(t, 5*time.Second, "connected node to compact", func() bool {
			return rn.node.FirstIndex() > 1
		})
	}

	// Rejoin the lagger; it must catch up via InstallSnapshot.
	net.Recover(lagger)
	laggerNode := nodes[lagger]
	waitFor(t, 8*time.Second, "lagging node to catch up", func() bool {
		_, ok := laggerNode.LocalGet("k59")
		return ok
	})
	assertHasAll(t, laggerNode, 60)
	if fi := laggerNode.node.FirstIndex(); fi <= 1 {
		t.Fatalf("lagger FirstIndex=%d, want > 1 (snapshot installed)", fi)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// TestRaftNode_CrashedNodeRecoversViaSnapshotFromDisk
//
// A 3-node cluster over BoltStorage.  One node is crashed; the cluster commits
// and compacts past the crashed node's persisted log.  The node is then
// restarted from its on-disk storage — its old log is stale and the leader
// must InstallSnapshot to bring it current.  Zero data loss: it ends holding
// every committed key.
// ──────────────────────────────────────────────────────────────────────────
func TestRaftNode_CrashedNodeRecoversViaSnapshotFromDisk(t *testing.T) {
	dir := t.TempDir()
	ids := []raft.NodeID{"n1", "n2", "n3"}

	net := simnet.NewNetwork(13)
	stores := make(map[raft.NodeID]*raftstore.BoltStorage)
	nodes := make(map[raft.NodeID]*RaftNode)
	for _, id := range ids {
		st := openBolt(t, dir, id)
		stores[id] = st
		rn := newRaftNodeWith(t, id, ids, net.Node(id), st, 10)
		nodes[id] = rn
		net.Register(id, rn.Step)
	}
	for _, rn := range nodes {
		rn.Start()
	}

	leader := waitForLeader(t, 5*time.Second, nodes)

	// Commit an initial batch everyone sees.
	proposeN(t, nodes, 8)

	// Pick a follower to crash.
	var crashID raft.NodeID
	for _, id := range ids {
		if nodes[id] != leader {
			crashID = id
			break
		}
	}

	// Crash the follower: stop its goroutines, close its storage, isolate it.
	nodes[crashID].Stop()
	if err := stores[crashID].Close(); err != nil {
		t.Fatalf("Close(%s): %v", crashID, err)
	}
	net.Crash(crashID)

	// Re-elect (the crashed node may have been needed for the old leader's
	// majority) and commit a large batch with the remaining two nodes.
	survivors := make(map[raft.NodeID]*RaftNode)
	for _, id := range ids {
		if id != crashID {
			survivors[id] = nodes[id]
		}
	}
	waitForLeader(t, 5*time.Second, survivors)
	proposeN(t, survivors, 80)

	// Wait for the survivors to compact past the crashed node's stale log.
	for id, rn := range survivors {
		rn := rn
		waitFor(t, 5*time.Second, "survivor "+string(id)+" to compact", func() bool {
			return rn.node.FirstIndex() > 1
		})
	}

	// ── restart the crashed node from disk ───────────────────────────────
	reopened := openBolt(t, dir, crashID)
	restarted := newRaftNodeWith(t, crashID, ids, net.Node(crashID), reopened, 10)
	net.Register(crashID, restarted.Step)
	restarted.Start()
	nodes[crashID] = restarted

	defer func() {
		for _, rn := range nodes {
			rn.Stop()
		}
		for _, st := range stores {
			_ = st.Close()
		}
		_ = reopened.Close()
	}()

	net.Recover(crashID)

	// The restarted node must catch up to every committed key via snapshot
	// install — zero data loss.
	waitFor(t, 10*time.Second, "restarted node to recover all state", func() bool {
		_, ok := restarted.LocalGet("k79")
		return ok
	})
	assertHasAll(t, restarted, 80)
	if fi := restarted.node.FirstIndex(); fi <= 1 {
		t.Fatalf("restarted node FirstIndex=%d, want > 1 (recovered via snapshot)", fi)
	}
}
