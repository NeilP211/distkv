package raft_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/NeilP211/distkv/internal/raft"
)

// --- membership test helpers -------------------------------------------------

// addJoiner constructs a fresh follower joining the cluster and wires it into
// the simnet. basePeers is the cluster configuration as it stands BEFORE this
// node joins: a joining node is told the current membership so that, when it
// later appends the conf-change entries, it derives the same configuration the
// leader did. storage may be nil, in which case a fresh MemStorage is used.
func (c *cluster) addJoiner(id raft.NodeID, basePeers []raft.NodeID, storage raft.Storage) {
	c.t.Helper()
	if storage == nil {
		storage = raft.NewMemStorage()
	}
	cfg := raft.Config{
		ID:                 id,
		Peers:              basePeers,
		Storage:            storage,
		Transport:          c.net.Node(id),
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
	}
	nd, err := raft.NewNode(cfg)
	if err != nil {
		c.t.Fatalf("NewNode(%s): %v", id, err)
	}
	c.node[id] = nd
	c.ids = append(c.ids, id)
	c.net.Register(id, nd.Step)
}

// currentLeader returns the single current leader, failing if there is not
// exactly one.
func (c *cluster) currentLeader() raft.NodeID {
	c.t.Helper()
	ls := c.leaders()
	if len(ls) != 1 {
		c.t.Fatalf("expected exactly one leader, got %v", ls)
	}
	return ls[0]
}

// membershipSettled reports whether every node in active sees exactly the
// membership want and is no longer in a joint configuration.
func (c *cluster) membershipSettled(active, want []raft.NodeID) bool {
	for _, id := range active {
		nd := c.node[id]
		if nd.ConfigJoint() {
			return false
		}
		if !sameIDs(nd.Members(), want) {
			return false
		}
	}
	return true
}

// waitMembership ticks until every active node has settled on want, or fails.
func (c *cluster) waitMembership(active, want []raft.NodeID, maxTicks int) {
	c.t.Helper()
	for i := 0; i < maxTicks; i++ {
		c.tickAll()
		if c.membershipSettled(active, want) {
			return
		}
	}
	c.t.Fatalf("membership did not settle to %v after %d ticks", want, maxTicks)
}

// proposeAndWaitCommit proposes one normal entry on the current leader and
// ticks until every active node has committed it.
func (c *cluster) proposeAndWaitCommit(active []raft.NodeID, maxTicks int) {
	c.t.Helper()
	leader := c.currentLeader()
	idx, err := c.node[leader].Propose([]byte("payload"))
	if err != nil {
		c.t.Fatalf("Propose on %s: %v", leader, err)
	}
	for i := 0; i < maxTicks; i++ {
		c.tickAll()
		done := true
		for _, id := range active {
			if c.node[id].CommitIndex() < idx {
				done = false
				break
			}
		}
		if done {
			return
		}
	}
	c.t.Fatalf("proposed entry %d not committed on all of %v", idx, active)
}

// removeMember proposes the removal of victim (which must not be the current
// leader), waits for the change to settle on activeAfter, then crashes the
// removed node — modelling an operator decommissioning it.
func (c *cluster) removeMember(victim raft.NodeID, activeAfter []raft.NodeID) {
	c.t.Helper()
	leader := c.currentLeader()
	if leader == victim {
		c.t.Fatalf("removeMember: victim %s is the current leader", victim)
	}
	if _, err := c.node[leader].ProposeConfChange(
		raft.ConfChange{Type: raft.ConfRemoveNode, Node: victim}); err != nil {
		c.t.Fatalf("ProposeConfChange remove %s: %v", victim, err)
	}
	c.waitMembership(activeAfter, sortIDs(activeAfter), 300)
	// The removed node no longer receives heartbeats; decommission it before
	// its election timeout fires so it cannot disrupt the cluster.
	c.net.Crash(victim)
}

// firstNonLeader returns an id in candidates that is not the current leader.
func (c *cluster) firstNonLeader(candidates []raft.NodeID) raft.NodeID {
	c.t.Helper()
	leader := c.currentLeader()
	for _, id := range candidates {
		if id != leader {
			return id
		}
	}
	c.t.Fatalf("no non-leader among %v", candidates)
	return ""
}

// sortIDs returns a sorted copy of ids.
func sortIDs(ids []raft.NodeID) []raft.NodeID {
	out := append([]raft.NodeID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// sameIDs reports whether a and b contain the same set of ids.
func sameIDs(a, b []raft.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	as, bs := sortIDs(a), sortIDs(b)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

// idsExcept returns ids with every element of drop removed.
func idsExcept(ids []raft.NodeID, drop ...raft.NodeID) []raft.NodeID {
	dropped := make(map[raft.NodeID]bool, len(drop))
	for _, d := range drop {
		dropped[d] = true
	}
	var out []raft.NodeID
	for _, id := range ids {
		if !dropped[id] {
			out = append(out, id)
		}
	}
	return out
}

// --- tests -------------------------------------------------------------------

// TestMembershipGrow3to5 grows a cluster from three nodes to five, one node at
// a time, and verifies a single leader holds throughout and proposals still
// commit on the enlarged cluster.
func TestMembershipGrow3to5(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.waitOneLeader(200)
	c.proposeAndWaitCommit([]raft.NodeID{"n1", "n2", "n3"}, 100)

	// Add n4. Its base configuration is the current three-node cluster.
	c.addJoiner("n4", []raft.NodeID{"n1", "n2", "n3"}, nil)
	leader := c.currentLeader()
	if _, err := c.node[leader].ProposeConfChange(
		raft.ConfChange{Type: raft.ConfAddNode, Node: "n4"}); err != nil {
		t.Fatalf("ProposeConfChange add n4: %v", err)
	}
	all4 := []raft.NodeID{"n1", "n2", "n3", "n4"}
	c.waitMembership(all4, all4, 300)

	// Add n5. Its base configuration is the current four-node cluster.
	c.addJoiner("n5", all4, nil)
	leader = c.currentLeader()
	if _, err := c.node[leader].ProposeConfChange(
		raft.ConfChange{Type: raft.ConfAddNode, Node: "n5"}); err != nil {
		t.Fatalf("ProposeConfChange add n5: %v", err)
	}
	all5 := []raft.NodeID{"n1", "n2", "n3", "n4", "n5"}
	c.waitMembership(all5, all5, 300)

	if got := len(c.leaders()); got != 1 {
		t.Fatalf("leader count after grow = %d, want 1", got)
	}
	c.proposeAndWaitCommit(all5, 200)
}

// TestMembershipShrink5to3 shrinks a five-node cluster to three by removing two
// non-leader members, verifying the cluster stays available throughout.
func TestMembershipShrink5to3(t *testing.T) {
	c := newCluster(t, 5, 7)
	c.waitOneLeader(300)
	all5 := []raft.NodeID{"n1", "n2", "n3", "n4", "n5"}
	c.proposeAndWaitCommit(all5, 200)

	// Remove a first non-leader node, leaving four.
	victim1 := c.firstNonLeader(all5)
	remaining4 := idsExcept(all5, victim1)
	c.removeMember(victim1, remaining4)

	// Remove a second non-leader node, leaving three.
	victim2 := c.firstNonLeader(remaining4)
	remaining3 := idsExcept(remaining4, victim2)
	c.removeMember(victim2, remaining3)

	if got := len(c.leaders()); got != 1 {
		t.Fatalf("leader count after shrink = %d, want 1", got)
	}
	c.proposeAndWaitCommit(remaining3, 200)
}

// TestConfChangeInProgressRejected verifies that a second membership change is
// rejected while the first is still in its joint phase.
func TestConfChangeInProgressRejected(t *testing.T) {
	c := newCluster(t, 3, 5)
	leader := c.waitOneLeader(200)

	// Isolate the leader so the joint-config entry cannot reach a quorum and
	// therefore cannot commit; the node remains in the joint configuration.
	c.net.Isolate(leader)
	if _, err := c.node[leader].ProposeConfChange(
		raft.ConfChange{Type: raft.ConfAddNode, Node: "n4"}); err != nil {
		t.Fatalf("first ProposeConfChange: %v", err)
	}
	if !c.node[leader].ConfigJoint() {
		t.Fatal("leader should be in a joint configuration after the first change")
	}

	_, err := c.node[leader].ProposeConfChange(
		raft.ConfChange{Type: raft.ConfAddNode, Node: "n5"})
	if !errors.Is(err, raft.ErrConfChangeInProgress) {
		t.Fatalf("second ProposeConfChange err = %v, want ErrConfChangeInProgress", err)
	}
}

// TestMembershipRecoveredOnRestart verifies that a node reconstructed from its
// persisted storage recovers the post-change cluster configuration by replay.
func TestMembershipRecoveredOnRestart(t *testing.T) {
	storages := map[raft.NodeID]raft.Storage{
		"n1": raft.NewMemStorage(),
		"n2": raft.NewMemStorage(),
		"n3": raft.NewMemStorage(),
	}
	c := newClusterWith(t, 3, 9, storages)
	c.waitOneLeader(200)

	c.addJoiner("n4", []raft.NodeID{"n1", "n2", "n3"}, nil)
	leader := c.currentLeader()
	if _, err := c.node[leader].ProposeConfChange(
		raft.ConfChange{Type: raft.ConfAddNode, Node: "n4"}); err != nil {
		t.Fatalf("ProposeConfChange add n4: %v", err)
	}
	all4 := []raft.NodeID{"n1", "n2", "n3", "n4"}
	c.waitMembership(all4, all4, 300)

	// Reconstruct n1 from its persisted storage. replayConfig must rebuild the
	// post-change membership from the EntryConfChange entries in the log.
	restarted, err := raft.NewNode(raft.Config{
		ID:                 "n1",
		Peers:              []raft.NodeID{"n1", "n2", "n3"},
		Storage:            storages["n1"],
		Transport:          c.net.Node("n1"),
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
	})
	if err != nil {
		t.Fatalf("reconstruct n1: %v", err)
	}
	if got := restarted.Members(); !sameIDs(got, all4) {
		t.Fatalf("restarted n1 Members() = %v, want %v", got, all4)
	}
}

// TestMembershipRecoveredFromSnapshotConf verifies that a node constructed from
// storage holding a snapshot recovers its membership from the snapshot's
// recorded ClusterConfig, overriding the construction-config peer seed.
func TestMembershipRecoveredFromSnapshotConf(t *testing.T) {
	st := raft.NewMemStorage()
	if err := st.SaveSnapshot(raft.Snapshot{
		Index: 5, Term: 2, Data: []byte("state"),
		Conf: raft.ClusterConfig{Voters: []raft.NodeID{"n1", "n2", "n3", "n4"}},
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	nd, err := raft.NewNode(raft.Config{
		ID:                 "n1",
		Peers:              []raft.NodeID{"n1", "n2", "n3"}, // stale seed
		Storage:            st,
		Transport:          &countingTransport{},
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	want := []raft.NodeID{"n1", "n2", "n3", "n4"}
	if got := nd.Members(); !sameIDs(got, want) {
		t.Fatalf("Members() = %v, want %v (snapshot Conf must override the peer seed)", got, want)
	}
}

// TestMembershipRestoredFromInstallSnapshot verifies that a follower installing
// a snapshot via MsgInstallSnapshot restores its membership from the snapshot's
// ClusterConfig.
func TestMembershipRestoredFromInstallSnapshot(t *testing.T) {
	nd, err := raft.NewNode(raft.Config{
		ID:                 "f",
		Peers:              []raft.NodeID{"f", "x", "y"},
		Storage:            raft.NewMemStorage(),
		Transport:          &countingTransport{},
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	conf := raft.ClusterConfig{Voters: []raft.NodeID{"a", "b", "c", "d", "e"}}
	resp := nd.Step(raft.Message{
		Type:     raft.MsgInstallSnapshot,
		From:     "leader",
		To:       "f",
		Term:     7,
		Snapshot: &raft.Snapshot{Index: 30, Term: 6, Data: []byte("snap"), Conf: conf},
	})
	if resp.Type != raft.MsgInstallSnapshotResp {
		t.Fatalf("resp type = %v, want MsgInstallSnapshotResp", resp.Type)
	}
	want := []raft.NodeID{"a", "b", "c", "d", "e"}
	if got := nd.Members(); !sameIDs(got, want) {
		t.Fatalf("Members() after InstallSnapshot = %v, want %v", got, want)
	}
}
