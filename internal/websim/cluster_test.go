package websim_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/NeilP211/distkv/internal/store"
	"github.com/NeilP211/distkv/internal/websim"
)

// newTestCluster builds a 5-node cluster at the fastest preset and registers
// cleanup. SpeedFast keeps elections quick (≈0.4–0.7s) without the jitter of
// even shorter ticks.
func newTestCluster(t *testing.T, n int) *websim.Cluster {
	t.Helper()
	c := websim.New(n, 42, websim.SpeedFast, nil)
	t.Cleanup(c.Stop)
	return c
}

// leaderOf returns the id of the single up node that claims the Leader role and
// names itself leader, or "" if there is no such node.
func leaderOf(s websim.ClusterState) string {
	for _, n := range s.Nodes {
		if n.Up && n.Role == "Leader" && n.Leader == n.ID {
			return n.ID
		}
	}
	return ""
}

// leaderInGroup is like leaderOf but restricted to a set of node ids.
func leaderInGroup(s websim.ClusterState, group []string) string {
	set := make(map[string]bool, len(group))
	for _, id := range group {
		set[id] = true
	}
	for _, n := range s.Nodes {
		if set[n.ID] && n.Up && n.Role == "Leader" && n.Leader == n.ID {
			return n.ID
		}
	}
	return ""
}

func waitFor(t *testing.T, timeout time.Duration, cond func() (string, bool), what string) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := cond(); ok {
			return v
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
	return ""
}

func TestLeaderElected(t *testing.T) {
	c := newTestCluster(t, 5)
	id := waitFor(t, 5*time.Second, func() (string, bool) {
		l := leaderOf(c.Snapshot())
		return l, l != ""
	}, "a leader")
	t.Logf("leader = %s", id)
}

func TestLeaderReelectedAfterCrash(t *testing.T) {
	c := newTestCluster(t, 5)
	first := waitFor(t, 5*time.Second, func() (string, bool) {
		l := leaderOf(c.Snapshot())
		return l, l != ""
	}, "initial leader")

	c.CrashNode(first)

	second := waitFor(t, 5*time.Second, func() (string, bool) {
		l := leaderOf(c.Snapshot())
		return l, l != "" && l != first
	}, "a new leader different from the crashed one")
	if second == first {
		t.Fatalf("new leader %s should differ from crashed %s", second, first)
	}
}

func TestMajorityElectsLeaderUnderPartition(t *testing.T) {
	c := newTestCluster(t, 5)
	waitFor(t, 5*time.Second, func() (string, bool) {
		l := leaderOf(c.Snapshot())
		return l, l != ""
	}, "initial leader")

	// Cut {n1,n2} off from the {n3,n4,n5} majority.
	c.Partition([][]string{{"n1", "n2"}, {"n3", "n4", "n5"}})

	majority := []string{"n3", "n4", "n5"}
	waitFor(t, 5*time.Second, func() (string, bool) {
		l := leaderInGroup(c.Snapshot(), majority)
		return l, l != ""
	}, "a leader in the majority partition")
}

func TestSnapshotReflectsCrashAndRecover(t *testing.T) {
	c := newTestCluster(t, 5)
	c.CrashNode("n2")
	s := c.Snapshot()
	for _, n := range s.Nodes {
		if n.ID == "n2" && n.Up {
			t.Fatalf("n2 should be down after CrashNode")
		}
	}
	c.RecoverNode("n2")
	s = c.Snapshot()
	for _, n := range s.Nodes {
		if n.ID == "n2" && !n.Up {
			t.Fatalf("n2 should be up after RecoverNode")
		}
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	c := newTestCluster(t, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Put(ctx, "alpha", "1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found, err := c.Get(ctx, "alpha")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || v != "1" {
		t.Fatalf("Get alpha = (%q,%v), want (\"1\",true)", v, found)
	}
}

func TestGetMissingKey(t *testing.T) {
	c := newTestCluster(t, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	v, found, err := c.Get(ctx, "nope")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found || v != "" {
		t.Fatalf("Get missing = (%q,%v), want (\"\",false)", v, found)
	}
}

func TestCAS(t *testing.T) {
	c := newTestCluster(t, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Put(ctx, "k", "1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := c.CAS(ctx, "k", "1", "2"); err != nil {
		t.Fatalf("CAS matching expect should succeed: %v", err)
	}
	if _, err := c.CAS(ctx, "k", "wrong", "3"); !errors.Is(err, store.ErrCASMismatch) {
		t.Fatalf("CAS mismatch err = %v, want ErrCASMismatch", err)
	}
}

func TestLogGrowsAfterPut(t *testing.T) {
	c := newTestCluster(t, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	leader := waitFor(t, 5*time.Second, func() (string, bool) {
		l := leaderOf(c.Snapshot())
		return l, l != ""
	}, "leader")

	if _, err := c.Put(ctx, "a", "1"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The leader's log must contain the PUT command entry.
	var found bool
	for _, e := range c.NodeLog(leader) {
		if e.Kind == "cmd" && e.Summary == "PUT a=1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("leader %s log missing PUT a=1 entry: %+v", leader, c.NodeLog(leader))
	}

	// Commit index must have advanced past 0 on the leader.
	for _, n := range c.Snapshot().Nodes {
		if n.ID == leader && n.Commit == 0 {
			t.Fatalf("leader commit index did not advance")
		}
	}
}

func TestNoQuorumBlocksWrites(t *testing.T) {
	c := newTestCluster(t, 5)
	waitFor(t, 5*time.Second, func() (string, bool) {
		l := leaderOf(c.Snapshot())
		return l, l != ""
	}, "initial leader")

	// Split every node into its own group: no group has a majority.
	c.Partition([][]string{{"n1"}, {"n2"}, {"n3"}, {"n4"}, {"n5"}})

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if _, err := c.Put(ctx, "x", "1"); err == nil {
		t.Fatalf("Put should fail with no quorum, got nil error")
	}
}
