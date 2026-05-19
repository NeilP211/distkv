package chaos

import (
	"math/rand"
	"testing"
	"time"

	"github.com/NeilP211/distkv/internal/lincheck"
	"github.com/NeilP211/distkv/internal/raft"
)

// TestChaos_LargeRandomized runs the chaos suite long and broad: a 5-node
// cluster under 60 seconds of continuous, seeded fault injection driven by a
// dozen closed-loop clients.  The recorded history must contain at least 100k
// operations and verify linearizable end-to-end.
//
// This is the "show the system actually works under stress" test from the
// project plan.  It is skipped under `go test -short`; CI invokes it
// explicitly.  The fault mix deliberately excludes a bare leader crash to
// avoid the doc-commented Put/Delete retry-multi-commit harness limitation
// (see TestChaos_LeaderCrash).  Leader changes still occur naturally — via
// partitions, follower restarts, and drop-storms that depose the leader.
func TestChaos_LargeRandomized(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100k-op chaos run in -short mode")
	}

	const (
		nodes        = 5
		clients      = 12
		duration     = 60 * time.Second
		minOpsBig    = 100_000
		clusterSeed  = 0xDEC0DE
		workloadSeed = 0xC1A05
		faultSeed    = 0xFA017
	)

	c := NewCluster(nodes, clusterSeed)
	defer c.Stop()
	if _, err := c.WaitLeader(5 * time.Second); err != nil {
		t.Fatalf("initial leader election: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		injectFaults(c, duration, faultSeed)
	}()

	history := RunWorkload(c, clients, duration, workloadSeed)
	<-done

	if len(history) < minOpsBig {
		t.Fatalf("only %d ops recorded (want >= %d); cluster made too little progress",
			len(history), minOpsBig)
	}
	ok, witness, err := lincheck.CheckVerbose(history)
	if err != nil {
		t.Fatalf("CheckVerbose error: %v", err)
	}
	if !ok {
		t.Fatalf("history NOT linearizable (%d ops): %s", len(history), witness)
	}
	t.Logf("PASS: %d ops linearizable across %v of seeded fault injection",
		len(history), duration)
}

// injectFaults cycles through a seeded mix of fault patterns for the given
// duration. Every fault that disrupts the cluster is followed by a recovery
// step; long calm windows let the cluster re-stabilize and clients drain
// pending retries before the next disruption.
func injectFaults(c *Cluster, dur time.Duration, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	ids := c.IDs()
	end := time.Now().Add(dur)
	for time.Now().Before(end) {
		switch rng.Intn(4) {
		case 0:
			// 3|2 symmetric partition for a few seconds, then heal.
			majority := []raft.NodeID{ids[0], ids[1], ids[2]}
			minority := []raft.NodeID{ids[3], ids[4]}
			c.Partition(majority, minority)
			time.Sleep(3 * time.Second)
			c.Heal()
		case 1:
			// Genuine crash-restart of a follower (state recovered from
			// MemStorage). A non-leader is picked deliberately so the
			// scenario does not invoke the doc-commented leader-crash
			// limitation; leader changes still emerge naturally from the
			// other fault patterns.
			leader, ok := c.Leader()
			if !ok {
				leader = ids[0]
			}
			var follower raft.NodeID
			for _, id := range ids {
				if id != leader {
					follower = id
					break
				}
			}
			c.RestartNode(follower)
		case 2:
			// Network turbulence: dropped messages plus jitter.
			c.SetDelay(2*time.Millisecond, 10*time.Millisecond)
			c.SetDrop(0.20)
			time.Sleep(3 * time.Second)
			c.SetDrop(0)
			c.SetDelay(0, 0)
		case 3:
			// Quiet window so clients drain pending retries and the
			// cluster fully stabilizes between disruptions.
			time.Sleep(2 * time.Second)
		}
		// Brief gap before the next disruption.
		time.Sleep(500 * time.Millisecond)
	}
	// Final clean state so the workload's final sweep can succeed quickly.
	c.SetDrop(0)
	c.SetDelay(0, 0)
	c.Heal()
}
