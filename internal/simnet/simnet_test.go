package simnet_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/simnet"
	"github.com/NeilP211/distkv/internal/transport"
)

const (
	nodeA raft.NodeID = "A"
	nodeB raft.NodeID = "B"
)

// echoHandler returns a handler that echoes the message back with From/To
// swapped and adds 1 to the Term so the caller can verify the handler ran.
func echoHandler(id raft.NodeID) func(raft.Message) raft.Message {
	return func(msg raft.Message) raft.Message {
		return raft.Message{
			Type: msg.Type,
			From: id,
			To:   msg.From,
			Term: msg.Term + 1,
		}
	}
}

// makeNet creates a fresh Network with two registered nodes A and B.
func makeNet(seed int64) (*simnet.Network, transport.Transport, transport.Transport) {
	n := simnet.NewNetwork(seed)
	n.Register(nodeA, echoHandler(nodeA))
	n.Register(nodeB, echoHandler(nodeB))
	tA := n.Node(nodeA)
	tB := n.Node(nodeB)
	return n, tA, tB
}

// TestObserverReportsOutcomes verifies that a registered observer is invoked
// once per send with the correct from/to/type and the right Dropped flag for
// delivered, drop-rate, partitioned, and crashed sends.
func TestObserverReportsOutcomes(t *testing.T) {
	t.Parallel()
	n, tA, _ := makeNet(7)

	var (
		mu     sync.Mutex
		events []simnet.MessageEvent
	)
	n.SetObserver(func(ev simnet.MessageEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})

	req := raft.Message{Type: raft.MsgAppendEntries, From: nodeA, To: nodeB, Term: 1}

	// 1) Delivered.
	if _, err := tA.Send(nodeB, req); err != nil {
		t.Fatalf("delivered send: unexpected error %v", err)
	}
	// 2) Dropped by drop rate.
	n.SetDrop(1.0)
	if _, err := tA.Send(nodeB, req); !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("dropped send: want ErrUnreachable, got %v", err)
	}
	n.SetDrop(0)
	// 3) Partitioned.
	n.Partition([]raft.NodeID{nodeA}, []raft.NodeID{nodeB})
	_, _ = tA.Send(nodeB, req)
	n.Heal()
	// 4) Crashed destination.
	n.Crash(nodeB)
	_, _ = tA.Send(nodeB, req)
	n.Recover(nodeB)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 4 {
		t.Fatalf("observer events = %d, want 4: %+v", len(events), events)
	}
	wantDropped := []bool{false, true, true, true}
	for i, ev := range events {
		if ev.From != nodeA || ev.To != nodeB || ev.Type != raft.MsgAppendEntries {
			t.Errorf("event %d = %+v, want From=A To=B Type=MsgAppendEntries", i, ev)
		}
		if ev.Dropped != wantDropped[i] {
			t.Errorf("event %d Dropped = %v, want %v", i, ev.Dropped, wantDropped[i])
		}
	}
}

// TestObserverNilSafe verifies that sends work when no observer is set.
func TestObserverNilSafe(t *testing.T) {
	t.Parallel()
	_, tA, _ := makeNet(8)
	if _, err := tA.Send(nodeB, raft.Message{Type: raft.MsgRequestVote, From: nodeA, To: nodeB}); err != nil {
		t.Fatalf("send with no observer: %v", err)
	}
}

// TestBasicDelivery checks that two registered nodes can exchange messages.
func TestBasicDelivery(t *testing.T) {
	t.Parallel()
	_, tA, _ := makeNet(1)

	req := raft.Message{Type: raft.MsgRequestVote, From: nodeA, To: nodeB, Term: 3}
	resp, err := tA.Send(nodeB, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Term != 4 {
		t.Errorf("expected echoed Term=4, got %d", resp.Term)
	}
	if resp.From != nodeB {
		t.Errorf("expected From=B, got %s", resp.From)
	}
}

// TestPartitionBlocksDelivery checks that after partitioning nodes into
// singleton groups, Send returns ErrUnreachable.
func TestPartitionBlocksDelivery(t *testing.T) {
	t.Parallel()
	n, tA, tB := makeNet(2)

	// Put each node in its own group.
	n.Partition([]raft.NodeID{nodeA}, []raft.NodeID{nodeB})

	req := raft.Message{Type: raft.MsgRequestVote, From: nodeA, To: nodeB, Term: 1}
	if _, err := tA.Send(nodeB, req); !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("expected ErrUnreachable after partition (A->B), got: %v", err)
	}

	// A partition blocks both directions.
	rev := raft.Message{Type: raft.MsgRequestVote, From: nodeB, To: nodeA, Term: 1}
	if _, err := tB.Send(nodeA, rev); !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("expected ErrUnreachable after partition (B->A), got: %v", err)
	}
}

// TestHealRestoresDelivery checks that after healing a partition, Send
// succeeds again.
func TestHealRestoresDelivery(t *testing.T) {
	t.Parallel()
	n, tA, _ := makeNet(3)

	n.Partition([]raft.NodeID{nodeA}, []raft.NodeID{nodeB})
	n.Heal()

	req := raft.Message{Type: raft.MsgRequestVote, From: nodeA, To: nodeB, Term: 5}
	resp, err := tA.Send(nodeB, req)
	if err != nil {
		t.Fatalf("expected delivery after heal, got: %v", err)
	}
	if resp.Term != 6 {
		t.Errorf("expected echoed Term=6, got %d", resp.Term)
	}
}

// TestSetDropFullRate checks that SetDrop(1.0) causes every Send to fail.
func TestSetDropFullRate(t *testing.T) {
	t.Parallel()
	n, tA, _ := makeNet(4)

	n.SetDrop(1.0)

	req := raft.Message{Type: raft.MsgAppendEntries, From: nodeA, To: nodeB, Term: 1}
	for i := 0; i < 10; i++ {
		_, err := tA.Send(nodeB, req)
		if !errors.Is(err, transport.ErrUnreachable) {
			t.Fatalf("iteration %d: expected ErrUnreachable with drop=1.0, got: %v", i, err)
		}
	}
}

// TestSetDropZeroRate checks that SetDrop(0.0) causes no drops.
func TestSetDropZeroRate(t *testing.T) {
	t.Parallel()
	n, tA, _ := makeNet(5)

	n.SetDrop(0.0)

	req := raft.Message{Type: raft.MsgAppendEntries, From: nodeA, To: nodeB, Term: 1}
	for i := 0; i < 5; i++ {
		_, err := tA.Send(nodeB, req)
		if err != nil {
			t.Fatalf("iteration %d: unexpected error with drop=0.0: %v", i, err)
		}
	}
}

// TestCrashAndRecover checks that a crashed node is unreachable and becomes
// reachable again after Recover.
func TestCrashAndRecover(t *testing.T) {
	t.Parallel()
	n, tA, tB := makeNet(6)

	n.Crash(nodeB)
	req := raft.Message{Type: raft.MsgAppendEntries, From: nodeA, To: nodeB, Term: 1}
	_, err := tA.Send(nodeB, req)
	if !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("expected ErrUnreachable receiving at crashed node, got: %v", err)
	}

	// A crashed node also cannot originate sends.
	rev := raft.Message{Type: raft.MsgAppendEntries, From: nodeB, To: nodeA, Term: 1}
	if _, err := tB.Send(nodeA, rev); !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("expected ErrUnreachable sending from crashed node, got: %v", err)
	}

	n.Recover(nodeB)
	resp, err := tA.Send(nodeB, req)
	if err != nil {
		t.Fatalf("expected delivery after recover, got: %v", err)
	}
	if resp.Term != 2 {
		t.Errorf("expected echoed Term=2 after recover, got %d", resp.Term)
	}
}

// TestIsolateAndRecover is similar to TestCrashAndRecover but uses Isolate.
func TestIsolateAndRecover(t *testing.T) {
	t.Parallel()
	n, tA, _ := makeNet(7)

	n.Isolate(nodeB)
	req := raft.Message{Type: raft.MsgAppendEntries, From: nodeA, To: nodeB, Term: 2}
	_, err := tA.Send(nodeB, req)
	if !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("expected ErrUnreachable for isolated node, got: %v", err)
	}

	n.Recover(nodeB)
	_, err = tA.Send(nodeB, req)
	if err != nil {
		t.Fatalf("expected delivery after recover, got: %v", err)
	}
}

// TestDelay checks that SetDelay does not prevent delivery (just slows it).
func TestDelay(t *testing.T) {
	t.Parallel()
	n, tA, _ := makeNet(8)

	n.SetDelay(1*time.Millisecond, 5*time.Millisecond)

	req := raft.Message{Type: raft.MsgAppendEntries, From: nodeA, To: nodeB, Term: 1}
	_, err := tA.Send(nodeB, req)
	if err != nil {
		t.Fatalf("unexpected error with delay set: %v", err)
	}
}

// TestDeterminism checks that two Networks with the same seed produce the
// same sequence of drop decisions when SetDrop(0.5) is set.
func TestDeterminism(t *testing.T) {
	t.Parallel()
	const seed = 42

	run := func() []bool {
		n := simnet.NewNetwork(seed)
		n.Register(nodeA, echoHandler(nodeA))
		n.Register(nodeB, echoHandler(nodeB))
		n.SetDrop(0.5)

		tA := n.Node(nodeA)
		req := raft.Message{Type: raft.MsgAppendEntries, From: nodeA, To: nodeB, Term: 1}
		var results []bool
		for i := 0; i < 20; i++ {
			_, err := tA.Send(nodeB, req)
			results = append(results, err == nil)
		}
		return results
	}

	r1 := run()
	r2 := run()

	for i := range r1 {
		if r1[i] != r2[i] {
			t.Errorf("non-deterministic at index %d: run1=%v run2=%v", i, r1[i], r2[i])
		}
	}
}
