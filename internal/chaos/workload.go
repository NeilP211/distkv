package chaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/NeilP211/distkv/internal/lincheck"
	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/store"
)

const (
	// keySpace is the number of distinct keys clients operate over.  Keeping
	// it small bounds per-key concurrency so the linearizability checker's
	// per-key sub-histories stay tractable.
	keySpace = 20

	// attemptTimeout bounds a single op attempt.  It is deliberately generous:
	// a Propose against a genuine, reachable leader either applies (returns a
	// result) or the node loses leadership (returns ErrNotLeader /
	// ErrLeadershipLost) well within this bound, so a timeout effectively
	// never fires on healthy operation.
	//
	// This matters for correctness, not just speed.  If a Propose is
	// abandoned by a ctx timeout, the entry it appended may still be sitting
	// in a leader's log and commit LATER — an unrecorded write that would
	// corrupt the history.  By making the timeout long enough that the
	// routine transient signal is instead ErrNotLeader (which only ever comes
	// from a node that is NOT the current leader, whose uncommitted tail Raft
	// truncates rather than commits), the harness avoids leaving any
	// late-committing ghost entry behind.  The timeout remains purely as a
	// deadlock backstop.
	attemptTimeout = 4 * time.Second

	// retryBackoff is the pause between failed attempts; it also gives the
	// cluster time to elect a new leader before the client re-resolves one.
	retryBackoff = 25 * time.Millisecond

	// confirmWindow bounds how long doCAS will poll for a unique newVal to
	// appear after a transient CAS error.  It must comfortably exceed any
	// realistic post-deposition replication window: by then a still-leader's
	// committed CAS would have shown up on a linearizable read, and a
	// deposed-leader's uncommitted CAS would have been truncated.
	confirmWindow = 2 * time.Second
	// confirmPoll is the gap between confirm-loop reads.
	confirmPoll = 20 * time.Millisecond

	// thinkTime is a short pause between a client's successive operations.
	// It throttles each client to a sane rate so a multi-second scenario
	// records a few thousand ops (plenty for the checker) rather than
	// hundreds of thousands (which would make the WGL search needlessly
	// slow).  Per-key sub-histories stay small enough to verify fast.
	thinkTime = 2 * time.Millisecond
)

// Workload describes a closed-loop client workload over a Cluster.
type Workload struct {
	c        *Cluster
	clients  int
	duration time.Duration
	rec      *lincheck.Recorder
	seed     int64

	// pendingMu guards pendingCAS, the list of CAS attempts whose outcome
	// confirmCAS could not directly observe.  Such a CAS may still have
	// committed: a concurrent write may have overwritten its newVal before
	// confirmCAS polled, hiding it.  After the workload ends, sweepPendingCAS
	// scans the recorded history — if any recorded Get observed a pending
	// newVal, that CAS provably landed and must be recorded retroactively.
	// Without this sweep a Get observation of a hidden CAS produces a
	// false-positive "non-linearizable" report.
	pendingMu  sync.Mutex
	pendingCAS []pendingCASEntry
}

// pendingCASEntry records the parameters of a CAS whose direct confirmation
// timed out, for the post-workload retroactive sweep.
type pendingCASEntry struct {
	clientID int
	key      string
	expect   string
	newVal   string
	call     int64
	// timeout is the wall-clock instant at which confirmCAS gave up; the
	// retroactive sweep can use it as a lower bound on a guaranteed return
	// time when no observing Get is found earlier.
	timeout int64
}

// RunWorkload launches `clients` closed-loop client goroutines against c for
// `duration`, each issuing a seeded random mix of Put/Get/Delete/CAS over a
// small key space, retrying every operation until it has a definitive result
// before recording it.  After the duration it performs a final linearizable
// Get of every key (also recorded) so any lost acked write surfaces as a
// non-linearizable read.  It returns the recorded History.
//
// All randomness is seeded so a given seed reproduces the same workload.
func RunWorkload(c *Cluster, clients int, duration time.Duration, seed int64) lincheck.History {
	w := &Workload{
		c:        c,
		clients:  clients,
		duration: duration,
		rec:      lincheck.NewRecorder(),
		seed:     seed,
	}
	w.run()
	return w.rec.History()
}

// run drives the client goroutines and the final read sweep.
func (w *Workload) run() {
	deadline := time.Now().Add(w.duration)
	var wg sync.WaitGroup
	for i := 0; i < w.clients; i++ {
		wg.Add(1)
		// Each client gets its own deterministic stream derived from the
		// base seed plus its id.
		go func(clientID int) {
			defer wg.Done()
			w.client(clientID, rand.New(rand.NewSource(w.seed+int64(clientID)*1_000_003)), deadline)
		}(i)
	}
	wg.Wait()

	// Final linearizable read of every key.  A write that was acked but
	// later lost would make one of these reads non-linearizable.
	w.finalSweep()

	// Retroactively record any CAS whose direct confirmation timed out but
	// whose newVal was observed by a recorded Get (so the CAS provably
	// landed and the history needs it to be linearizable).
	w.sweepPendingCAS()
}

// sweepPendingCAS scans the recorded history for Gets observing the newVal of
// any CAS that hit confirmCAS's timeout, and records those CASes — since
// newVal is globally unique, observing it proves the CAS committed.  Without
// this the harness would silently drop a CAS that committed and was then
// overwritten before confirmCAS polled, producing a false linearizability
// violation when a Get earlier in the timeline observed the now-hidden value.
func (w *Workload) sweepPendingCAS() {
	w.pendingMu.Lock()
	pending := w.pendingCAS
	w.pendingCAS = nil
	w.pendingMu.Unlock()
	if len(pending) == 0 {
		return
	}

	hist := w.rec.History()
	// Index Gets by (key, observed-value) to the earliest-call-time Get.
	type gkey struct{ key, val string }
	earliest := make(map[gkey]int64)
	for _, op := range hist {
		if op.Kind != lincheck.OpGet || !op.Ok {
			continue
		}
		k := gkey{op.Key, op.Result}
		if prev, seen := earliest[k]; !seen || op.CallTime < prev {
			earliest[k] = op.CallTime
		}
	}

	for _, p := range pending {
		obsCall, found := earliest[gkey{p.key, p.newVal}]
		if !found {
			// No recorded Get observed newVal: the CAS either never
			// committed or committed but was overwritten before any Get
			// saw it.  Either way the history needs no record of it.
			continue
		}
		// The CAS provably committed at some point in [p.call, obsCall].
		// Record it with ReturnTime = obsCall, which is the tightest upper
		// bound we can justify (it had certainly applied by the time the
		// first observing Get was called).
		ret := obsCall
		if ret <= p.call {
			// Shouldn't happen — obsCall > p.call since the Get observed
			// what only this CAS could have written — but guard anyway.
			ret = p.timeout
			if ret <= p.call {
				ret = p.call + 1
			}
		}
		w.rec.Record(lincheck.Operation{
			ClientID:   p.clientID,
			Kind:       lincheck.OpCAS,
			Key:        p.key,
			Value:      p.newVal,
			Expect:     p.expect,
			Ok:         true,
			CallTime:   p.call,
			ReturnTime: ret,
		})
	}
}

// client runs one closed-loop client until the deadline.
func (w *Workload) client(clientID int, rng *rand.Rand, deadline time.Time) {
	seq := 0
	for time.Now().Before(deadline) {
		key := fmt.Sprintf("k%d", rng.Intn(keySpace))
		switch rng.Intn(4) {
		case 0:
			val := w.uniqueValue(clientID, &seq)
			w.doPut(clientID, key, val)
		case 1:
			w.doGet(clientID, key)
		case 2:
			w.doDelete(clientID, key)
		default:
			val := w.uniqueValue(clientID, &seq)
			w.doCAS(clientID, key, val)
		}
		time.Sleep(thinkTime)
	}
}

// uniqueValue returns a globally-unique value string and advances seq, so
// every Put/CAS-new-value in the history is unambiguous.
func (w *Workload) uniqueValue(clientID int, seq *int) string {
	v := fmt.Sprintf("c%d-%d", clientID, *seq)
	*seq++
	return v
}

// targetNode picks the node a client should aim its next attempt at: the
// best-known leader, falling back to a deterministic node when none is
// known.
func (w *Workload) targetNode() raft.NodeID {
	if l := w.c.leaderHint(); l != "" {
		return l
	}
	return w.c.IDs()[0]
}

// isCASMismatch reports whether err is a definitive CAS rejection by the
// store state machine (as opposed to a transient consensus error).
func isCASMismatch(err error) bool {
	return errors.Is(err, store.ErrCASMismatch)
}

// --- Definitive-result operation helpers. ---
//
// Each helper retries until it obtains a definitive outcome, then records
// exactly one Operation stamped with CallTime before the first attempt and
// ReturnTime after the definitive success.

// doPut retries an idempotent Put until it succeeds, then records it.
func (w *Workload) doPut(clientID int, key, value string) {
	call := time.Now().UnixNano()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		_, err := w.c.proposeOn(ctx, w.targetNode(), store.Command{
			Op: store.OpPut, Key: key, Value: value,
		})
		cancel()
		if err == nil {
			ret := time.Now().UnixNano()
			w.rec.Record(lincheck.Operation{
				ClientID: clientID, Kind: lincheck.OpPut, Key: key, Value: value,
				Ok: true, CallTime: call, ReturnTime: ret,
			})
			return
		}
		time.Sleep(retryBackoff)
	}
}

// doDelete retries an idempotent Delete until it succeeds, then records it.
func (w *Workload) doDelete(clientID int, key string) {
	call := time.Now().UnixNano()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		_, err := w.c.proposeOn(ctx, w.targetNode(), store.Command{
			Op: store.OpDelete, Key: key,
		})
		cancel()
		if err == nil {
			ret := time.Now().UnixNano()
			w.rec.Record(lincheck.Operation{
				ClientID: clientID, Kind: lincheck.OpDelete, Key: key,
				Ok: true, CallTime: call, ReturnTime: ret,
			})
			return
		}
		time.Sleep(retryBackoff)
	}
}

// doGet retries an idempotent linearizable Get until it succeeds, then
// records the value it read.
func (w *Workload) doGet(clientID int, key string) {
	call := time.Now().UnixNano()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		val, _, err := w.c.getOn(ctx, w.targetNode(), key)
		cancel()
		if err == nil {
			ret := time.Now().UnixNano()
			w.rec.Record(lincheck.Operation{
				ClientID: clientID, Kind: lincheck.OpGet, Key: key,
				Result: val, Ok: true, CallTime: call, ReturnTime: ret,
			})
			return
		}
		time.Sleep(retryBackoff)
	}
}

// doCAS performs a compare-and-swap and records a definitive successful
// outcome — or, if no attempt can be confirmed to have landed, records
// nothing at all.
//
// CAS retries are uniquely hazardous.  A CAS Propose that returns a
// transient error (ctx-timeout, "stopped", etc.) leaves an entry that MAY
// still commit later on the same leader.  Re-issuing the same CAS with the
// same newVal would then risk applying it twice — a "ghost" first apply not
// matched by any recorded op.  Worse, a Get observing newVal between the
// two applies would be unexplainable to the checker (the recorded CAS's
// Expect refers to the value just before the second apply, so the checker
// can only linearize it at the second apply point, leaving the first
// observation orphaned).  Put/Delete don't suffer from this because their
// recorded operations carry no Expect — the checker can linearize them
// anywhere — but CAS pins the linearization point.
//
// The safe contract: never re-issue a CAS once an attempt has been
// observably issued.  Concretely:
//
//   - read a fresh baseline expect;
//   - if expect == newVal, an earlier attempt of THIS doCAS already landed
//     (newVal is globally unique) — record success;
//   - issue the CAS; a nil return is a definitive applied success;
//   - an ErrCASMismatch is a definitive applied rejection by the state
//     machine — no value was written, the attempt left no ghost, and we can
//     start over with a fresh expect;
//   - any other (transient) error means the attempt may still land later.
//     Switch to confirm-or-give-up: poll definitiveGet for confirmWindow;
//     if the key ever shows newVal, the ghost landed — record success;
//     if the window expires without observation, treat the CAS as if it
//     never happened (it may have been truncated when its leader was
//     deposed) and do NOT record or re-issue it.  This trades a small loss
//     of recorded ops for a history that is provably ghost-free.
func (w *Workload) doCAS(clientID int, key, newVal string) {
	call := time.Now().UnixNano()
	// Outer loop handles the safe retry cases: fresh baseline reads and
	// ErrCASMismatch from a CAS that definitively did NOT write.  The inner
	// confirm path below leaves the function without re-issuing.
	for {
		expect, ok := w.definitiveGet(key)
		if !ok {
			time.Sleep(retryBackoff)
			continue
		}
		if expect == newVal {
			w.recordCAS(clientID, key, expect, newVal, call)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		_, err := w.c.proposeOn(ctx, w.targetNode(), store.Command{
			Op: store.OpCAS, Key: key, Value: newVal, ExpectValue: expect,
		})
		cancel()
		switch {
		case err == nil:
			// Definitive applied success.
			w.recordCAS(clientID, key, expect, newVal, call)
			return
		case isCASMismatch(err):
			// Definitive applied rejection — no write happened, safe to
			// start over with a fresh baseline.
			continue
		default:
			// Transient error: confirm-or-give-up without re-issuing.
			if w.confirmCAS(clientID, key, expect, newVal, call) {
				return
			}
			// confirmCAS did not directly observe newVal in its window, but
			// the CAS may still have committed and been immediately overwritten
			// by a concurrent write.  Record it as pending so the post-workload
			// sweep can recover the CAS if any Get in the history observed it.
			w.pendingMu.Lock()
			w.pendingCAS = append(w.pendingCAS, pendingCASEntry{
				clientID: clientID,
				key:      key,
				expect:   expect,
				newVal:   newVal,
				call:     call,
				timeout:  time.Now().UnixNano(),
			})
			w.pendingMu.Unlock()
			return
		}
	}
}

// confirmCAS polls the key looking for newVal; if it appears within
// confirmWindow the prior CAS attempt landed (record success and return
// true), otherwise return false without recording — the entry was either
// truncated on a deposed leader or never applied.
func (w *Workload) confirmCAS(clientID int, key, expect, newVal string, call int64) bool {
	deadline := time.Now().Add(confirmWindow)
	for time.Now().Before(deadline) {
		cur, ok := w.definitiveGet(key)
		if ok && cur == newVal {
			w.recordCAS(clientID, key, expect, newVal, call)
			return true
		}
		time.Sleep(confirmPoll)
	}
	return false
}

// recordCAS appends a confirmed successful CAS to the history.
func (w *Workload) recordCAS(clientID int, key, expect, newVal string, call int64) {
	w.rec.Record(lincheck.Operation{
		ClientID: clientID, Kind: lincheck.OpCAS, Key: key,
		Value: newVal, Expect: expect, Ok: true,
		CallTime: call, ReturnTime: time.Now().UnixNano(),
	})
}

// definitiveGet retries a linearizable Get until it succeeds, returning the
// value.  ok is false only if the cluster never answered (it never does so
// for the scenarios here, which always keep a majority reachable), in which
// case it gives up after a bounded number of attempts so a client cannot
// spin forever.
func (w *Workload) definitiveGet(key string) (string, bool) {
	for attempt := 0; attempt < 400; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		val, _, err := w.c.getOn(ctx, w.targetNode(), key)
		cancel()
		if err == nil {
			return val, true
		}
		time.Sleep(retryBackoff)
	}
	return "", false
}

// finalSweep records a linearizable Get of every key after the workload
// ends, exposing any acked-but-lost write as a non-linearizable read.
func (w *Workload) finalSweep() {
	for i := 0; i < keySpace; i++ {
		key := fmt.Sprintf("k%d", i)
		call := time.Now().UnixNano()
		val, ok := w.definitiveGet(key)
		if !ok {
			continue
		}
		ret := time.Now().UnixNano()
		w.rec.Record(lincheck.Operation{
			ClientID: -1, Kind: lincheck.OpGet, Key: key,
			Result: val, Ok: true, CallTime: call, ReturnTime: ret,
		})
	}
}
