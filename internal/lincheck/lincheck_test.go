package lincheck

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// op is a small helper to build an observed Operation with an explicit
// [call, ret) interval.
func op(kind OpKind, key, value, expect, result string, ok bool, call, ret int64) Operation {
	return Operation{
		Kind:       kind,
		Key:        key,
		Value:      value,
		Expect:     expect,
		Result:     result,
		Ok:         ok,
		CallTime:   call,
		ReturnTime: ret,
	}
}

func mustCheck(t *testing.T, h History, want bool) {
	t.Helper()
	got, err := Check(h)
	if err != nil {
		t.Fatalf("Check error: %v", err)
	}
	if got != want {
		ok, witness, _ := CheckVerbose(h)
		t.Fatalf("Check = %v, want %v (verbose ok=%v witness=%q)", got, want, ok, witness)
	}
}

// TestSequentialLinearizable: a non-overlapping single-client history is
// always linearizable.
func TestSequentialLinearizable(t *testing.T) {
	h := History{
		op(OpPut, "k", "1", "", "", false, 10, 20),
		op(OpGet, "k", "", "", "1", false, 30, 40),
		op(OpPut, "k", "2", "", "", false, 50, 60),
		op(OpGet, "k", "", "", "2", false, 70, 80),
		op(OpDelete, "k", "", "", "", false, 90, 100),
		op(OpGet, "k", "", "", "", false, 110, 120),
	}
	mustCheck(t, h, true)
}

// TestConcurrentLinearizable: overlapping operations whose results admit a
// valid order are linearizable.
func TestConcurrentLinearizable(t *testing.T) {
	// Put("a") and Put("b") overlap; Get reads "b" — order Put(a) < Put(b) < Get.
	h := History{
		op(OpPut, "k", "a", "", "", false, 10, 50),
		op(OpPut, "k", "b", "", "", false, 20, 60),
		op(OpGet, "k", "", "", "b", false, 70, 80),
	}
	mustCheck(t, h, true)

	// Get overlaps both Puts and may read either value: choosing "a" works.
	h2 := History{
		op(OpPut, "k", "a", "", "", false, 10, 50),
		op(OpPut, "k", "b", "", "", false, 60, 100),
		op(OpGet, "k", "", "", "a", false, 40, 70),
	}
	mustCheck(t, h2, true)
}

// TestStaleRead: Put fully precedes a Get that reads an older value.
func TestStaleRead(t *testing.T) {
	h := History{
		op(OpPut, "k", "1", "", "", false, 10, 20),
		op(OpGet, "k", "", "", "0", false, 30, 40), // returns before? no: stale
	}
	mustCheck(t, h, false)
}

// TestTwoGetsDisagree: two Gets, both fully after a single Put, disagree.
func TestTwoGetsDisagree(t *testing.T) {
	h := History{
		op(OpPut, "k", "x", "", "", false, 10, 20),
		op(OpGet, "k", "", "", "x", false, 30, 40),
		op(OpGet, "k", "", "", "y", false, 50, 60),
	}
	mustCheck(t, h, false)
}

// TestDistinctKeysIndependent: a violation on one key fails even if other keys
// are fine; distinct keys are checked independently.
func TestDistinctKeysIndependent(t *testing.T) {
	good := History{
		op(OpPut, "k1", "1", "", "", false, 10, 20),
		op(OpGet, "k1", "", "", "1", false, 30, 40),
	}
	mustCheck(t, good, true)

	bad := History{
		op(OpPut, "k1", "1", "", "", false, 10, 20),
		op(OpGet, "k1", "", "", "1", false, 30, 40),
		op(OpPut, "k2", "1", "", "", false, 10, 20),
		op(OpGet, "k2", "", "", "0", false, 30, 40), // stale on k2
	}
	mustCheck(t, bad, false)
}

// TestCASLinearizable: a CAS history with success/failure consistent with an
// order.
func TestCASLinearizable(t *testing.T) {
	h := History{
		op(OpPut, "k", "0", "", "", false, 10, 20),
		op(OpCAS, "k", "1", "0", "", true, 30, 40),  // 0 -> 1, succeeds
		op(OpCAS, "k", "2", "0", "", false, 50, 60), // expects 0, current is 1: fails
		op(OpGet, "k", "", "", "1", false, 70, 80),
		op(OpCAS, "k", "2", "1", "", true, 90, 100), // 1 -> 2, succeeds
		op(OpGet, "k", "", "", "2", false, 110, 120),
	}
	mustCheck(t, h, true)

	// Concurrent CAS: two CAS both expect "0"; exactly one can succeed.
	hc := History{
		op(OpPut, "k", "0", "", "", false, 10, 20),
		op(OpCAS, "k", "a", "0", "", true, 30, 90),
		op(OpCAS, "k", "b", "0", "", false, 40, 100),
		op(OpGet, "k", "", "", "a", false, 110, 120),
	}
	mustCheck(t, hc, true)
}

// TestCASNonLinearizable: a CAS reports success but no order makes its expected
// value current.
func TestCASNonLinearizable(t *testing.T) {
	h := History{
		op(OpPut, "k", "0", "", "", false, 10, 20),
		op(OpCAS, "k", "9", "5", "", true, 30, 40), // expects "5", but value is "0"
	}
	mustCheck(t, h, false)

	// Both concurrent CAS expecting "0" report success — impossible.
	hc := History{
		op(OpPut, "k", "0", "", "", false, 10, 20),
		op(OpCAS, "k", "a", "0", "", true, 30, 90),
		op(OpCAS, "k", "b", "0", "", true, 40, 100),
	}
	mustCheck(t, hc, false)
}

// TestUnobservedReturnPlaceable: an operation with ReturnTime == 0 can be
// placed to make the history linearizable.
func TestUnobservedReturnPlaceable(t *testing.T) {
	// Put("9") never observed its return. Get reads "9", so the Put must have
	// linearized before the Get even though its return time is unknown.
	h := History{
		op(OpPut, "k", "9", "", "", false, 10, 0), // ReturnTime unknown
		op(OpGet, "k", "", "", "9", false, 20, 30),
	}
	mustCheck(t, h, true)

	// An unobserved Put that conflicts cannot break a history: it can be placed
	// last (after everything) — here it does not need to affect earlier Gets.
	h2 := History{
		op(OpPut, "k", "1", "", "", false, 10, 20),
		op(OpGet, "k", "", "", "1", false, 30, 40),
		op(OpPut, "k", "7", "", "", false, 35, 0), // place after the Get
	}
	mustCheck(t, h2, true)
}

// TestUnobservedReturnStillFails: an unobserved return does not magically fix a
// genuine violation.
func TestUnobservedReturnStillFails(t *testing.T) {
	// Get reads "5" but nothing ever wrote "5"; Put has unknown return but
	// writes "1". No placement yields "5".
	h := History{
		op(OpPut, "k", "1", "", "", false, 10, 0),
		op(OpGet, "k", "", "", "5", false, 20, 30),
	}
	mustCheck(t, h, false)
}

// TestEmptyHistory: an empty history is trivially linearizable.
func TestEmptyHistory(t *testing.T) {
	mustCheck(t, History{}, true)
}

// TestVerboseWitness: CheckVerbose names the offending key.
func TestVerboseWitness(t *testing.T) {
	h := History{
		op(OpPut, "kbad", "1", "", "", false, 10, 20),
		op(OpGet, "kbad", "", "", "0", false, 30, 40),
	}
	ok, witness, err := CheckVerbose(h)
	if err != nil {
		t.Fatalf("CheckVerbose error: %v", err)
	}
	if ok {
		t.Fatalf("CheckVerbose ok = true, want false")
	}
	if witness == "" {
		t.Fatalf("expected a non-empty witness")
	}
	t.Logf("witness: %s", witness)
}

// genSeqHistory builds a random non-overlapping single-client history over a
// few keys. Because operations never overlap, it is always linearizable.
func genSeqHistory(rng *rand.Rand, n int) History {
	keys := []string{"x", "y", "z"}
	state := map[string]string{}
	var h History
	var t int64 = 1
	for i := 0; i < n; i++ {
		k := keys[rng.Intn(len(keys))]
		call := t
		ret := t + 1
		t += 2
		switch rng.Intn(4) {
		case 0:
			v := fmt.Sprintf("v%d", rng.Intn(5))
			state[k] = v
			h = append(h, op(OpPut, k, v, "", "", false, call, ret))
		case 1:
			h = append(h, op(OpGet, k, "", "", state[k], false, call, ret))
		case 2:
			state[k] = ""
			h = append(h, op(OpDelete, k, "", "", "", false, call, ret))
		case 3:
			expect := fmt.Sprintf("v%d", rng.Intn(5))
			v := fmt.Sprintf("v%d", rng.Intn(5))
			ok := state[k] == expect
			if ok {
				state[k] = v
			}
			h = append(h, op(OpCAS, k, v, expect, "", ok, call, ret))
		}
	}
	return h
}

// TestRandomSequentialLinearizable: random sequential histories are always
// linearizable; perturbing one result makes them non-linearizable.
func TestRandomSequentialLinearizable(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		seed := rapid.Int64().Draw(rt, "seed")
		n := rapid.IntRange(1, 60).Draw(rt, "n")
		rng := rand.New(rand.NewSource(seed))
		h := genSeqHistory(rng, n)

		ok, err := Check(h)
		if err != nil {
			rt.Fatalf("Check error: %v", err)
		}
		if !ok {
			rt.Fatalf("sequential history not linearizable: %#v", h)
		}

		// Perturb one Get or CAS into an impossible result and expect failure.
		perturbed := perturb(rng, h)
		if perturbed != nil {
			bad, err := Check(perturbed)
			if err != nil {
				rt.Fatalf("Check error on perturbed: %v", err)
			}
			if bad {
				rt.Fatalf("perturbed history unexpectedly linearizable: %#v", perturbed)
			}
		}
	})
}

// perturb returns a copy of h with one Get's result changed to an impossible
// value, or one successful CAS turned into an impossible success. Returns nil
// if no suitable operation exists.
func perturb(rng *rand.Rand, h History) History {
	// Find candidate indices: Gets, plus successful CAS.
	var idxs []int
	for i, o := range h {
		if o.Kind == OpGet || (o.Kind == OpCAS && o.Ok) {
			idxs = append(idxs, i)
		}
	}
	if len(idxs) == 0 {
		return nil
	}
	cp := make(History, len(h))
	copy(cp, h)
	i := idxs[rng.Intn(len(idxs))]
	switch cp[i].Kind {
	case OpGet:
		cp[i].Result = "IMPOSSIBLE_SENTINEL"
	case OpCAS:
		// Make it expect a value that can never be current.
		cp[i].Expect = "IMPOSSIBLE_SENTINEL"
	}
	return cp
}

// TestPerformance: a few thousand operations across many keys checks quickly.
func TestPerformance(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const nKeys = 200
	const opsPerKey = 30
	var h History
	var clk int64 = 1
	for k := 0; k < nKeys; k++ {
		key := fmt.Sprintf("key-%d", k)
		state := ""
		// A few concurrent clients per key to exercise the search a little.
		for i := 0; i < opsPerKey; i++ {
			call := clk
			ret := clk + 3 // slight overlap between adjacent ops
			clk += 2
			if rng.Intn(2) == 0 {
				v := fmt.Sprintf("v%d", rng.Intn(8))
				state = v
				h = append(h, op(OpPut, key, v, "", "", false, call, ret))
			} else {
				h = append(h, op(OpGet, key, "", "", state, false, call, ret))
			}
		}
	}
	start := time.Now()
	ok, err := Check(h)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Check error: %v", err)
	}
	if !ok {
		t.Fatalf("performance history should be linearizable")
	}
	if elapsed > time.Second {
		t.Fatalf("check too slow: %v for %d ops", elapsed, len(h))
	}
	t.Logf("checked %d ops across %d keys in %v", len(h), nKeys, elapsed)
}

// TestRecorder exercises the thread-safe Recorder API.
func TestRecorder(t *testing.T) {
	r := NewRecorder()
	done := make(chan struct{})
	for c := 0; c < 8; c++ {
		go func(cid int) {
			for i := 0; i < 50; i++ {
				call := int64(cid*1000 + i*2)
				o := Stamp(Operation{ClientID: cid, Kind: OpPut, Key: "k", Value: "v"}, call, call+1)
				r.Record(o)
			}
			done <- struct{}{}
		}(c)
	}
	for c := 0; c < 8; c++ {
		<-done
	}
	if got := r.Len(); got != 8*50 {
		t.Fatalf("Recorder.Len = %d, want %d", got, 8*50)
	}
	h := r.History()
	if len(h) != 8*50 {
		t.Fatalf("History len = %d, want %d", len(h), 8*50)
	}
	// History returns a copy: mutating it must not affect the Recorder.
	h[0].Key = "mutated"
	if r.History()[0].Key == "mutated" {
		t.Fatalf("History did not return an independent copy")
	}
}
