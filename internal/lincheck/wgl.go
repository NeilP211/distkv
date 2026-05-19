package lincheck

import (
	"fmt"
	"sort"
)

// Check reports whether the history h is linearizable.
//
// Operations on distinct keys commute, so h is linearizable iff every per-key
// sub-history is linearizable; Check partitions by key and checks each key's
// register sub-history independently with the Wing-Gong-Liu (WGL) algorithm.
//
// The error return is reserved for malformed input (it is always nil today;
// the signature leaves room for future validation).
func Check(h History) (bool, error) {
	ok, _, err := CheckVerbose(h)
	return ok, err
}

// CheckVerbose is Check plus a human-readable witness describing the first key
// whose sub-history failed to linearize. When ok is true, witness is empty.
func CheckVerbose(h History) (ok bool, witness string, err error) {
	byKey := partitionByKey(h)

	// Check keys in a deterministic order so the reported witness is stable.
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		sub := byKey[k]
		if w, good := linearizableRegister(sub); !good {
			return false, fmt.Sprintf("key %q: sub-history of %d operation(s) is not linearizable; %s",
				k, len(sub), w), nil
		}
	}
	return true, "", nil
}

// partitionByKey groups operations by their Key.
func partitionByKey(h History) map[string]History {
	byKey := make(map[string]History)
	for _, o := range h {
		byKey[o.Key] = append(byKey[o.Key], o)
	}
	return byKey
}

// node is an entry in the doubly-linked list of not-yet-linearized operations
// used by the WGL search.
type node struct {
	op         Operation
	id         int // stable index used for the linearized-set bitset
	prev, next *node
}

// applies returns the model value after op is applied to register value `cur`,
// together with whether op's recorded result is consistent with `cur`.
//
//   - Put / Delete: always succeed; never inconsistent. Delete == Put("").
//   - Get: consistent iff Result == cur; value unchanged.
//   - CAS: success iff cur == Expect. The recorded Ok must equal that; on a
//     reported success the value becomes Value, otherwise it is unchanged.
func applies(op Operation, cur string) (next string, consistent bool) {
	switch op.Kind {
	case OpPut:
		return op.Value, true
	case OpDelete:
		return "", true
	case OpGet:
		return cur, op.Result == cur
	case OpCAS:
		shouldSucceed := cur == op.Expect
		if op.Ok != shouldSucceed {
			return cur, false
		}
		if op.Ok {
			return op.Value, true
		}
		return cur, true
	default:
		return cur, false
	}
}

// linearizableRegister runs the WGL algorithm on a single key's sub-history,
// modelled as a string-valued register. It returns a witness string (only
// meaningful when ok is false) and whether the sub-history linearizes.
func linearizableRegister(sub History) (witness string, ok bool) {
	if len(sub) == 0 {
		return "", true
	}
	if len(sub) > 63 {
		// The visited-set memo key packs the linearized set into a uint64
		// bitset. Per-key concurrency is modest in practice, but guard the
		// bound explicitly rather than silently corrupting the memo.
		return linearizableRegisterLarge(sub)
	}

	// Build the doubly-linked list of operations, ordered by call time so that
	// "minimal" candidates appear early. A sentinel head simplifies splicing.
	ops := append(History(nil), sub...)
	sort.SliceStable(ops, func(i, j int) bool {
		if ops[i].CallTime != ops[j].CallTime {
			return ops[i].CallTime < ops[j].CallTime
		}
		return effReturn(ops[i]) < effReturn(ops[j])
	})

	head := &node{id: -1}
	cur := head
	nodes := make([]*node, len(ops))
	for i := range ops {
		n := &node{op: ops[i], id: i}
		cur.next = n
		n.prev = cur
		cur = n
		nodes[i] = n
	}

	// minReturn is the smallest effective return time among entries currently
	// in the list. An operation may be chosen next only if its CallTime is not
	// strictly greater than minReturn — otherwise some other not-yet-linearized
	// operation must have finished before it started, so it cannot go first.
	//
	// We recompute minReturn lazily inside the search rather than maintaining
	// it incrementally; the list is small per key.

	visited := make(map[memoKey]struct{})

	var search func(value string, linearized uint64, remaining int) bool
	search = func(value string, linearized uint64, remaining int) bool {
		if remaining == 0 {
			return true
		}
		mk := memoKey{linearized: linearized, value: value}
		if _, seen := visited[mk]; seen {
			return false
		}

		// minReturn over the operations still in the list.
		minRet := int64(1<<63 - 1)
		for n := head.next; n != nil; n = n.next {
			if r := effReturn(n.op); r < minRet {
				minRet = r
			}
		}

		for n := head.next; n != nil; n = n.next {
			// Real-time gate: n can be linearized next only if nothing else
			// strictly precedes it (its call is at or before the earliest
			// pending return).
			if n.op.CallTime > minRet {
				continue
			}
			nextVal, consistent := applies(n.op, value)
			if !consistent {
				continue
			}
			// Lift n out of the list.
			n.prev.next = n.next
			if n.next != nil {
				n.next.prev = n.prev
			}
			if search(nextVal, linearized|(1<<uint(n.id)), remaining-1) {
				return true
			}
			// Backtrack: splice n back exactly where it was.
			n.prev.next = n
			if n.next != nil {
				n.next.prev = n
			}
		}

		visited[mk] = struct{}{}
		return false
	}

	if search("", 0, len(ops)) {
		return "", true
	}
	return failureWitness(ops), false
}

// linearizableRegisterLarge handles the rare case of a per-key sub-history
// with more than 63 operations, where the uint64 bitset memo cannot be used.
// It runs the same WGL search without memoization. Real DistKV per-key
// concurrency is modest, so this fallback is acceptable; the search still
// prunes via the real-time gate and consistency checks.
func linearizableRegisterLarge(sub History) (witness string, ok bool) {
	ops := append(History(nil), sub...)
	sort.SliceStable(ops, func(i, j int) bool {
		if ops[i].CallTime != ops[j].CallTime {
			return ops[i].CallTime < ops[j].CallTime
		}
		return effReturn(ops[i]) < effReturn(ops[j])
	})

	head := &node{id: -1}
	cur := head
	for i := range ops {
		n := &node{op: ops[i], id: i}
		cur.next = n
		n.prev = cur
		cur = n
	}

	var search func(value string, remaining int) bool
	search = func(value string, remaining int) bool {
		if remaining == 0 {
			return true
		}
		minRet := int64(1<<63 - 1)
		for n := head.next; n != nil; n = n.next {
			if r := effReturn(n.op); r < minRet {
				minRet = r
			}
		}
		for n := head.next; n != nil; n = n.next {
			if n.op.CallTime > minRet {
				continue
			}
			nextVal, consistent := applies(n.op, value)
			if !consistent {
				continue
			}
			n.prev.next = n.next
			if n.next != nil {
				n.next.prev = n.prev
			}
			if search(nextVal, remaining-1) {
				return true
			}
			n.prev.next = n
			if n.next != nil {
				n.next.prev = n
			}
		}
		return false
	}

	if search("", len(ops)) {
		return "", true
	}
	return failureWitness(ops), false
}

// effReturn is op's effective return time: ReturnTime, or "infinite" when the
// return was never observed (ReturnTime == 0). An infinite return never blocks
// another operation from being chosen as minimal.
func effReturn(op Operation) int64 {
	if op.ReturnTime == 0 {
		return int64(1<<63 - 1)
	}
	return op.ReturnTime
}

// memoKey identifies a search configuration: which operations have been
// linearized (as a bitset) and the resulting register value. Two paths that
// reach the same configuration fail or succeed identically, so a failed
// configuration can be memoized to prune the search.
type memoKey struct {
	linearized uint64
	value      string
}

// failureWitness builds a short human-readable description of a sub-history
// that did not linearize, listing the operations involved.
func failureWitness(ops History) string {
	const maxList = 6
	s := "operations: "
	for i, o := range ops {
		if i >= maxList {
			s += fmt.Sprintf("... (+%d more)", len(ops)-maxList)
			break
		}
		if i > 0 {
			s += ", "
		}
		s += describeOp(o)
	}
	return s
}

// describeOp renders a single operation for a witness.
func describeOp(o Operation) string {
	ret := fmt.Sprintf("%d", o.ReturnTime)
	if o.ReturnTime == 0 {
		ret = "inf"
	}
	switch o.Kind {
	case OpPut:
		return fmt.Sprintf("Put(%q)@[%d,%s]", o.Value, o.CallTime, ret)
	case OpDelete:
		return fmt.Sprintf("Delete@[%d,%s]", o.CallTime, ret)
	case OpGet:
		return fmt.Sprintf("Get->%q@[%d,%s]", o.Result, o.CallTime, ret)
	case OpCAS:
		return fmt.Sprintf("CAS(expect=%q,new=%q)->ok=%t@[%d,%s]",
			o.Expect, o.Value, o.Ok, o.CallTime, ret)
	default:
		return "Op?"
	}
}
