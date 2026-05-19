// Package lincheck records key-value operation histories and checks whether
// they are linearizable.
//
// A history is a set of completed client operations, each with a real-time
// interval [CallTime, ReturnTime). The Wing-Gong-Liu (WGL) checker decides
// whether some total order of the operations, consistent with real-time
// precedence, reproduces the results a correct sequential key-value store
// would yield.
//
// Register model. Each key is treated as an independent register holding a
// string value. The absent key is modelled as the empty string "" — Delete is
// therefore equivalent to Put(""), and a Get of an absent key reads "". This
// is sufficient for DistKV's semantics: the store never distinguishes an
// explicitly-stored "" from an absent key in any observable way.
package lincheck

import "sync"

// OpKind is the kind of key-value operation.
type OpKind int

// The supported operation kinds.
const (
	OpPut OpKind = iota
	OpGet
	OpDelete
	OpCAS
)

// String renders an OpKind for witnesses and debugging.
func (k OpKind) String() string {
	switch k {
	case OpPut:
		return "Put"
	case OpGet:
		return "Get"
	case OpDelete:
		return "Delete"
	case OpCAS:
		return "CAS"
	default:
		return "Op?"
	}
}

// Operation is one completed client operation with its real-time interval.
//
// CallTime and ReturnTime are monotonic timestamps (e.g. time.Now().UnixNano());
// for an observed operation CallTime < ReturnTime. An operation whose response
// was never observed (the client timed out or crashed) is represented with
// ReturnTime == 0, meaning "return time unknown / infinite" — the checker
// treats such an operation as able to linearize anywhere at or after CallTime.
type Operation struct {
	ClientID   int
	Kind       OpKind
	Key        string
	Value      string // Put/CAS: the new value
	Expect     string // CAS only: the expected value
	Result     string // Get: the value read back
	Ok         bool   // CAS: whether it reported success
	CallTime   int64
	ReturnTime int64
}

// History is a set of recorded operations. Order within the slice is not
// significant; real-time order is given by CallTime/ReturnTime.
type History []Operation

// Recorder is a thread-safe collector of completed operations, used by the
// chaos suite to build a History from concurrent clients.
type Recorder struct {
	mu  sync.Mutex
	ops History
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{}
}

// Record appends a completed operation under the Recorder's lock. The caller
// is responsible for stamping CallTime and ReturnTime (see Stamp).
func (r *Recorder) Record(op Operation) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// History returns a copy of the operations recorded so far. The copy is safe
// to read and mutate without affecting the Recorder.
func (r *Recorder) History() History {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(History, len(r.ops))
	copy(out, r.ops)
	return out
}

// Len reports how many operations have been recorded.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// Stamp fills in op.CallTime and op.ReturnTime and returns the completed
// Operation. It is a convenience for callers that capture a call timestamp
// before issuing the request and a return timestamp after it completes:
//
//	call := time.Now().UnixNano()
//	val, err := client.Get(key)
//	ret := time.Now().UnixNano()
//	rec.Record(lincheck.Stamp(op, call, ret))
//
// Pass ret == 0 to mark an operation whose return was never observed.
func Stamp(op Operation, call, ret int64) Operation {
	op.CallTime = call
	op.ReturnTime = ret
	return op
}
