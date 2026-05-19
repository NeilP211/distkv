package raft

// HardState is the subset of Raft state that must be persisted to stable
// storage before a node responds to any RPC: the current term and the
// candidate this node voted for in that term.
type HardState struct {
	// CurrentTerm is the latest term the node has seen.
	CurrentTerm uint64
	// VotedFor is the candidate that received this node's vote in the
	// current term; "" means the node has not voted.
	VotedFor NodeID
}

// Storage persists Raft log entries, HardState, and snapshots.  Phase 4 only
// needs an in-memory implementation (MemStorage); Phase 5 adds a bbolt-backed
// one.  All log indices are one-based; index 0 / term 0 is the sentinel that
// represents the position before the log starts.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Storage interface {
	// LoadHardState returns the persisted HardState, or a zero-value
	// HardState if none has ever been saved.
	LoadHardState() (HardState, error)
	// SaveHardState persists hs durably.
	SaveHardState(hs HardState) error
	// AppendEntries appends entries to the log, truncating any existing
	// suffix that conflicts with the supplied entries (an entry conflicts
	// if it has the same index but a different term).
	AppendEntries(entries []LogEntry) error
	// Entries returns the log entries in the half-open range [lo, hi).
	Entries(lo, hi uint64) ([]LogEntry, error)
	// LastIndex returns the index of the last entry in the log, or the
	// snapshot index (0 if no snapshot) when the log is empty.
	LastIndex() (uint64, error)
	// Term returns the term of the entry at index, or 0 for index 0.
	Term(index uint64) (uint64, error)
	// FirstIndex returns 1 + the snapshot index, or 1 if there is no
	// snapshot.  It is the index of the first entry the log can serve.
	FirstIndex() (uint64, error)
	// SaveSnapshot persists snap durably.
	SaveSnapshot(snap Snapshot) error
	// LoadSnapshot returns the persisted snapshot, or a zero-value
	// Snapshot if none has ever been saved.
	LoadSnapshot() (Snapshot, error)
	// Compact discards all log entries with index <= upto.
	Compact(upto uint64) error
}
