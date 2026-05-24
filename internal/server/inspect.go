package server

import "github.com/NeilP211/distkv/internal/raft"

// These read-only accessors expose the underlying Raft log to observability
// consumers (the web showcase) without widening the Status snapshot. They
// delegate straight to the concurrency-safe raft.Node accessors.

// LastIndex returns the index of the last entry in this node's Raft log
// (0 when the log is empty).
func (rn *RaftNode) LastIndex() uint64 {
	return rn.node.LastIndex()
}

// FirstIndex returns the index of the first entry retained in the log, i.e.
// snapshotIndex+1.
func (rn *RaftNode) FirstIndex() uint64 {
	return rn.node.FirstIndex()
}

// LogEntries returns the log entries in the half-open range [lo, hi). An
// out-of-range request yields a nil slice rather than an error.
func (rn *RaftNode) LogEntries(lo, hi uint64) []raft.LogEntry {
	return rn.node.LogEntries(lo, hi)
}
