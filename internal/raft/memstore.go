package raft

import (
	"errors"
	"sync"
)

// ErrCompacted is returned when a caller requests a log index that has
// already been discarded by a snapshot/compaction.
var ErrCompacted = errors.New("raft: requested index is compacted")

// ErrUnavailable is returned when a caller requests a log index beyond the
// end of the log.
var ErrUnavailable = errors.New("raft: requested index is unavailable")

// MemStorage is a fully in-memory, goroutine-safe implementation of Storage.
// It backs every Phase 4 test and is the behavioural reference the Phase 5
// bbolt store is validated against.
//
// The entries slice holds the live log.  When a snapshot is present, the log
// is offset: entries[i] has index snap.Index + 1 + i.
type MemStorage struct {
	mu      sync.RWMutex
	hard    HardState
	entries []LogEntry
	snap    Snapshot
}

// NewMemStorage returns an empty MemStorage.
func NewMemStorage() *MemStorage {
	return &MemStorage{}
}

// LoadHardState implements Storage.
func (m *MemStorage) LoadHardState() (HardState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hard, nil
}

// SaveHardState implements Storage.
func (m *MemStorage) SaveHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hard = hs
	return nil
}

// firstIndex returns 1 + snapshot index.  Caller must hold m.mu.
func (m *MemStorage) firstIndex() uint64 {
	return m.snap.Index + 1
}

// lastIndex returns the index of the last log entry.  Caller must hold m.mu.
func (m *MemStorage) lastIndex() uint64 {
	return m.snap.Index + uint64(len(m.entries))
}

// AppendEntries implements Storage.  Entries before the snapshot index are
// ignored; entries that conflict with the existing log truncate the suffix.
func (m *MemStorage) AppendEntries(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	first := m.firstIndex()
	last := entries[len(entries)-1].Index

	// Drop any leading entries already covered by the snapshot.
	if last < first {
		return nil
	}
	if entries[0].Index < first {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - first // position in m.entries
	switch {
	case uint64(len(m.entries)) > offset:
		// Overlap: keep the non-conflicting prefix, replace the rest.
		keep := m.entries[:offset]
		m.entries = append(keep[:len(keep):len(keep)], entries...)
	case uint64(len(m.entries)) == offset:
		m.entries = append(m.entries, entries...)
	default:
		return errors.New("raft: AppendEntries leaves a gap in the log")
	}
	return nil
}

// Entries implements Storage; returns the half-open range [lo, hi).
func (m *MemStorage) Entries(lo, hi uint64) ([]LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if lo > hi {
		return nil, errors.New("raft: Entries lo > hi")
	}
	first := m.firstIndex()
	if lo < first {
		return nil, ErrCompacted
	}
	if hi > m.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	out := make([]LogEntry, hi-lo)
	copy(out, m.entries[lo-first:hi-first])
	return out, nil
}

// LastIndex implements Storage.
func (m *MemStorage) LastIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastIndex(), nil
}

// FirstIndex implements Storage.
func (m *MemStorage) FirstIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.firstIndex(), nil
}

// Term implements Storage.
func (m *MemStorage) Term(index uint64) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if index == 0 {
		return 0, nil
	}
	if index == m.snap.Index {
		return m.snap.Term, nil
	}
	first := m.firstIndex()
	if index < first {
		return 0, ErrCompacted
	}
	if index > m.lastIndex() {
		return 0, ErrUnavailable
	}
	return m.entries[index-first].Term, nil
}

// SaveSnapshot implements Storage.  Any log entries already covered by the
// snapshot are discarded so the entries slice stays consistent with the new
// firstIndex.
func (m *MemStorage) SaveSnapshot(snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	oldFirst := m.firstIndex()
	m.snap = snap
	// Drop entries the snapshot now subsumes.
	if snap.Index >= oldFirst {
		drop := snap.Index - oldFirst + 1
		if drop >= uint64(len(m.entries)) {
			m.entries = nil
		} else {
			rest := make([]LogEntry, uint64(len(m.entries))-drop)
			copy(rest, m.entries[drop:])
			m.entries = rest
		}
	}
	return nil
}

// LoadSnapshot implements Storage.
func (m *MemStorage) LoadSnapshot() (Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snap, nil
}

// Compact implements Storage; discards entries with index <= upto.
func (m *MemStorage) Compact(upto uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	first := m.firstIndex()
	if upto < first {
		return nil // already compacted
	}
	if upto > m.lastIndex() {
		return ErrUnavailable
	}
	cut := upto - first + 1
	rest := make([]LogEntry, uint64(len(m.entries))-cut)
	copy(rest, m.entries[cut:])
	m.entries = rest
	return nil
}
