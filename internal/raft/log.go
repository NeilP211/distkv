package raft

// raftLog is the in-memory view of the Raft replicated log layered over a
// Storage.  It tracks the commit index and provides the log-comparison
// helpers used by election (§5.4.1) and replication (§5.3).
//
// raftLog itself is not goroutine-safe; the owning Node serialises all access
// behind its own mutex.
type raftLog struct {
	storage Storage

	// commitIndex is the highest log index known to be committed.
	commitIndex uint64
}

// newRaftLog builds a raftLog over storage.  The commit index starts at the
// snapshot index, since everything a snapshot covers is committed by
// definition.
func newRaftLog(storage Storage) (*raftLog, error) {
	snap, err := storage.LoadSnapshot()
	if err != nil {
		return nil, err
	}
	return &raftLog{storage: storage, commitIndex: snap.Index}, nil
}

// lastIndex returns the index of the last entry in the log.
func (l *raftLog) lastIndex() uint64 {
	li, err := l.storage.LastIndex()
	if err != nil {
		panic("raft: storage.LastIndex failed: " + err.Error())
	}
	return li
}

// lastTerm returns the term of the last entry in the log.
func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		panic("raft: lastTerm failed: " + err.Error())
	}
	return t
}

// term returns the term of the entry at index i (0 for the index-0 sentinel).
func (l *raftLog) term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}
	return l.storage.Term(i)
}

// append assigns indices to entries (sequential from the current last index)
// and writes them to storage, truncating any conflicting suffix.  Entries that
// already carry indices are written as-is; entries with a zero index are
// numbered sequentially after the current last index.
func (l *raftLog) append(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}
	next := l.lastIndex() + 1
	numbered := make([]LogEntry, len(entries))
	for i, e := range entries {
		if e.Index == 0 {
			e.Index = next + uint64(i)
		}
		numbered[i] = e
	}
	if err := l.storage.AppendEntries(numbered); err != nil {
		panic("raft: storage.AppendEntries failed: " + err.Error())
	}
}

// slice returns the log entries in the half-open range [lo, hi).
func (l *raftLog) slice(lo, hi uint64) ([]LogEntry, error) {
	return l.storage.Entries(lo, hi)
}

// isUpToDate implements the §5.4.1 election restriction: it reports whether a
// candidate whose last entry is (lastTerm, lastIdx) has a log at least as
// up-to-date as this one.  The log with the later final term is more
// up-to-date; if the terms are equal, the longer log is more up-to-date.
func (l *raftLog) isUpToDate(lastIdx, lastTerm uint64) bool {
	myTerm := l.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= l.lastIndex()
}

// findConflict scans entries against the existing log and returns the index
// of the first entry that conflicts — an entry with the same index but a
// different term, or the first entry beyond the end of the log.  It returns 0
// when every supplied entry already matches the log.
func (l *raftLog) findConflict(entries []LogEntry) uint64 {
	last := l.lastIndex()
	for _, e := range entries {
		t, err := l.term(e.Index)
		if e.Index > last || err != nil || t != e.Term {
			return e.Index
		}
	}
	return 0
}

// commitTo advances the commit index to idx.  It never moves the commit index
// backwards.
func (l *raftLog) commitTo(idx uint64) {
	if idx > l.commitIndex {
		l.commitIndex = idx
	}
}
