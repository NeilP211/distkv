package raft

import (
	"reflect"
	"testing"
)

func newTestLog(t *testing.T, entries ...LogEntry) *raftLog {
	t.Helper()
	s := NewMemStorage()
	if len(entries) > 0 {
		if err := s.AppendEntries(entries); err != nil {
			t.Fatalf("seed AppendEntries: %v", err)
		}
	}
	l, err := newRaftLog(s)
	if err != nil {
		t.Fatalf("newRaftLog: %v", err)
	}
	return l
}

func TestRaftLogLastIndexTerm(t *testing.T) {
	l := newTestLog(t)
	if l.lastIndex() != 0 || l.lastTerm() != 0 {
		t.Fatalf("empty log lastIndex/lastTerm = %d/%d", l.lastIndex(), l.lastTerm())
	}
	l = newTestLog(t, ent(1, 1), ent(2, 2), ent(2, 3))
	if l.lastIndex() != 3 {
		t.Fatalf("lastIndex = %d, want 3", l.lastIndex())
	}
	if l.lastTerm() != 2 {
		t.Fatalf("lastTerm = %d, want 2", l.lastTerm())
	}
}

func TestRaftLogIsUpToDate(t *testing.T) {
	// Local log: term 2, index 3.
	l := newTestLog(t, ent(1, 1), ent(2, 2), ent(2, 3))
	cases := []struct {
		name     string
		idx, trm uint64
		want     bool
	}{
		{"higher term wins", 1, 3, true},
		{"lower term loses", 5, 1, false},
		{"equal term longer log wins", 4, 2, true},
		{"equal term same length is up to date", 3, 2, true},
		{"equal term shorter log loses", 2, 2, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := l.isUpToDate(c.idx, c.trm); got != c.want {
				t.Fatalf("isUpToDate(%d,%d) = %v, want %v", c.idx, c.trm, got, c.want)
			}
		})
	}
}

func TestRaftLogFindConflict(t *testing.T) {
	l := newTestLog(t, ent(1, 1), ent(2, 2), ent(2, 3))

	// No conflict: identical entries.
	if got := l.findConflict([]LogEntry{ent(1, 1), ent(2, 2)}); got != 0 {
		t.Fatalf("findConflict identical = %d, want 0", got)
	}
	// Conflict in the middle: index 2 has term 3 vs existing term 2.
	if got := l.findConflict([]LogEntry{ent(1, 1), ent(3, 2), ent(3, 3)}); got != 2 {
		t.Fatalf("findConflict middle = %d, want 2", got)
	}
	// All new: entries beyond the end of the log.
	if got := l.findConflict([]LogEntry{ent(2, 4), ent(2, 5)}); got != 4 {
		t.Fatalf("findConflict all new = %d, want 4", got)
	}
	// Partial overlap then new: first new entry is index 4.
	if got := l.findConflict([]LogEntry{ent(2, 3), ent(2, 4)}); got != 4 {
		t.Fatalf("findConflict overlap-then-new = %d, want 4", got)
	}
}

func TestRaftLogAppendTruncates(t *testing.T) {
	l := newTestLog(t, ent(1, 1), ent(1, 2), ent(1, 3), ent(1, 4))
	l.append([]LogEntry{ent(2, 3), ent(2, 4), ent(2, 5)})
	if l.lastIndex() != 5 {
		t.Fatalf("lastIndex after truncating append = %d, want 5", l.lastIndex())
	}
	got, err := l.slice(1, 6)
	if err != nil {
		t.Fatalf("slice: %v", err)
	}
	want := []LogEntry{ent(1, 1), ent(1, 2), ent(2, 3), ent(2, 4), ent(2, 5)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("log = %+v, want %+v", got, want)
	}
}

func TestRaftLogCommitTo(t *testing.T) {
	l := newTestLog(t, ent(1, 1), ent(1, 2), ent(1, 3))
	l.commitTo(2)
	if l.commitIndex != 2 {
		t.Fatalf("commitIndex = %d, want 2", l.commitIndex)
	}
	// commitTo never moves backwards.
	l.commitTo(1)
	if l.commitIndex != 2 {
		t.Fatalf("commitIndex after backward commitTo = %d, want 2", l.commitIndex)
	}
}

func TestRaftLogTerm(t *testing.T) {
	l := newTestLog(t, ent(1, 1), ent(3, 2))
	if tm, err := l.term(0); err != nil || tm != 0 {
		t.Fatalf("term(0) = %d, %v", tm, err)
	}
	if tm, err := l.term(2); err != nil || tm != 3 {
		t.Fatalf("term(2) = %d, %v, want 3", tm, err)
	}
}
