package raft

import (
	"reflect"
	"testing"
)

func ent(term, index uint64) LogEntry {
	return LogEntry{Term: term, Index: index, Type: EntryNormal}
}

func TestMemStorageHardStateRoundTrip(t *testing.T) {
	s := NewMemStorage()
	hs, err := s.LoadHardState()
	if err != nil {
		t.Fatalf("LoadHardState: %v", err)
	}
	if hs != (HardState{}) {
		t.Fatalf("fresh storage hardstate = %+v, want zero", hs)
	}
	want := HardState{CurrentTerm: 7, VotedFor: "n2"}
	if err := s.SaveHardState(want); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	got, err := s.LoadHardState()
	if err != nil {
		t.Fatalf("LoadHardState: %v", err)
	}
	if got != want {
		t.Fatalf("hardstate = %+v, want %+v", got, want)
	}
}

func TestMemStorageAppendAndRead(t *testing.T) {
	s := NewMemStorage()
	in := []LogEntry{ent(1, 1), ent(1, 2), ent(2, 3)}
	if err := s.AppendEntries(in); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	got, err := s.Entries(1, 4)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("Entries = %+v, want %+v", got, in)
	}
	// Partial range.
	got, err = s.Entries(2, 3)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if !reflect.DeepEqual(got, []LogEntry{ent(1, 2)}) {
		t.Fatalf("Entries(2,3) = %+v", got)
	}
}

func TestMemStorageConflictingAppendTruncates(t *testing.T) {
	s := NewMemStorage()
	if err := s.AppendEntries([]LogEntry{ent(1, 1), ent(1, 2), ent(1, 3), ent(1, 4)}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	// New entries diverge at index 3 (different term).
	if err := s.AppendEntries([]LogEntry{ent(2, 3), ent(2, 4), ent(2, 5)}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	li, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if li != 5 {
		t.Fatalf("LastIndex = %d, want 5", li)
	}
	got, err := s.Entries(1, 6)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	want := []LogEntry{ent(1, 1), ent(1, 2), ent(2, 3), ent(2, 4), ent(2, 5)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries = %+v, want %+v", got, want)
	}
}

func TestMemStorageIdenticalAppendNoTruncate(t *testing.T) {
	s := NewMemStorage()
	if err := s.AppendEntries([]LogEntry{ent(1, 1), ent(1, 2), ent(1, 3)}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	// Re-append entries that match existing ones plus one new.
	if err := s.AppendEntries([]LogEntry{ent(1, 2), ent(1, 3), ent(1, 4)}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	li, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if li != 4 {
		t.Fatalf("LastIndex = %d, want 4", li)
	}
}

func TestMemStorageTermAndIndices(t *testing.T) {
	s := NewMemStorage()
	li, err := s.LastIndex()
	if err != nil || li != 0 {
		t.Fatalf("empty LastIndex = %d, %v", li, err)
	}
	fi, err := s.FirstIndex()
	if err != nil || fi != 1 {
		t.Fatalf("empty FirstIndex = %d, %v", fi, err)
	}
	tm, err := s.Term(0)
	if err != nil || tm != 0 {
		t.Fatalf("Term(0) = %d, %v", tm, err)
	}
	if err := s.AppendEntries([]LogEntry{ent(1, 1), ent(3, 2), ent(3, 3)}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if tm, err := s.Term(2); err != nil || tm != 3 {
		t.Fatalf("Term(2) = %d, %v, want 3", tm, err)
	}
	if li, err := s.LastIndex(); err != nil || li != 3 {
		t.Fatalf("LastIndex = %d, %v, want 3", li, err)
	}
}

func TestMemStorageSnapshotRoundTripAndCompact(t *testing.T) {
	s := NewMemStorage()
	if err := s.AppendEntries([]LogEntry{ent(1, 1), ent(1, 2), ent(2, 3), ent(2, 4)}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	snap := Snapshot{Index: 2, Term: 1, Data: []byte("state")}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	got, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if !reflect.DeepEqual(got, snap) {
		t.Fatalf("snapshot = %+v, want %+v", got, snap)
	}
	if err := s.Compact(2); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	fi, err := s.FirstIndex()
	if err != nil || fi != 3 {
		t.Fatalf("FirstIndex after compact = %d, %v, want 3", fi, err)
	}
	// Term of compacted-away index still answerable via snapshot.
	if tm, err := s.Term(2); err != nil || tm != 1 {
		t.Fatalf("Term(2) after compact = %d, %v, want 1", tm, err)
	}
	rest, err := s.Entries(3, 5)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if !reflect.DeepEqual(rest, []LogEntry{ent(2, 3), ent(2, 4)}) {
		t.Fatalf("Entries after compact = %+v", rest)
	}
}
