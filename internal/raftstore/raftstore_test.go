package raftstore_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/raftstore"
)

// ───────────────────────────────────────────────────────────────────────────
// helpers
// ───────────────────────────────────────────────────────────────────────────

func ent(term, index uint64) raft.LogEntry {
	return raft.LogEntry{Term: term, Index: index, Type: raft.EntryNormal}
}

// newBolt opens a fresh BoltStorage in a temp directory, registering cleanup.
func newBolt(t *testing.T) *raftstore.BoltStorage {
	t.Helper()
	dir := t.TempDir()
	s, err := raftstore.Open(filepath.Join(dir, "raft.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// ───────────────────────────────────────────────────────────────────────────
// Shared conformance suite — run against both MemStorage and BoltStorage
// ───────────────────────────────────────────────────────────────────────────

// testStorageConformance exercises the common behaviour that both Storage
// implementations must satisfy.  Compacted-index error types are verified only
// against sentinels exported from the raft package where both implementations
// agree; bbolt-only edge cases live in separate tests below.
func testStorageConformance(t *testing.T, newStore func() raft.Storage) {
	t.Helper()

	// ── 1. Empty state ───────────────────────────────────────────────────
	t.Run("EmptyState", func(t *testing.T) {
		s := newStore()

		li, err := s.LastIndex()
		if err != nil || li != 0 {
			t.Fatalf("empty LastIndex = %d, %v; want 0, nil", li, err)
		}
		fi, err := s.FirstIndex()
		if err != nil || fi != 1 {
			t.Fatalf("empty FirstIndex = %d, %v; want 1, nil", fi, err)
		}
		tm, err := s.Term(0)
		if err != nil || tm != 0 {
			t.Fatalf("Term(0) = %d, %v; want 0, nil", tm, err)
		}
		hs, err := s.LoadHardState()
		if err != nil {
			t.Fatalf("LoadHardState: %v", err)
		}
		if hs != (raft.HardState{}) {
			t.Fatalf("fresh HardState = %+v; want zero", hs)
		}
		snap, err := s.LoadSnapshot()
		if err != nil {
			t.Fatalf("LoadSnapshot: %v", err)
		}
		if snap.Index != 0 || snap.Term != 0 || snap.Data != nil {
			t.Fatalf("fresh Snapshot = %+v; want zero", snap)
		}
	})

	// ── 2. Append and read back ──────────────────────────────────────────
	t.Run("AppendAndRead", func(t *testing.T) {
		s := newStore()
		in := []raft.LogEntry{ent(1, 1), ent(1, 2), ent(2, 3)}
		if err := s.AppendEntries(in); err != nil {
			t.Fatalf("AppendEntries: %v", err)
		}
		got, err := s.Entries(1, 4)
		if err != nil {
			t.Fatalf("Entries(1,4): %v", err)
		}
		if !reflect.DeepEqual(got, in) {
			t.Fatalf("Entries = %+v; want %+v", got, in)
		}
		// Partial range.
		got, err = s.Entries(2, 3)
		if err != nil {
			t.Fatalf("Entries(2,3): %v", err)
		}
		if !reflect.DeepEqual(got, []raft.LogEntry{ent(1, 2)}) {
			t.Fatalf("Entries(2,3) = %+v", got)
		}
	})

	// ── 3. Conflicting suffix truncation ─────────────────────────────────
	t.Run("ConflictTruncation", func(t *testing.T) {
		s := newStore()
		if err := s.AppendEntries([]raft.LogEntry{
			ent(1, 1), ent(1, 2), ent(1, 3), ent(1, 4),
		}); err != nil {
			t.Fatalf("first AppendEntries: %v", err)
		}
		// Diverge at index 3.
		if err := s.AppendEntries([]raft.LogEntry{
			ent(2, 3), ent(2, 4), ent(2, 5),
		}); err != nil {
			t.Fatalf("second AppendEntries: %v", err)
		}
		li, err := s.LastIndex()
		if err != nil || li != 5 {
			t.Fatalf("LastIndex = %d, %v; want 5", li, err)
		}
		got, err := s.Entries(1, 6)
		if err != nil {
			t.Fatalf("Entries: %v", err)
		}
		want := []raft.LogEntry{ent(1, 1), ent(1, 2), ent(2, 3), ent(2, 4), ent(2, 5)}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Entries = %+v; want %+v", got, want)
		}
	})

	// ── 4. Identical re-append (no truncation) ───────────────────────────
	t.Run("IdenticalReappend", func(t *testing.T) {
		s := newStore()
		if err := s.AppendEntries([]raft.LogEntry{ent(1, 1), ent(1, 2), ent(1, 3)}); err != nil {
			t.Fatalf("AppendEntries: %v", err)
		}
		if err := s.AppendEntries([]raft.LogEntry{ent(1, 2), ent(1, 3), ent(1, 4)}); err != nil {
			t.Fatalf("second AppendEntries: %v", err)
		}
		li, err := s.LastIndex()
		if err != nil || li != 4 {
			t.Fatalf("LastIndex = %d, %v; want 4", li, err)
		}
	})

	// ── 5. LastIndex / FirstIndex / Term correctness ──────────────────────
	t.Run("IndicesAndTerms", func(t *testing.T) {
		s := newStore()
		if err := s.AppendEntries([]raft.LogEntry{ent(1, 1), ent(3, 2), ent(3, 3)}); err != nil {
			t.Fatalf("AppendEntries: %v", err)
		}
		if tm, err := s.Term(2); err != nil || tm != 3 {
			t.Fatalf("Term(2) = %d, %v; want 3", tm, err)
		}
		if li, err := s.LastIndex(); err != nil || li != 3 {
			t.Fatalf("LastIndex = %d, %v; want 3", li, err)
		}
		if fi, err := s.FirstIndex(); err != nil || fi != 1 {
			t.Fatalf("FirstIndex = %d, %v; want 1", fi, err)
		}
	})

	// ── 6. HardState round-trip ───────────────────────────────────────────
	t.Run("HardStateRoundTrip", func(t *testing.T) {
		s := newStore()
		want := raft.HardState{CurrentTerm: 7, VotedFor: "n2"}
		if err := s.SaveHardState(want); err != nil {
			t.Fatalf("SaveHardState: %v", err)
		}
		got, err := s.LoadHardState()
		if err != nil {
			t.Fatalf("LoadHardState: %v", err)
		}
		if got != want {
			t.Fatalf("HardState = %+v; want %+v", got, want)
		}
	})

	// ── 7. Snapshot round-trip ────────────────────────────────────────────
	t.Run("SnapshotRoundTrip", func(t *testing.T) {
		s := newStore()
		snap := raft.Snapshot{Index: 10, Term: 3, Data: []byte("state-image")}
		if err := s.SaveSnapshot(snap); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		got, err := s.LoadSnapshot()
		if err != nil {
			t.Fatalf("LoadSnapshot: %v", err)
		}
		if !reflect.DeepEqual(got, snap) {
			t.Fatalf("Snapshot = %+v; want %+v", got, snap)
		}
	})

	// ── 8. Compact then FirstIndex/Term ───────────────────────────────────
	t.Run("CompactBehaviour", func(t *testing.T) {
		s := newStore()
		if err := s.AppendEntries([]raft.LogEntry{
			ent(1, 1), ent(1, 2), ent(2, 3), ent(2, 4),
		}); err != nil {
			t.Fatalf("AppendEntries: %v", err)
		}
		snap := raft.Snapshot{Index: 2, Term: 1, Data: []byte("snap")}
		if err := s.SaveSnapshot(snap); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		if err := s.Compact(2); err != nil {
			t.Fatalf("Compact(2): %v", err)
		}
		fi, err := s.FirstIndex()
		if err != nil || fi != 3 {
			t.Fatalf("FirstIndex after compact = %d, %v; want 3", fi, err)
		}
		// Compacted-away index is still answerable via snapshot.
		if tm, err := s.Term(2); err != nil || tm != 1 {
			t.Fatalf("Term(2) after compact = %d, %v; want 1", tm, err)
		}
		// Remaining entries still readable.
		rest, err := s.Entries(3, 5)
		if err != nil {
			t.Fatalf("Entries(3,5): %v", err)
		}
		if !reflect.DeepEqual(rest, []raft.LogEntry{ent(2, 3), ent(2, 4)}) {
			t.Fatalf("Entries after compact = %+v", rest)
		}
		// Index 1 is compacted below the snapshot: must get ErrCompacted.
		_, err = s.Term(1)
		if !errors.Is(err, raft.ErrCompacted) {
			t.Fatalf("Term(1) below snapshot = %v; want ErrCompacted", err)
		}
	})

	// ── 9. Entries(lo, lo) below FirstIndex: ErrCompacted ────────────────
	t.Run("EntriesLoLoCompacted", func(t *testing.T) {
		s := newStore()
		if err := s.AppendEntries([]raft.LogEntry{
			ent(1, 1), ent(1, 2), ent(2, 3),
		}); err != nil {
			t.Fatalf("AppendEntries: %v", err)
		}
		snap := raft.Snapshot{Index: 2, Term: 1, Data: []byte("snap")}
		if err := s.SaveSnapshot(snap); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		if err := s.Compact(2); err != nil {
			t.Fatalf("Compact(2): %v", err)
		}
		// lo == hi == 1 is below FirstIndex (3); must return ErrCompacted.
		_, err := s.Entries(1, 1)
		if !errors.Is(err, raft.ErrCompacted) {
			t.Fatalf("Entries(1,1) below FirstIndex = %v; want ErrCompacted", err)
		}
	})

	// ── 10. Term(index) for index above LastIndex: ErrUnavailable ────────
	t.Run("TermAboveLastIndex", func(t *testing.T) {
		s := newStore()
		if err := s.AppendEntries([]raft.LogEntry{ent(1, 1)}); err != nil {
			t.Fatalf("AppendEntries: %v", err)
		}
		_, err := s.Term(999)
		if !errors.Is(err, raft.ErrUnavailable) {
			t.Fatalf("Term(999) = %v; want ErrUnavailable", err)
		}
	})
}

// ───────────────────────────────────────────────────────────────────────────
// Run conformance against MemStorage
// ───────────────────────────────────────────────────────────────────────────

func TestMemStorageConformance(t *testing.T) {
	testStorageConformance(t, func() raft.Storage { return raft.NewMemStorage() })
}

// ───────────────────────────────────────────────────────────────────────────
// Run conformance against BoltStorage
// ───────────────────────────────────────────────────────────────────────────

func TestBoltStorageConformance(t *testing.T) {
	testStorageConformance(t, func() raft.Storage {
		dir := t.TempDir()
		s, err := raftstore.Open(filepath.Join(dir, "raft.db"))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

// ───────────────────────────────────────────────────────────────────────────
// bbolt-specific: durability across close/reopen
// ───────────────────────────────────────────────────────────────────────────

func TestBoltDurabilityAfterClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.db")

	// Phase 1: write state.
	s, err := raftstore.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	entries := []raft.LogEntry{ent(1, 1), ent(1, 2), ent(2, 3)}
	if err := s.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	hs := raft.HardState{CurrentTerm: 4, VotedFor: "n2"}
	if err := s.SaveHardState(hs); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	snap := raft.Snapshot{Index: 1, Term: 1, Data: []byte("state")}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify the file exists.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("db file missing after close: %v", err)
	}

	// Phase 2: reopen and verify everything is intact.
	s2, err := raftstore.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	// HardState.
	gotHS, err := s2.LoadHardState()
	if err != nil {
		t.Fatalf("LoadHardState after reopen: %v", err)
	}
	if gotHS != hs {
		t.Fatalf("HardState = %+v; want %+v", gotHS, hs)
	}

	// Snapshot.
	gotSnap, err := s2.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot after reopen: %v", err)
	}
	if !reflect.DeepEqual(gotSnap, snap) {
		t.Fatalf("Snapshot = %+v; want %+v", gotSnap, snap)
	}

	// Log entries (after the snapshot, entries at indices 2 and 3 remain).
	got, err := s2.Entries(2, 4)
	if err != nil {
		t.Fatalf("Entries(2,4) after reopen: %v", err)
	}
	wantEntries := []raft.LogEntry{ent(1, 2), ent(2, 3)}
	if !reflect.DeepEqual(got, wantEntries) {
		t.Fatalf("Entries after reopen = %+v; want %+v", got, wantEntries)
	}
}

// ───────────────────────────────────────────────────────────────────────────
// bbolt-specific: compact then reopen
// ───────────────────────────────────────────────────────────────────────────

func TestBoltCompactAndReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.db")

	s, err := raftstore.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := s.AppendEntries([]raft.LogEntry{
		ent(1, 1), ent(1, 2), ent(2, 3), ent(2, 4),
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	snap := raft.Snapshot{Index: 2, Term: 1, Data: []byte("compact-snap")}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.Compact(2); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := raftstore.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	fi, err := s2.FirstIndex()
	if err != nil || fi != 3 {
		t.Fatalf("FirstIndex after compact+reopen = %d, %v; want 3", fi, err)
	}

	// Compacted index below snapshot covered by snapshot term.
	tm, err := s2.Term(2)
	if err != nil || tm != 1 {
		t.Fatalf("Term(2) after compact+reopen = %d, %v; want 1", tm, err)
	}

	// Index 1 fully compacted — ErrCompacted.
	_, err = s2.Term(1)
	if !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Term(1) after compact+reopen = %v; want ErrCompacted", err)
	}

	// Remaining entries.
	rest, err := s2.Entries(3, 5)
	if err != nil {
		t.Fatalf("Entries(3,5): %v", err)
	}
	if !reflect.DeepEqual(rest, []raft.LogEntry{ent(2, 3), ent(2, 4)}) {
		t.Fatalf("Entries after compact+reopen = %+v", rest)
	}
}

// ───────────────────────────────────────────────────────────────────────────
// bbolt-specific: SaveSnapshot moves FirstIndex even without Compact
// ───────────────────────────────────────────────────────────────────────────

func TestBoltSnapshotMovesFirstIndex(t *testing.T) {
	s := newBolt(t)

	if err := s.AppendEntries([]raft.LogEntry{ent(1, 1), ent(1, 2), ent(2, 3)}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	snap := raft.Snapshot{Index: 2, Term: 1, Data: []byte("s")}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// FirstIndex must reflect snapshot even before Compact is called.
	fi, err := s.FirstIndex()
	if err != nil || fi != 3 {
		t.Fatalf("FirstIndex after snapshot = %d, %v; want 3", fi, err)
	}
	// Snapshot term is answerable.
	if tm, err := s.Term(2); err != nil || tm != 1 {
		t.Fatalf("Term(snap index) = %d, %v; want 1", tm, err)
	}
}

// ───────────────────────────────────────────────────────────────────────────
// bbolt-specific: LastIndex reflects snapshot when log is empty
// ───────────────────────────────────────────────────────────────────────────

func TestBoltLastIndexReflectsSnapshot(t *testing.T) {
	s := newBolt(t)

	snap := raft.Snapshot{Index: 5, Term: 2, Data: []byte("x")}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	// Compact away hypothetical log entries; log is now "empty" relative to snap.
	// Even without compaction: LastIndex should return 5.
	li, err := s.LastIndex()
	if err != nil || li != 5 {
		t.Fatalf("LastIndex with empty log+snapshot = %d, %v; want 5", li, err)
	}
}
