// Package raftstore provides a bbolt-backed implementation of raft.Storage.
// It persists HardState, log entries, and snapshots in a single bbolt database
// with three buckets: "meta", "log", and "snapshot".
package raftstore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	bolt "go.etcd.io/bbolt"

	"github.com/NeilP211/distkv/internal/raft"
)

// bucket names
var (
	bucketMeta     = []byte("meta")
	bucketLog      = []byte("log")
	bucketSnapshot = []byte("snapshot")
)

// meta keys
var (
	keyHardState    = []byte("hardstate")
	keySnapshotMeta = []byte("snapshotmeta")
)

// snapshot bucket key
var keySnapshotData = []byte("data")

// hardStatePersist is the gob-encodable form of raft.HardState.
type hardStatePersist struct {
	CurrentTerm uint64
	VotedFor    string
}

// snapshotMeta is the gob-encodable snapshot header (index + term) stored in meta.
type snapshotMeta struct {
	Index uint64
	Term  uint64
}

// BoltStorage is a goroutine-safe, bbolt-backed implementation of raft.Storage.
//
// Bucket layout:
//
//	meta      → "hardstate"    : gob(hardStatePersist)
//	          → "snapshotmeta" : gob(snapshotMeta)
//	log       → bigEndian(index) : gob(raft.LogEntry)
//	snapshot  → "data"          : raw snapshot Data bytes
type BoltStorage struct {
	mu sync.RWMutex
	db *bolt.DB

	// cached snapshot index/term (loaded at Open, updated on SaveSnapshot).
	snapIndex uint64
	snapTerm  uint64
}

// Open opens (or creates) a bbolt database at path and ensures all required
// buckets exist.  The caller must eventually call Close.
func Open(path string) (*BoltStorage, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("raftstore: open %s: %w", path, err)
	}

	// Ensure all three buckets exist in a single write transaction.
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketMeta, bucketLog, bucketSnapshot} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("raftstore: create buckets: %w", err)
	}

	s := &BoltStorage{db: db}

	// Load the persisted snapshot header to prime our cached values.
	if err := db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		v := meta.Get(keySnapshotMeta)
		if v == nil {
			return nil // no snapshot yet
		}
		var sm snapshotMeta
		if err := gobDecode(v, &sm); err != nil {
			return err
		}
		s.snapIndex = sm.Index
		s.snapTerm = sm.Term
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("raftstore: load snapshot meta: %w", err)
	}

	return s, nil
}

// Close closes the underlying bbolt database.
func (s *BoltStorage) Close() error {
	return s.db.Close()
}

// ──────────────────────────────────────────────────────────────────────────
// HardState
// ──────────────────────────────────────────────────────────────────────────

// LoadHardState implements raft.Storage.
func (s *BoltStorage) LoadHardState() (raft.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var hs raft.HardState
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketMeta).Get(keyHardState)
		if v == nil {
			return nil // zero value
		}
		var p hardStatePersist
		if err := gobDecode(v, &p); err != nil {
			return err
		}
		hs = raft.HardState{CurrentTerm: p.CurrentTerm, VotedFor: raft.NodeID(p.VotedFor)}
		return nil
	})
	return hs, err
}

// SaveHardState implements raft.Storage.
func (s *BoltStorage) SaveHardState(hs raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p := hardStatePersist{CurrentTerm: hs.CurrentTerm, VotedFor: string(hs.VotedFor)}
	encoded, err := gobEncode(p)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put(keyHardState, encoded)
	})
}

// ──────────────────────────────────────────────────────────────────────────
// Log entries
// ──────────────────────────────────────────────────────────────────────────

// AppendEntries implements raft.Storage.
//
// For each incoming entry, if an entry already exists at that index with a
// different term, the log is truncated from that index onwards before writing
// the new entries.  Entries whose index/term already match the stored entry
// are rewritten (harmless, avoids extra reads).
func (s *BoltStorage) AppendEntries(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bolt.Tx) error {
		lb := tx.Bucket(bucketLog)

		// Find the first conflicting entry (same index, different term).
		truncateFrom := uint64(0)
		for _, e := range entries {
			existing := lb.Get(indexKey(e.Index))
			if existing == nil {
				// No existing entry at this index — no conflict here.
				// Once we pass the end of the stored log we can stop checking.
				break
			}
			var stored raft.LogEntry
			if err := gobDecode(existing, &stored); err != nil {
				return err
			}
			if stored.Term != e.Term {
				truncateFrom = e.Index
				break
			}
			// Terms match: identical entry, continue checking.
		}

		// Truncate conflicting suffix if needed.
		if truncateFrom != 0 {
			// Delete all keys >= truncateFrom.
			// bbolt keys are sorted, so we can use a cursor.
			c := lb.Cursor()
			from := indexKey(truncateFrom)
			for k, _ := c.Seek(from); k != nil; k, _ = c.Next() {
				if err := lb.Delete(k); err != nil {
					return err
				}
			}
		}

		// Write all incoming entries.
		for _, e := range entries {
			encoded, err := gobEncode(e)
			if err != nil {
				return err
			}
			if err := lb.Put(indexKey(e.Index), encoded); err != nil {
				return err
			}
		}
		return nil
	})
}

// Entries implements raft.Storage; returns the half-open range [lo, hi).
func (s *BoltStorage) Entries(lo, hi uint64) ([]raft.LogEntry, error) {
	if lo > hi {
		return nil, errors.New("raftstore: Entries lo > hi")
	}
	if lo == hi {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	firstIdx := s.snapIndex + 1
	if lo < firstIdx {
		return nil, raft.ErrCompacted
	}

	var out []raft.LogEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		lb := tx.Bucket(bucketLog)

		// Check hi-1 is available.
		if lb.Get(indexKey(hi-1)) == nil {
			// hi might be exactly lastIndex+1 — check lo..hi-1 exist.
			if lb.Get(indexKey(lo)) == nil {
				return raft.ErrUnavailable
			}
			return raft.ErrUnavailable
		}

		out = make([]raft.LogEntry, 0, hi-lo)
		for i := lo; i < hi; i++ {
			v := lb.Get(indexKey(i))
			if v == nil {
				return raft.ErrUnavailable
			}
			var e raft.LogEntry
			if err := gobDecode(v, &e); err != nil {
				return err
			}
			out = append(out, e)
		}
		return nil
	})
	return out, err
}

// LastIndex implements raft.Storage.
// Returns the highest stored log index, or the snapshot index if the log is
// empty, or 0 if there is neither.
func (s *BoltStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var li uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		lb := tx.Bucket(bucketLog)
		k, _ := lb.Cursor().Last()
		if k != nil {
			li = indexFromKey(k)
		} else {
			li = s.snapIndex
		}
		return nil
	})
	return li, err
}

// FirstIndex implements raft.Storage.
// Returns snapIndex + 1, or 1 if there is no snapshot.
func (s *BoltStorage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapIndex + 1, nil
}

// Term implements raft.Storage.
func (s *BoltStorage) Term(index uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index == 0 {
		return 0, nil
	}

	// The snapshot index itself is covered by the snapshot's term.
	if index == s.snapIndex {
		return s.snapTerm, nil
	}

	// Indices below firstIndex that the snapshot doesn't cover.
	firstIdx := s.snapIndex + 1
	if index < firstIdx {
		return 0, raft.ErrCompacted
	}

	var term uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketLog).Get(indexKey(index))
		if v == nil {
			// Determine if beyond the end of the log.
			// We'll check last key.
			lb := tx.Bucket(bucketLog)
			k, _ := lb.Cursor().Last()
			if k == nil || indexFromKey(k) < index {
				return raft.ErrUnavailable
			}
			return raft.ErrUnavailable
		}
		var e raft.LogEntry
		if err := gobDecode(v, &e); err != nil {
			return err
		}
		term = e.Term
		return nil
	})
	return term, err
}

// ──────────────────────────────────────────────────────────────────────────
// Snapshot
// ──────────────────────────────────────────────────────────────────────────

// SaveSnapshot implements raft.Storage.
// Persists the snapshot and updates the cached snapshot index/term.
// Log entries now subsumed by the snapshot are NOT automatically compacted
// here; use Compact to do that explicitly.  (MemStorage does discard them, but
// the interface only requires Compact to remove them.  To keep FirstIndex
// consistent we rely on snapIndex, just like MemStorage's firstIndex().)
func (s *BoltStorage) SaveSnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sm := snapshotMeta{Index: snap.Index, Term: snap.Term}
	smEncoded, err := gobEncode(sm)
	if err != nil {
		return err
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if err := meta.Put(keySnapshotMeta, smEncoded); err != nil {
			return err
		}
		snapBucket := tx.Bucket(bucketSnapshot)
		return snapBucket.Put(keySnapshotData, snap.Data)
	})
	if err != nil {
		return err
	}

	s.snapIndex = snap.Index
	s.snapTerm = snap.Term
	return nil
}

// LoadSnapshot implements raft.Storage.
func (s *BoltStorage) LoadSnapshot() (raft.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.snapIndex == 0 {
		return raft.Snapshot{}, nil
	}

	var snap raft.Snapshot
	err := s.db.View(func(tx *bolt.Tx) error {
		snap.Index = s.snapIndex
		snap.Term = s.snapTerm
		snap.Data = tx.Bucket(bucketSnapshot).Get(keySnapshotData)
		// Make a copy so the caller owns the slice.
		if snap.Data != nil {
			cp := make([]byte, len(snap.Data))
			copy(cp, snap.Data)
			snap.Data = cp
		}
		return nil
	})
	return snap, err
}

// Compact implements raft.Storage; discards all log entries with index <= upto.
func (s *BoltStorage) Compact(upto uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bolt.Tx) error {
		lb := tx.Bucket(bucketLog)
		c := lb.Cursor()
		uptoKey := indexKey(upto)
		for k, _ := c.First(); k != nil && bytes.Compare(k, uptoKey) <= 0; k, _ = c.Next() {
			if err := lb.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// ──────────────────────────────────────────────────────────────────────────
// Encoding helpers
// ──────────────────────────────────────────────────────────────────────────

// indexKey encodes a uint64 log index as an 8-byte big-endian slice, which
// preserves ordering in bbolt's sorted key space.
func indexKey(index uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, index)
	return b
}

func indexFromKey(k []byte) uint64 {
	return binary.BigEndian.Uint64(k)
}

func gobEncode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecode(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}
