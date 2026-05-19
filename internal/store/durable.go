package store

import (
	"bytes"
	"encoding/gob"
	"path/filepath"

	"github.com/NeilP211/distkv/internal/wal"
)

// DurableStore wraps an in-memory Store with a WAL for crash recovery.
// Every command is serialized and appended to the WAL before being applied,
// so a crash never leaves an applied-but-unlogged write.
type DurableStore struct {
	*Store
	w *wal.WAL
}

// OpenDurable opens (or creates) a durable KV store in dir.
// It opens a WAL at filepath.Join(dir, "kv.wal") and replays it to rebuild
// the in-memory state.
func OpenDurable(dir string) (*DurableStore, error) {
	walPath := filepath.Join(dir, "kv.wal")
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}

	s := New()

	// Replay the WAL to reconstruct state.
	err = w.Replay(func(payload []byte) error {
		var cmd Command
		dec := gob.NewDecoder(bytes.NewReader(payload))
		if err := dec.Decode(&cmd); err != nil {
			return err
		}
		_, err := s.Apply(cmd)
		// Ignore ErrCASMismatch during replay — it was already applied
		// (or already failed) in a previous session; we must not error out.
		if err != nil && err != ErrCASMismatch {
			return err
		}
		return nil
	})
	if err != nil {
		w.Close()
		return nil, err
	}

	return &DurableStore{Store: s, w: w}, nil
}

// Do serializes cmd, appends it to the WAL, then applies it to the Store.
// Append-before-Apply ensures no applied write is ever unlogged.
func (d *DurableStore) Do(cmd Command) (string, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(cmd); err != nil {
		return "", err
	}

	if err := d.w.Append(buf.Bytes()); err != nil {
		return "", err
	}

	return d.Store.Apply(cmd)
}

// Close closes the underlying WAL.
func (d *DurableStore) Close() error {
	return d.w.Close()
}
