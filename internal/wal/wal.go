package wal

import (
	"errors"
	"io"
	"os"
)

// WAL is an append-only write-ahead log backed by a single file.
type WAL struct {
	f *os.File
}

// Open opens (or creates) a WAL at the given path.
// The file is opened for both read and write so Replay can be called on it.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f}, nil
}

// Append writes one encoded record to the WAL and fsyncs.
func (w *WAL) Append(payload []byte) error {
	encoded := encodeRecord(payload)
	if _, err := w.f.Write(encoded); err != nil {
		return err
	}
	return w.f.Sync()
}

// Replay reads all complete records from the start of the WAL, calling fn for
// each payload.  If it encounters a torn trailing record it truncates the file
// at the start of that record and stops without error.
func (w *WAL) Replay(fn func([]byte) error) error {
	// Seek to the beginning for reading.
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var offset int64
	for {
		payload, err := decodeRecord(w.f)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Clean end of log.
				return nil
			}
			if errors.Is(err, errTornRecord) {
				// Truncate the torn record from the file.
				if truncErr := w.f.Truncate(offset); truncErr != nil {
					return truncErr
				}
				// Seek to end so subsequent Appends work correctly.
				if _, seekErr := w.f.Seek(0, io.SeekEnd); seekErr != nil {
					return seekErr
				}
				return nil
			}
			return err
		}

		if err := fn(payload); err != nil {
			return err
		}

		// Advance offset past the record we just consumed.
		offset += int64(headerSize + len(payload))
	}
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	return w.f.Close()
}
