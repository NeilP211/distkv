package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"pgregory.net/rapid"
)

// rapidTempDir creates a temp dir and registers cleanup via t.Cleanup using the
// underlying *testing.T embedded in *rapid.T.
func rapidTempDir(t *rapid.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal-rapid-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// walPath creates a temp dir and returns the path to a WAL file.
func walPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

func TestAppendReplayRoundTrip(t *testing.T) {
	path := walPath(t)
	payloads := [][]byte{
		[]byte("entry one"),
		[]byte("entry two"),
		[]byte("entry three"),
	}

	// Write records.
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, p := range payloads {
		if err := w.Append(p); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Replay from reopened file.
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	var got [][]byte
	err = w2.Replay(func(payload []byte) error {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		got = append(got, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close (reopen): %v", err)
	}

	if len(got) != len(payloads) {
		t.Fatalf("want %d payloads, got %d", len(payloads), len(got))
	}
	for i, p := range payloads {
		if !bytes.Equal(got[i], p) {
			t.Fatalf("payload %d: want %q, got %q", i, p, got[i])
		}
	}
}

func TestTornTrailingRecord(t *testing.T) {
	path := walPath(t)
	payloads := [][]byte{
		[]byte("good record 1"),
		[]byte("good record 2"),
		[]byte("good record 3"),
	}

	// Write good records.
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, p := range payloads {
		if err := w.Append(p); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append a partial (torn) record directly to the file.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	// Write an incomplete header (3 bytes instead of 8).
	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("Write torn: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close torn: %v", err)
	}

	// Note the size just before the torn record.
	sizeBeforeTorn := int64(0)
	for _, p := range payloads {
		sizeBeforeTorn += int64(headerSize + len(p))
	}

	// Reopen: Replay should yield N good records and truncate the torn tail.
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	var got [][]byte
	err = w2.Replay(func(payload []byte) error {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		got = append(got, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close (reopen): %v", err)
	}

	if len(got) != len(payloads) {
		t.Fatalf("want %d payloads, got %d", len(payloads), len(got))
	}

	// Verify file size is back to the size without torn record.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != sizeBeforeTorn {
		t.Fatalf("want file size %d, got %d", sizeBeforeTorn, fi.Size())
	}
}

func TestEmptyFileReplaysZero(t *testing.T) {
	path := walPath(t)
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	count := 0
	err = w.Replay(func(_ []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("want 0 records, got %d", count)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPropertyAppendReplay(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		path := filepath.Join(rapidTempDir(t), "prop.wal")
		payloads := rapid.SliceOf(rapid.SliceOf(rapid.Byte())).Draw(t, "payloads")

		w, err := Open(path)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for _, p := range payloads {
			if err := w.Append(p); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		w2, err := Open(path)
		if err != nil {
			t.Fatalf("Open (reopen): %v", err)
		}
		var got [][]byte
		err = w2.Replay(func(payload []byte) error {
			cp := make([]byte, len(payload))
			copy(cp, payload)
			got = append(got, cp)
			return nil
		})
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}
		if err := w2.Close(); err != nil {
			t.Fatalf("Close (reopen): %v", err)
		}

		if len(got) != len(payloads) {
			t.Fatalf("want %d payloads, got %d", len(payloads), len(got))
		}
		for i := range payloads {
			if !bytes.Equal(got[i], payloads[i]) {
				t.Fatalf("payload %d mismatch", i)
			}
		}
	})
}
