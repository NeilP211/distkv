package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDurableStoreRecovery(t *testing.T) {
	dir := t.TempDir()

	// Open, apply some commands, close.
	ds, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}
	commands := []Command{
		{Op: OpPut, Key: "a", Value: "1"},
		{Op: OpPut, Key: "b", Value: "2"},
		{Op: OpPut, Key: "a", Value: "updated"},
	}
	for _, cmd := range commands {
		if _, err := ds.Do(cmd); err != nil {
			t.Fatalf("Do(%v): %v", cmd, err)
		}
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify state is recovered.
	ds2, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable (reopen): %v", err)
	}
	defer ds2.Close()

	// "a" should be "updated", "b" should be "2".
	if v, ok := ds2.Get("a"); !ok || v != "updated" {
		t.Fatalf("Get(a): want %q true, got %q %v", "updated", v, ok)
	}
	if v, ok := ds2.Get("b"); !ok || v != "2" {
		t.Fatalf("Get(b): want %q true, got %q %v", "2", v, ok)
	}
}

func TestDurableStoreCASRecovery(t *testing.T) {
	dir := t.TempDir()

	ds, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}
	if _, err := ds.Do(Command{Op: OpPut, Key: "x", Value: "old"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	result, err := ds.Do(Command{Op: OpCAS, Key: "x", Value: "new", ExpectValue: "old"})
	if err != nil {
		t.Fatalf("CAS Do: %v", err)
	}
	if result != "new" {
		t.Fatalf("CAS result: want %q, got %q", "new", result)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ds2, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable (reopen): %v", err)
	}
	defer ds2.Close()

	if v, ok := ds2.Get("x"); !ok || v != "new" {
		t.Fatalf("Get(x) after CAS recovery: want %q true, got %q %v", "new", v, ok)
	}
}

func TestDurableStoreDeleteRecovery(t *testing.T) {
	dir := t.TempDir()

	ds, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}
	if _, err := ds.Do(Command{Op: OpPut, Key: "del", Value: "present"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if _, err := ds.Do(Command{Op: OpDelete, Key: "del"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ds2, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable (reopen): %v", err)
	}
	defer ds2.Close()

	if _, ok := ds2.Get("del"); ok {
		t.Fatalf("key 'del' should be absent after recovery")
	}
}

func TestDurableStoreTornRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write 3 good commands.
	ds, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}
	goods := []Command{
		{Op: OpPut, Key: "p", Value: "1"},
		{Op: OpPut, Key: "q", Value: "2"},
		{Op: OpPut, Key: "r", Value: "3"},
	}
	for _, cmd := range goods {
		if _, err := ds.Do(cmd); err != nil {
			t.Fatalf("Do: %v", err)
		}
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the WAL by appending a partial record.
	walPath := filepath.Join(dir, "kv.wal")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile WAL: %v", err)
	}
	// Write 5 garbage bytes — not a complete record header.
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00})
	f.Close()

	// Reopen: should succeed and recover only the 3 good commands.
	ds2, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable after torn: %v", err)
	}
	defer ds2.Close()

	for _, cmd := range goods {
		v, ok := ds2.Get(cmd.Key)
		if !ok || v != cmd.Value {
			t.Fatalf("Get(%q) after torn recovery: want %q true, got %q %v", cmd.Key, cmd.Value, v, ok)
		}
	}
}

func TestDurableStoreCASMismatchNotPersisted(t *testing.T) {
	dir := t.TempDir()

	ds, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}
	if _, err := ds.Do(Command{Op: OpPut, Key: "m", Value: "val"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	_, err = ds.Do(Command{Op: OpCAS, Key: "m", Value: "new", ExpectValue: "wrong"})
	if !errors.Is(err, ErrCASMismatch) {
		t.Fatalf("want ErrCASMismatch, got %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// On recovery, m must still be "val".
	ds2, err := OpenDurable(dir)
	if err != nil {
		t.Fatalf("OpenDurable (reopen): %v", err)
	}
	defer ds2.Close()

	if v, ok := ds2.Get("m"); !ok || v != "val" {
		t.Fatalf("Get(m): want %q true, got %q %v", "val", v, ok)
	}
}
