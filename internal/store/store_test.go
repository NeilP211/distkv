package store

import (
	"errors"
	"sync"
	"testing"
)

func TestPutGet(t *testing.T) {
	s := New()
	result, err := s.Apply(Command{Op: OpPut, Key: "foo", Value: "bar"})
	if err != nil {
		t.Fatalf("Apply Put: %v", err)
	}
	if result != "bar" {
		t.Fatalf("Put result: want %q, got %q", "bar", result)
	}

	val, ok := s.Get("foo")
	if !ok || val != "bar" {
		t.Fatalf("Get: want %q true, got %q %v", "bar", val, ok)
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Apply(Command{Op: OpPut, Key: "k", Value: "v"})
	result, err := s.Apply(Command{Op: OpDelete, Key: "k"})
	if err != nil {
		t.Fatalf("Apply Delete: %v", err)
	}
	if result != "" {
		t.Fatalf("Delete result: want empty, got %q", result)
	}
	_, ok := s.Get("k")
	if ok {
		t.Fatalf("key should be deleted")
	}
}

func TestCASSuccess(t *testing.T) {
	s := New()
	s.Apply(Command{Op: OpPut, Key: "x", Value: "old"})

	result, err := s.Apply(Command{Op: OpCAS, Key: "x", Value: "new", ExpectValue: "old"})
	if err != nil {
		t.Fatalf("Apply CAS: %v", err)
	}
	if result != "new" {
		t.Fatalf("CAS result: want %q, got %q", "new", result)
	}
	val, _ := s.Get("x")
	if val != "new" {
		t.Fatalf("after CAS: want %q, got %q", "new", val)
	}
}

func TestCASMismatch(t *testing.T) {
	s := New()
	s.Apply(Command{Op: OpPut, Key: "x", Value: "current"})

	_, err := s.Apply(Command{Op: OpCAS, Key: "x", Value: "new", ExpectValue: "wrong"})
	if !errors.Is(err, ErrCASMismatch) {
		t.Fatalf("want ErrCASMismatch, got %v", err)
	}
	// State must be unchanged.
	val, _ := s.Get("x")
	if val != "current" {
		t.Fatalf("state changed after mismatch: want %q, got %q", "current", val)
	}
}

func TestCASMissingKey(t *testing.T) {
	s := New()
	// Missing key has current value "".
	result, err := s.Apply(Command{Op: OpCAS, Key: "missing", Value: "v", ExpectValue: ""})
	if err != nil {
		t.Fatalf("CAS on missing key: %v", err)
	}
	if result != "v" {
		t.Fatalf("CAS on missing key result: want %q, got %q", "v", result)
	}
}

func TestSnapshotRestore(t *testing.T) {
	s := New()
	s.Apply(Command{Op: OpPut, Key: "a", Value: "1"})
	s.Apply(Command{Op: OpPut, Key: "b", Value: "2"})

	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	s2 := New()
	if err := s2.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		val, ok := s2.Get(kv.k)
		if !ok || val != kv.v {
			t.Fatalf("after restore Get(%q): want %q true, got %q %v", kv.k, kv.v, val, ok)
		}
	}
}

func TestConcurrentApplyGet(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	const goroutines = 50

	// Writers.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key"
			s.Apply(Command{Op: OpPut, Key: key, Value: "val"})
		}(i)
	}
	// Readers.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Get("key")
		}()
	}
	wg.Wait()
}
