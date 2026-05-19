package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
)

// OpType represents the type of a KV operation.
type OpType int

const (
	// OpPut sets the value for a key.
	OpPut OpType = iota
	// OpDelete removes a key from the store.
	OpDelete
	// OpCAS performs a compare-and-swap: sets the value only if the current
	// value matches ExpectValue.
	OpCAS
)

// Command is a KV operation to be applied to the state machine.
type Command struct {
	// Op is the operation type (OpPut, OpDelete, or OpCAS).
	Op OpType
	// Key is the key to operate on.
	// Value is the value to set (used by OpPut and OpCAS).
	Key, Value string
	// ExpectValue is the expected current value; only used by OpCAS.
	ExpectValue string
}

// ErrCASMismatch is returned by Apply when a CAS operation fails because the
// current value does not match ExpectValue.
var ErrCASMismatch = errors.New("store: CAS mismatch")

// ErrUnknownOp is returned by Apply when the Command carries an unrecognised OpType.
var ErrUnknownOp = errors.New("store: unknown op")

// Store is an in-memory key-value state machine guarded by a RWMutex.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New creates a new empty Store.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Apply applies a Command to the state machine and returns a result string.
// OpPut: sets key=value, returns value.
// OpDelete: deletes key, returns "".
// OpCAS: if current value equals ExpectValue, sets key=value and returns value;
// otherwise returns "", ErrCASMismatch.
func (s *Store) Apply(cmd Command) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
		return cmd.Value, nil

	case OpDelete:
		delete(s.data, cmd.Key)
		return "", nil

	case OpCAS:
		current := s.data[cmd.Key] // zero value "" for missing key
		if current != cmd.ExpectValue {
			return "", ErrCASMismatch
		}
		s.data[cmd.Key] = cmd.Value
		return cmd.Value, nil

	default:
		return "", ErrUnknownOp
	}
}

// Get retrieves the value for a key.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Snapshot serializes the current state to bytes using gob.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Restore replaces the current state with the state from data.
func (s *Store) Restore(data []byte) error {
	m := make(map[string]string)
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&m); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = m
	return nil
}
