package wal

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	payload := []byte("hello, world")
	encoded := encodeRecord(payload)
	got, err := decodeRecord(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decodeRecord: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("want %q, got %q", payload, got)
	}
}

func TestRoundTripEmpty(t *testing.T) {
	payload := []byte{}
	encoded := encodeRecord(payload)
	got, err := decodeRecord(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decodeRecord: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("want %q, got %q", payload, got)
	}
}

func TestFlippedCRC(t *testing.T) {
	payload := []byte("test payload")
	encoded := encodeRecord(payload)
	// CRC bytes are at offset 4–7 (bytes 4..7); flip one byte
	encoded[5] ^= 0xFF
	_, err := decodeRecord(bytes.NewReader(encoded))
	if !errors.Is(err, errTornRecord) {
		t.Fatalf("want errTornRecord, got %v", err)
	}
}

func TestTruncatedPayload(t *testing.T) {
	payload := []byte("test payload for truncation test")
	encoded := encodeRecord(payload)
	// Keep header (8 bytes) but drop half the payload
	truncated := encoded[:8+len(payload)/2]
	_, err := decodeRecord(bytes.NewReader(truncated))
	if !errors.Is(err, errTornRecord) {
		t.Fatalf("want errTornRecord, got %v", err)
	}
}

func TestTruncatedHeader(t *testing.T) {
	payload := []byte("test")
	encoded := encodeRecord(payload)
	// Only provide 4 bytes of the 8-byte header
	truncated := encoded[:4]
	_, err := decodeRecord(bytes.NewReader(truncated))
	if !errors.Is(err, errTornRecord) {
		t.Fatalf("want errTornRecord, got %v", err)
	}
}

func TestCleanEOF(t *testing.T) {
	// Empty reader → clean EOF
	_, err := decodeRecord(bytes.NewReader([]byte{}))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}
}
