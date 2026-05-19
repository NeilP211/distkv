package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// errTornRecord is returned when a record is partial or its CRC does not match.
var errTornRecord = errors.New("wal: torn record")

// headerSize is the size of the on-disk record header in bytes.
// Layout: [uint32 length][uint32 crc32] — all little-endian.
const headerSize = 8

// encodeRecord encodes payload into an on-disk record:
// [uint32 length][uint32 crc32][payload]
func encodeRecord(payload []byte) []byte {
	length := uint32(len(payload))
	checksum := crc32.ChecksumIEEE(payload)

	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], length)
	binary.LittleEndian.PutUint32(buf[4:8], checksum)
	copy(buf[8:], payload)
	return buf
}

// decodeRecord reads one record from r.
// Returns io.EOF on a clean end-of-stream (reader exhausted exactly at a record boundary).
// Returns errTornRecord when the record is partial or the CRC does not match.
func decodeRecord(r io.Reader) ([]byte, error) {
	header := make([]byte, headerSize)
	_, err := io.ReadFull(r, header)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Clean EOF: reader was exhausted before the header started.
			return nil, io.EOF
		}
		// io.ErrUnexpectedEOF means partial header read.
		return nil, errTornRecord
	}

	length := binary.LittleEndian.Uint32(header[0:4])
	expectedCRC := binary.LittleEndian.Uint32(header[4:8])

	payload := make([]byte, length)
	_, err = io.ReadFull(r, payload)
	if err != nil {
		// Partial or missing payload.
		return nil, errTornRecord
	}

	actualCRC := crc32.ChecksumIEEE(payload)
	if actualCRC != expectedCRC {
		return nil, errTornRecord
	}

	return payload, nil
}
