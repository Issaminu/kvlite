package page

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrInvalid             = errors.New("invalid database")
	ErrVersionNotSupported = errors.New("database version is not supported by current kvlite version")
	ErrChecksum            = errors.New("checksum error")
)

// Identifier for the page number (Pgid)
type ID uint64

// IDSize is the encoded size of a page ID. An ID is one uint64 value.
const IDSize = 8

func EncodeID(id ID) []byte {
	data := make([]byte, IDSize)
	binary.LittleEndian.PutUint64(data, uint64(id))
	return data
}

func DecodeID(data []byte) (ID, error) {
	if len(data) != IDSize {
		return 0, fmt.Errorf("decode page ID: got %d bytes, want %d: %w", len(data), IDSize, ErrInvalid)
	}
	return ID(binary.LittleEndian.Uint64(data)), nil
}
