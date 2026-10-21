package page

import (
	"encoding/binary"
	"fmt"
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

// errIDSize is the error of DecodeID for data that is not IDSize bytes long.
// A fixed error keeps DecodeID small enough to inline, so a bucket lookup decodes its root page ID without a call.
var errIDSize = fmt.Errorf("decode page ID: the data is not %d bytes: %w", IDSize, ErrInvalid)

func DecodeID(data []byte) (ID, error) {
	if len(data) != IDSize {
		return 0, errIDSize
	}
	return ID(binary.LittleEndian.Uint64(data)), nil
}
