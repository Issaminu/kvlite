package kvlite

import (
	"encoding/binary"
	"fmt"
)

// TODO(component:page): fixed-size page abstraction — page header, page ids, page
// types (meta, freelist, branch, leaf), and reading/writing pages to the file/mmap.

type Pgid uint64
type Txid uint64

// A Pgid is a uint64, so every encoded page ID occupies eight bytes.
const pgidEncodedSize = 8
const txidEncodedSize = pgidEncodedSize

func encodePgid(pgid Pgid) []byte {
	buf := make([]byte, pgidEncodedSize)
	binary.LittleEndian.PutUint64(buf, uint64(pgid))
	return buf
}

func decodePgid(data []byte) (Pgid, error) {
	if len(data) != pgidEncodedSize {
		return 0, fmt.Errorf("decode pgid: got %d bytes, want %d: %w", len(data), pgidEncodedSize, ErrInvalid)
	}
	return Pgid(binary.LittleEndian.Uint64(data)), nil
}
