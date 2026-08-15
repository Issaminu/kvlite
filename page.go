package kvlite

import (
	"encoding/binary"
	"fmt"
)

// TODO(component:page): fixed-size page abstraction — page header, page ids, page
// types (meta, freelist, branch, leaf), and reading/writing pages to the file/mmap.

type Pgid uint64
type Txid uint64

func encode[T Pgid | Txid](value T) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(value))
	return buf
}

func decode[T Pgid | Txid](value []byte) (T, error) {
	if len(value) < 8 {
		var zero T
		return zero, fmt.Errorf("insufficient bytes: need 8, got %d", len(value))
	}

	data := T(binary.LittleEndian.Uint64(value))
	return data, nil
}
