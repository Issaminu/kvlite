package page

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

const version uint32 = 1        // format version, bumps only when making a breaking change to the DB file format itself
const magic uint32 = 0x7317DC29 // The hexadecimal value encodes the KVLite file marker ("KVLT").

// MetaID identifies the metadata page. Node pages start after page zero.
const MetaID ID = 0

const (
	// MetaSize is the encoded size of two uint32 fields and four uint64 fields.
	MetaSize = 40
	// The final metadata field is one uint64 checksum.
	metaChecksumSize = 8
)

type Meta struct {
	magic    uint32
	version  uint32
	pageSize int64
	pgid     ID
	root     ID // pgid of the root node
	checksum uint64
}

func NewMeta(pageSize int64) *Meta {
	const firstNodeID ID = MetaID + 1

	meta := &Meta{
		magic:    magic,
		version:  version,
		pageSize: pageSize,
		pgid:     firstNodeID,
		root:     firstNodeID,
	}
	meta.RefreshChecksum()
	return meta
}

func EncodeMeta(meta *Meta) []byte {
	data := make([]byte, 0, MetaSize)
	data = binary.LittleEndian.AppendUint32(data, meta.magic)
	data = binary.LittleEndian.AppendUint32(data, meta.version)
	data = binary.LittleEndian.AppendUint64(data, uint64(meta.pageSize))
	data = binary.LittleEndian.AppendUint64(data, uint64(meta.pgid))
	data = binary.LittleEndian.AppendUint64(data, uint64(meta.root))
	data = binary.LittleEndian.AppendUint64(data, meta.checksum)
	return data
}

func DecodeMeta(data []byte) (*Meta, error) {
	if len(data) != MetaSize {
		return nil, fmt.Errorf("decode meta: got %d bytes, want %d: %w", len(data), MetaSize, ErrInvalid)
	}

	// Layout: magic[0:4], version[4:8], pageSize[8:16], pgid[16:24], root[24:32], and checksum[32:40].
	return &Meta{
		magic:    binary.LittleEndian.Uint32(data[0:4]),
		version:  binary.LittleEndian.Uint32(data[4:8]),
		pageSize: int64(binary.LittleEndian.Uint64(data[8:16])),
		pgid:     ID(binary.LittleEndian.Uint64(data[16:24])),
		root:     ID(binary.LittleEndian.Uint64(data[24:32])),
		checksum: binary.LittleEndian.Uint64(data[32:40]),
	}, nil
}

func (m *Meta) PageSize() int64 {
	return m.pageSize
}

func (m *Meta) Root() ID {
	return m.root
}

func (m *Meta) SetRoot(root ID) {
	m.root = root
}

func (m *Meta) Allocate() ID {
	m.pgid++
	return m.pgid
}

func (m *Meta) RefreshChecksum() {
	m.checksum = m.generateChecksum()
}

func (m *Meta) Validate() error {
	if m.magic != magic {
		return ErrInvalid
	}
	if m.version != version {
		return ErrVersionNotSupported
	}
	if m.checksum != m.generateChecksum() {
		return ErrChecksum
	}
	return nil
}

func (m *Meta) generateChecksum() uint64 {
	encoded := EncodeMeta(m)
	sealed := encoded[:MetaSize-metaChecksumSize]

	hashFunc := fnv.New64a()
	hashFunc.Write(sealed)
	return hashFunc.Sum64()
}
