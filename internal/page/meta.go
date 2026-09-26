package page

import (
	"encoding/binary"
	"fmt"

	"github.com/Issaminu/kvlite/internal/checksum"
)

// FormatVersion identifies the database and WAL format.
const FormatVersion uint32 = 1

const magic uint32 = 0x7317DC29 // The hexadecimal value encodes the KVLite file marker ("KVLT").

const (
	// Meta0ID identifies the first metadata page.
	Meta0ID ID = 0
	// Meta1ID identifies the second metadata page. Allocation pages follow it.
	Meta1ID ID = 1
)

const (
	// MetaSize is the encoded size of three uint32 fields and four uint64 fields.
	MetaSize = 44
	// The final metadata field is one CRC32C checksum.
	metaChecksumSize = 4
)

type Meta struct {
	magic      uint32
	version    uint32
	pageSize   int64
	pgid       ID
	root       ID // pgid of the root node
	generation uint64
	checksum   uint32
}

func NewMeta(pageSize int64) *Meta {
	const firstNodeID ID = Meta1ID + 2

	meta := &Meta{
		magic:    magic,
		version:  FormatVersion,
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
	data = binary.LittleEndian.AppendUint64(data, meta.generation)
	data = binary.LittleEndian.AppendUint32(data, meta.checksum)
	return data
}

func DecodeMeta(data []byte) (*Meta, error) {
	if len(data) != MetaSize {
		return nil, fmt.Errorf("decode meta: got %d bytes, want %d: %w", len(data), MetaSize, ErrInvalid)
	}

	// Layout: magic[0:4], version[4:8], pageSize[8:16], pgid[16:24], root[24:32], generation[32:40], and checksum[40:44].
	return &Meta{
		magic:      binary.LittleEndian.Uint32(data[0:4]),
		version:    binary.LittleEndian.Uint32(data[4:8]),
		pageSize:   int64(binary.LittleEndian.Uint64(data[8:16])),
		pgid:       ID(binary.LittleEndian.Uint64(data[16:24])),
		root:       ID(binary.LittleEndian.Uint64(data[24:32])),
		generation: binary.LittleEndian.Uint64(data[32:40]),
		checksum:   binary.LittleEndian.Uint32(data[40:44]),
	}, nil
}

func IsMetaID(id ID) bool {
	return id == Meta0ID || id == Meta1ID
}

func (m *Meta) PageSize() int64 {
	return m.pageSize
}

func (m *Meta) Root() ID {
	return m.root
}

func (m *Meta) Generation() uint64 {
	return m.generation
}

func (m *Meta) AdvanceGeneration() {
	m.generation++
}

func (m *Meta) SetRoot(root ID) {
	m.root = root
}

// LastPage returns the highest page ID that can contain stored data.
func (m *Meta) LastPage() ID {
	return m.pgid
}

// SetLastPage changes the highest page ID after KVLite releases a free suffix.
func (m *Meta) SetLastPage(pageID ID) {
	m.pgid = pageID
}

func (m *Meta) RefreshChecksum() {
	m.checksum = m.generateChecksum()
}

func (m *Meta) Validate() error {
	if m.magic != magic {
		return ErrInvalid
	}
	if m.version != FormatVersion {
		return ErrVersionMismatch
	}
	if m.checksum != m.generateChecksum() {
		return ErrChecksum
	}
	return nil
}

func (m *Meta) generateChecksum() uint32 {
	encoded := EncodeMeta(m)
	sealed := encoded[:MetaSize-metaChecksumSize]
	return checksum.Sum32(sealed)
}
