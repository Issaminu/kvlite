package kvlite

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/Issaminu/kvlite/internal/page"
)

const version uint32 = 1 // format version, bumps only when making a breaking change to the DB file format itself

const magic uint32 = 0x7317DC29 // magic string is "KVLT"

// metaPgid is the page id that holds the meta. It is always page 0, the first page
// of the file; node pages start at pgid 1. WAL meta records use this id too.
const metaPgid page.ID = 0

type Meta struct {
	magic    uint32
	version  uint32
	pageSize int64
	pgid     page.ID
	root     page.ID // pgid of the root node
	checksum uint64
}

const (
	// Metadata contains two uint32 fields and four uint64 fields. Its encoded
	// form therefore occupies 40 bytes.
	metaEncodedSize = 40
	// The final metadata field is a uint64 checksum, which occupies eight bytes.
	metaChecksumSize = 8
)

func encodeMeta(meta *Meta) []byte {
	data := make([]byte, 0, metaEncodedSize)
	data = binary.LittleEndian.AppendUint32(data, meta.magic)
	data = binary.LittleEndian.AppendUint32(data, meta.version)
	data = binary.LittleEndian.AppendUint64(data, uint64(meta.pageSize))
	data = binary.LittleEndian.AppendUint64(data, uint64(meta.pgid))
	data = binary.LittleEndian.AppendUint64(data, uint64(meta.root))
	data = binary.LittleEndian.AppendUint64(data, meta.checksum)
	return data
}

func decodeMeta(data []byte) (*Meta, error) {
	if len(data) != metaEncodedSize {
		return nil, fmt.Errorf("decode meta: got %d bytes, want %d: %w", len(data), metaEncodedSize, ErrInvalid)
	}

	// Layout: magic[0:4], version[4:8], pageSize[8:16], pgid[16:24], root[24:32], and checksum[32:40].
	return &Meta{
		magic:    binary.LittleEndian.Uint32(data[0:4]),
		version:  binary.LittleEndian.Uint32(data[4:8]),
		pageSize: int64(binary.LittleEndian.Uint64(data[8:16])),
		pgid:     page.ID(binary.LittleEndian.Uint64(data[16:24])),
		root:     page.ID(binary.LittleEndian.Uint64(data[24:32])),
		checksum: binary.LittleEndian.Uint64(data[32:40]),
	}, nil
}

func NewMeta(pageSize int64) *Meta {
	meta := &Meta{
		magic:    magic,
		version:  version,
		pageSize: pageSize,
		pgid:     1, // Nodes start at pgid 1 (the pgid 0 is occupied by the meta)
		root:     1, // needs to stay in sync with `pgid` field above
	}
	meta.checksum = meta.GenerateChecksum()
	return meta
}

func (m *Meta) Validate() error {
	if m.magic != magic {
		return ErrInvalid
	}

	if m.version != version {
		return ErrVersionNotSupported
	}

	if m.checksum != m.GenerateChecksum() {
		return ErrChecksum
	}
	return nil
}

func (m *Meta) GenerateChecksum() uint64 {
	encoded := encodeMeta(m)
	sealed := encoded[:metaEncodedSize-metaChecksumSize]

	hashFunc := fnv.New64a()
	hashFunc.Write(sealed)
	return hashFunc.Sum64()
}
