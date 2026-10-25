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
	// MetaSize is the encoded size of four uint32 fields and four uint64 fields.
	MetaSize = 48
	// The final metadata field is one CRC32C checksum.
	metaChecksumSize = 4
)

// metaFlags stores one enabled setting in each bit of the metadata flags field.
type metaFlags uint32

const (
	metaFlagDataPageChecksums metaFlags = 1 << iota
)

type Meta struct {
	magic      uint32
	version    uint32
	pageSize   int64
	pgid       ID
	root       ID // pgid of the root node
	generation uint64
	flags      metaFlags
	checksum   uint32
}

func NewMeta(pageSize int64) *Meta {
	firstNodeID := Meta1ID + 1 + AllocationSegmentPages(pageSize)

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

// AllocationSegmentPages returns the number of bitmap pages stored together in one group.
// A bitmap page uses one bit for each database page that it tracks.
// Keeping bitmap pages together leaves room for a maximum-size node between groups.
// For example, 4 KiB pages need 17 bitmap pages per group.
// Another example, for 16 KiB pages, we would need 2 pages per group.
// It returns zero when pageSize cannot hold a bitmap header and bitmap data.
func AllocationSegmentPages(pageSize int64) ID {
	const (
		bitmapHeaderBytes int64 = 16
		// Bound the value bytes, key bytes, and node header space.
		maximumNodeBytes int64 = (1 << 31) + (1 << 16) + 128
		// Keep the existing layout's allowance for metadata and spare pages.
		extraGroupPages int64 = 130
	)
	if pageSize <= bitmapHeaderBytes {
		return 0
	}

	// Round up: even a partly used final page needs a complete database page.
	maximumNodePages := (maximumNodeBytes + pageSize - 1) / pageSize
	pagesToCover := maximumNodePages + extraGroupPages

	bitmapBytes := pageSize - bitmapHeaderBytes
	pagesCoveredPerBitmap := bitmapBytes * 8
	// Each added bitmap covers more pages but also occupies one page in the group.
	dataPagesAddedPerBitmap := pagesCoveredPerBitmap - 1

	// Round up so the group has enough bitmap pages to cover the complete node.
	bitmapPages := (pagesToCover + dataPagesAddedPerBitmap - 1) / dataPagesAddedPerBitmap
	return ID(bitmapPages)
}

func EncodeMeta(meta *Meta) []byte {
	data := make([]byte, 0, MetaSize)
	data = binary.LittleEndian.AppendUint32(data, meta.magic)
	data = binary.LittleEndian.AppendUint32(data, meta.version)
	data = binary.LittleEndian.AppendUint64(data, uint64(meta.pageSize))
	data = binary.LittleEndian.AppendUint64(data, uint64(meta.pgid))
	data = binary.LittleEndian.AppendUint64(data, uint64(meta.root))
	data = binary.LittleEndian.AppendUint64(data, meta.generation)
	data = binary.LittleEndian.AppendUint32(data, uint32(meta.flags))
	data = binary.LittleEndian.AppendUint32(data, meta.checksum)
	return data
}

func DecodeMeta(data []byte) (*Meta, error) {
	if len(data) != MetaSize {
		return nil, fmt.Errorf("decode meta: got %d bytes, want %d: %w", len(data), MetaSize, ErrInvalid)
	}

	// Layout: magic[0:4], version[4:8], pageSize[8:16], pgid[16:24], root[24:32], generation[32:40], flags[40:44], and checksum[44:48].
	return &Meta{
		magic:      binary.LittleEndian.Uint32(data[0:4]),
		version:    binary.LittleEndian.Uint32(data[4:8]),
		pageSize:   int64(binary.LittleEndian.Uint64(data[8:16])),
		pgid:       ID(binary.LittleEndian.Uint64(data[16:24])),
		root:       ID(binary.LittleEndian.Uint64(data[24:32])),
		generation: binary.LittleEndian.Uint64(data[32:40]),
		flags:      metaFlags(binary.LittleEndian.Uint32(data[40:44])),
		checksum:   binary.LittleEndian.Uint32(data[44:48]),
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

// DataPageChecksums reports whether this file stores and checks data-page checksums.
func (m *Meta) DataPageChecksums() bool {
	return m.flags&metaFlagDataPageChecksums != 0
}

// SetDataPageChecksums selects the data-page checksum mode for a new file.
// RefreshChecksum seals the changed metadata before it is stored.
func (m *Meta) SetDataPageChecksums(enabled bool) {
	m.flags &^= metaFlagDataPageChecksums
	if enabled {
		m.flags |= metaFlagDataPageChecksums
	}
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
	if m.flags&^metaFlagDataPageChecksums != 0 {
		return ErrInvalid
	}
	return nil
}

func (m *Meta) generateChecksum() uint32 {
	encoded := EncodeMeta(m)
	sealed := encoded[:MetaSize-metaChecksumSize]
	return checksum.Sum32(sealed)
}
