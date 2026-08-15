package kvlite

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
)

const version uint32 = 1 // format version, bumps only when making a breaking change to the DB file format itself

const magic uint32 = 0x7317DC29 // magic string is "KVLT"

// metaPgid is the page id that holds the meta. It is always page 0, the first page
// of the file; node pages start at pgid 1. WAL meta records use this id too.
const metaPgid Pgid = 0

type Meta struct {
	magic    uint32
	version  uint32
	pageSize int64
	pgid     Pgid
	root     Pgid // pgid of the root node
	checksum uint64
}

func NewMeta() *Meta {
	meta := &Meta{
		magic:    magic,
		version:  version,
		pageSize: int64(os.Getpagesize()),
		pgid:     1, // Nodes start at pgid 1 (the pgid 0 is occupied by the meta)
		root:     1, // needs to stay in sync with `pgid` field above
	}
	meta.checksum = meta.GenerateChecksum()
	return meta
}

func readMeta(r io.Reader) (*Meta, error) {
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return nil, ErrInvalid
	}

	var version uint32
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return nil, ErrInvalid
	}

	var pageSize int64
	if err := binary.Read(r, binary.LittleEndian, &pageSize); err != nil {
		return nil, ErrInvalid
	}

	var pgid Pgid
	if err := binary.Read(r, binary.LittleEndian, &pgid); err != nil {
		return nil, ErrInvalid
	}

	var root Pgid
	if err := binary.Read(r, binary.LittleEndian, &root); err != nil {
		return nil, ErrInvalid
	}

	var checksum uint64
	if err := binary.Read(r, binary.LittleEndian, &checksum); err != nil {
		return nil, ErrInvalid
	}

	meta := &Meta{
		magic, version, pageSize, pgid, root, checksum,
	}
	return meta, nil
}

func writeMeta(w io.Writer, meta *Meta) error {
	buf := new(bytes.Buffer)
	meta.encode(buf)

	if _, err := w.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}

	return nil
}

func (meta *Meta) encode(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.LittleEndian, meta.magic); err != nil {
		return fmt.Errorf("write meta magic: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, meta.version); err != nil {
		return fmt.Errorf("write meta version: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, meta.pageSize); err != nil {
		return fmt.Errorf("write meta pageSize: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, meta.pgid); err != nil {
		return fmt.Errorf("write meta pgid: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, meta.root); err != nil {
		return fmt.Errorf("write meta root: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, meta.checksum); err != nil {
		return fmt.Errorf("write meta checksum: %w", err)
	}

	return nil
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
	buf := new(bytes.Buffer)
	_ = m.encode(buf) // encode writes to a bytes.Buffer, which never fails

	// The checksum is the last field: an 8-byte uint64. we hash everything before it
	encoded := buf.Bytes()
	sealed := encoded[:len(encoded)-8]

	hashFunc := fnv.New64a()
	hashFunc.Write(sealed)
	return hashFunc.Sum64()
}
