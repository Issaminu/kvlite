package page

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestIDCodec_UsesExactlyEightLittleEndianBytes(t *testing.T) {
	// The byte pattern makes the uint64 byte order visible in the assertion.
	const id ID = 0x0102030405060708
	want := []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}

	encoded := EncodeID(id)
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encode page ID: got %x, want %x", encoded, want)
	}

	decoded, err := DecodeID(encoded)
	if err != nil {
		t.Fatalf("decode page ID: %v", err)
	}
	if decoded != id {
		t.Fatalf("decode page ID: got %x, want %x", decoded, id)
	}

	// A page ID is a uint64, so its encoding must contain exactly eight bytes.
	for _, size := range []int{7, 9} {
		if _, err := DecodeID(make([]byte, size)); !errors.Is(err, ErrInvalid) {
			t.Errorf("decode %d bytes: got %v, want ErrInvalid", size, err)
		}
	}
}

func TestMetaCodec_UsesFixedLittleEndianLayout(t *testing.T) {
	// Distinct field values make the order and width of every field visible.
	meta := &Meta{magic: 1, version: 2, pageSize: 3, pgid: 4, root: 5, checksum: 6}
	want := []byte{
		1, 0, 0, 0,
		2, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
		4, 0, 0, 0, 0, 0, 0, 0,
		5, 0, 0, 0, 0, 0, 0, 0,
		6, 0, 0, 0,
	}

	encoded := EncodeMeta(meta)
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encode meta: got %x, want %x", encoded, want)
	}

	decoded, err := DecodeMeta(encoded)
	if err != nil {
		t.Fatalf("decode meta: %v", err)
	}
	if *decoded != *meta {
		t.Fatalf("decode meta: got %+v, want %+v", decoded, meta)
	}

	// The six metadata fields occupy exactly 36 bytes.
	for _, size := range []int{35, 37} {
		if _, err := DecodeMeta(make([]byte, size)); !errors.Is(err, ErrInvalid) {
			t.Errorf("decode %d bytes: got %v, want ErrInvalid", size, err)
		}
	}
}

func TestMetaChecksum_UsesCRC32C(t *testing.T) {
	meta := NewMeta(4096)
	encoded := EncodeMeta(meta)

	want := crc32.Checksum(encoded[:MetaSize-metaChecksumSize], crc32.MakeTable(crc32.Castagnoli))
	if got := binary.LittleEndian.Uint32(encoded[MetaSize-metaChecksumSize:]); got != want {
		t.Fatalf("metadata checksum: got %x, want CRC32C %x", got, want)
	}
}

func TestMeta_TracksRootAndAllocatedPages(t *testing.T) {
	const pageSize int64 = 4096 // The page size does not affect page ID allocation.
	const (
		initialRoot ID = 1 // Page zero holds metadata, so the first node uses page one.
		nextPage    ID = 2 // The next allocation follows the initial root page.
	)

	meta := NewMeta(pageSize)
	if got := meta.PageSize(); got != pageSize {
		t.Fatalf("page size: got %d, want %d", got, pageSize)
	}
	if got := meta.Root(); got != initialRoot {
		t.Fatalf("initial root: got %d, want %d", got, initialRoot)
	}
	if got := meta.Allocate(); got != nextPage {
		t.Fatalf("allocated page: got %d, want %d", got, nextPage)
	}

	meta.SetRoot(nextPage)
	if got := meta.Root(); got != nextPage {
		t.Fatalf("replacement root: got %d, want %d", got, nextPage)
	}
}
