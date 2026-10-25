package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func TestMainReaders_DisabledChecksumFields(t *testing.T) {
	for _, continued := range []bool{false, true} {
		for _, branch := range []bool{false, true} {
			t.Run(fmt.Sprintf("continued=%t/branch=%t", continued, branch), func(t *testing.T) {
				const pageSize int64 = 128
				key := []byte("key")
				if continued {
					key = bytes.Repeat([]byte("k"), 256)
				}
				node := NewLeafNode(7)
				if branch {
					node = NewRootNode(7, NewLeafNode(9), NewLeafNode(10), key)
				} else if err := node.InsertEntry(NewEntry(0, key, []byte("value"))); err != nil {
					t.Fatal(err)
				}
				ids := make([]page.ID, NodePageCount(node, pageSize))
				ids[0] = 7
				for i := 1; i < len(ids); i++ {
					ids[i] = ids[0] + page.ID(i)
				}
				images, err := EncodeNodePages(node, pageSize, ids)
				if err != nil {
					t.Fatal(err)
				}
				pages := make(map[page.ID][]byte)
				for i, id := range ids {
					pages[id] = images[i]
				}
				sum := binary.LittleEndian.Uint32(images[0][14:18])
				binary.LittleEndian.PutUint32(images[0][14:18], sum^0x80000000)
				read := func(id page.ID) ([]byte, error) {
					data, ok := pages[id]
					if !ok {
						return nil, ErrInvalid
					}
					return data, nil
				}
				if _, err := DecodeNode(bytes.Clone(images[0]), 7, pageSize, read, true); !errors.Is(err, page.ErrChecksum) {
					t.Fatalf("checked decode: %v", err)
				}
				if _, err := DecodeNode(bytes.Clone(images[0]), 7, pageSize, read, false); err != nil {
					t.Fatalf("unchecked decode: %v", err)
				}
				entry, found, child, err := LookupMappedNode(images[0], 7, key, read, true, false)
				if err != nil || branch && child != 10 || !branch && (!found || !bytes.Equal(entry.Value(), []byte("value"))) {
					t.Fatalf("unchecked lookup: found=%t child=%d err=%v", found, child, err)
				}
				if err := VisitMappedNodeReferences(images[0], 7, pageSize, read, func(page.ID) error { return nil }, nil, false); err != nil {
					t.Fatalf("unchecked references: %v", err)
				}
				if !branch {
					if err := VisitMappedLeafRange(images[0], 7, nil, nil, func(uint32, []byte, []byte) error { return nil }, false, read); err != nil {
						t.Fatal(err)
					}
					if err := VisitMappedLeafReverse(images[0], 7, func(uint32, []byte, []byte) error { return nil }, false, read); err != nil {
						t.Fatal(err)
					}
				}

				// Disabled checksums do not permit a wrong page ID.
				binary.LittleEndian.PutUint64(images[0][2:10], 8)
				if _, err := DecodeNode(images[0], 7, pageSize, read, false); !errors.Is(err, ErrInvalid) {
					t.Fatalf("unchecked wrong ID: %v", err)
				}
			})
		}
	}
}
