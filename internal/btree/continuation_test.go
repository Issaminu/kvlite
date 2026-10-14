package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func TestContinuationNode_Codec(t *testing.T) {
	for _, ids := range [][]page.ID{nil, {42, 91}} {
		node := NewContinuationNode(8, ids, []byte("body"))
		data, err := EncodeContinuationNode(node, 64)
		if err != nil {
			t.Fatal(err)
		}
		if binary.LittleEndian.Uint16(data[:2]) != 3 || binary.LittleEndian.Uint64(data[2:10]) != 8 || binary.LittleEndian.Uint32(data[14:18]) != uint32(len(ids)) {
			t.Fatal("wrong continuation header")
		}
		var h ContinuationHeader
		var encodedIDs, body []byte
		allocations := testing.AllocsPerRun(100, func() { h, encodedIDs, body, err = ReadMappedContinuationNode(data, 8, 64, true) })
		if err != nil || h.Type != NodeTypeContinuation || h.PageID != 8 || int(h.NextPageCount) != len(ids) || len(encodedIDs) != len(ids)*8 || !bytes.Equal(body[:4], []byte("body")) {
			t.Fatalf("mapped continuation: %+v %v", h, err)
		}
		if allocations != 0 {
			t.Fatalf("mapped continuation allocated %v times", allocations)
		}
		if cap(encodedIDs) != len(encodedIDs) || cap(body) != len(body) {
			t.Fatal("mapped fields can grow into other bytes")
		}
		decoded, err := DecodeContinuationNode(data, 8, 64, true)
		if err != nil || decoded.Header() != h || decoded.PageID() != 8 {
			t.Fatalf("decoded continuation: %v", err)
		}
		roundTrip, err := EncodeContinuationNode(decoded, 64)
		if err != nil || !bytes.Equal(roundTrip, data) {
			t.Fatalf("continuation round trip: %v", err)
		}
		decoded.header.PageID = 9
		if len(ids) > 0 {
			decoded.nextPageIDs[0] = 100
			if binary.LittleEndian.Uint64(encodedIDs[:8]) != 42 {
				t.Fatal("decoded IDs changed mapped bytes")
			}
		}
		if binary.LittleEndian.Uint64(data[2:10]) != 8 {
			t.Fatal("decoded header changed mapped bytes")
		}
		damaged := bytes.Clone(data)
		damaged[63] ^= 1
		if _, _, _, err := ReadMappedContinuationNode(damaged, 8, 64, true); !errors.Is(err, page.ErrChecksum) {
			t.Fatalf("mapped checksum: %v", err)
		}
		if _, err := DecodeContinuationNode(damaged, 8, 64, true); !errors.Is(err, page.ErrChecksum) {
			t.Fatalf("decoded checksum: %v", err)
		}
		invalid := bytes.Clone(data)
		binary.LittleEndian.PutUint32(invalid[14:18], ^uint32(0))
		sealContinuationPage(invalid)
		if _, _, _, err := ReadMappedContinuationNode(invalid, 8, 64, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid array with valid CRC: %v", err)
		}
		binary.LittleEndian.PutUint32(invalid[14:18], 1)
		sealContinuationPage(invalid[:26])
		if _, _, _, err := ReadMappedContinuationNode(invalid[:26], 8, 26, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("array leaves no body byte: %v", err)
		}
		binary.LittleEndian.PutUint16(invalid[:2], uint16(NodeTypeBranch))
		sealContinuationPage(invalid)
		if _, _, _, err := ReadMappedContinuationNode(invalid, 8, 64, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("wrong type with valid CRC: %v", err)
		}
		if _, _, _, err := ReadMappedContinuationNode(data[:17], 8, 64, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("short header: %v", err)
		}
		if _, _, _, err := ReadMappedContinuationNode(data, 9, 64, true); !errors.Is(err, ErrInvalid) {
			t.Fatalf("wrong page ID: %v", err)
		}
	}
}

func TestPageIDSet_FindsRepeatedIDs(t *testing.T) {
	for _, test := range []struct {
		ids      []page.ID
		repeated bool
	}{
		{nil, false},
		{[]page.ID{3, 4, 9}, false},
		{[]page.ID{3, 4, 4}, true},
		{[]page.ID{5, 5}, true},
		{[]page.ID{9, 4, 3}, false},
		{[]page.ID{3, 9, 4, 9}, true},
		{[]page.ID{3, 9, 4, 3}, true},
		{[]page.ID{3, 9, 4, 7}, false},
	} {
		set := pageIDSet{}
		firstRepeat := -1
		for index, id := range test.ids {
			if !set.add(id) && firstRepeat < 0 {
				firstRepeat = index
			}
		}
		if (firstRepeat >= 0) != test.repeated || hasRepeatedPageID(test.ids) != test.repeated {
			t.Fatalf("IDs %v: set found repeat at %d, function found %t; want repeated %t", test.ids, firstRepeat, hasRepeatedPageID(test.ids), test.repeated)
		}
		if test.repeated && firstRepeat != len(test.ids)-1 {
			t.Fatalf("IDs %v: set found repeat at %d, want the last ID", test.ids, firstRepeat)
		}
	}
}
