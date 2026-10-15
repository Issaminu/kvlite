package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

func makePagePatch(base, target []byte) []byte {
	if len(base) != len(target) {
		panic("page patch test images have different sizes")
	}
	var patch PagePatch
	patch.reset()
	patch.appendChangedRanges(0, base, target)
	return patch.appendEncoded(nil)
}

func TestPagePatch_RoundTripsChangedRanges(t *testing.T) {
	base := []byte("0123456789abcdefghijkl")
	target := bytes.Clone(base)
	copy(target[4:6], "AB")
	copy(target[14:17], "XYZ")
	patch := makePagePatch(base, target)

	got, err := ApplyPagePatch(base, patch, 64)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, target) {
		t.Fatalf("patched page: got %q, want %q", got, target)
	}
}

func TestPagePatch_ReplayChainRestoresLatestImage(t *testing.T) {
	base := []byte("0123456789")
	first := []byte("01AA456789")
	latest := []byte("01AA45ZZ89")
	firstPatch := makePagePatch(base, first)
	latestPatch := makePagePatch(first, latest)

	for _, start := range [][]byte{base, latest} {
		got, err := ApplyPagePatch(start, firstPatch, 64)
		if err != nil {
			t.Fatal(err)
		}
		got, err = ApplyPagePatch(got, latestPatch, 64)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, latest) {
			t.Fatalf("replayed page from %q: got %q, want %q", start, got, latest)
		}
	}
}

func TestApplyPagePatch_RejectsInvalidRange(t *testing.T) {
	patch := makePagePatch([]byte("base"), []byte("targ"))
	binary.LittleEndian.PutUint32(patch[4:8], ^uint32(0))

	if _, err := ApplyPagePatch([]byte("base"), patch, 64); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("invalid range error: got %v, want ErrInvalid", err)
	}
}

func TestApplyPagePatch_RejectsIncompleteRangeHeader(t *testing.T) {
	patch := append(makePagePatch([]byte("base"), []byte("targ")), 0)

	if _, err := ApplyPagePatch([]byte("base"), patch, 64); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("incomplete range header error: got %v, want ErrInvalid", err)
	}
}

func TestApplyPagePatch_RejectsBaseLargerThanPage(t *testing.T) {
	patch := makePagePatch([]byte("base"), []byte("targ"))

	if _, err := ApplyPagePatch([]byte("base"), patch, 3); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("large base error: got %v, want ErrInvalid", err)
	}
}

func TestWALCommit_EncodesLeafDeletes(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	base := btree.NewLeafNode(2)
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		if err := base.InsertEntry(btree.NewEntry(0, []byte(key), bytes.Repeat([]byte(key), 16))); err != nil {
			t.Fatal(err)
		}
	}
	target := base.Clone()
	for _, key := range []string{"b", "d"} {
		if err := target.DeleteKeyIfPresent([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	log := New(Config{File: file, PageSize: 256, CheckpointThresholdBytes: 1 << 20})
	if _, err := log.Commit([]NodeRecord{{Final: base}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Commit([]NodeRecord{{Original: base, Final: target}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	records, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 || records[0].Header.Type != RecordTypeNode || records[2].Header.Type != RecordTypeLeafDelete {
		t.Fatalf("WAL records: got %+v, want a complete image followed by a leaf delete", records)
	}
	if got := records[2].Payload; !bytes.Equal(got, []byte{5, 0, 0, 0, 3, 0, 0, 0, 1, 0, 'b', 1, 0, 'd'}) {
		t.Fatalf("deleted keys: got %v, want b and d", got)
	}
	image, err := ApplyLeafDeletes(btree.EncodeWALNode(base), records[2].Payload, base.PageID())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(image, btree.EncodeWALNode(target)) {
		t.Fatal("leaf delete did not rebuild the final node")
	}
	image, err = ApplyLeafDeletes(btree.EncodeWALNode(target), records[2].Payload, base.PageID())
	if err != nil || !bytes.Equal(image, btree.EncodeWALNode(target)) {
		t.Fatalf("repeated leaf delete changed final node: error %v", err)
	}
}

func TestApplyLeafDeletes_RejectsInvalidKeys(t *testing.T) {
	base := btree.NewLeafNode(2)
	for _, key := range []string{"a", "b", "c"} {
		if err := base.InsertEntry(btree.NewEntry(0, []byte(key), []byte("value"))); err != nil {
			t.Fatal(err)
		}
	}
	image := btree.EncodeWALNode(base)
	for name, keys := range map[string][]byte{
		"empty":     nil,
		"short":     {1},
		"duplicate": {3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 'a', 1, 0, 'a'},
		"reverse":   {3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 'b', 1, 0, 'a'},
		"short-key": {3, 0, 0, 0, 2, 0, 0, 0, 4, 0, 'a'},
		"empty-key": {3, 0, 0, 0, 2, 0, 0, 0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ApplyLeafDeletes(image, keys, base.PageID()); !errors.Is(err, page.ErrInvalid) {
				t.Fatalf("invalid keys: got %v, want invalid page", err)
			}
		})
	}
	branch := btree.NewRootNode(2, btree.NewLeafNode(3), btree.NewLeafNode(4), []byte("b"))
	if _, err := ApplyLeafDeletes(btree.EncodeWALNode(branch), []byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 'a'}, branch.PageID()); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("branch delete: got %v, want invalid page", err)
	}
}

// TestWALCommit_LargeNodeToSinglePage covers the full WAL write and read path.
// The original is a decoded large node. Recovery needs only the final image.
func TestWALCommit_LargeNodeToSinglePage(t *testing.T) {
	const pageSize int64 = 128
	path := t.TempDir() + "/wal"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	base := btree.NewLeafNode(2)
	for _, key := range []string{"a", "b", "c"} {
		size := 16
		if key == "b" {
			size = 256
		}
		if err := base.InsertEntry(btree.NewEntry(0, []byte(key), bytes.Repeat([]byte(key), size))); err != nil {
			t.Fatal(err)
		}
	}
	base.SetOverflowPages([]page.ID{7, 9})
	target := base.Clone()
	target.SetOverflowPages(nil)
	if err := target.DeleteKeyIfPresent([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if int64(base.EncodedSize()) <= pageSize || int64(target.EncodedSize()) > pageSize {
		t.Fatal("test must shrink a large node to one page")
	}
	config := Config{File: file, PageSize: pageSize, SyncOnCommit: true, SyncOnCheckpoint: true, CheckpointThresholdBytes: 1 << 20}
	log := New(config)
	if _, err := log.Commit([]NodeRecord{{Original: base, Final: target}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	config.File = file
	log = New(config)
	records, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Header.Type != RecordTypeNode || records[1].Header.Type != RecordTypeCommit || records[0].Header.TxID != records[1].Header.TxID {
		t.Fatalf("WAL records: got %+v, want a complete node and its commit", records)
	}
	if !bytes.Equal(records[0].Payload, btree.EncodeWALNode(target)) {
		t.Fatal("WAL did not store the complete final node")
	}
	log.LoadCommittedRecord(records[0])
	mainFile, err := os.CreateTemp(t.TempDir(), "main")
	if err != nil {
		t.Fatal(err)
	}
	defer mainFile.Close()
	if err := log.Checkpoint(mainFile, 3*pageSize, true); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, pageSize)
	if _, err := mainFile.ReadAt(data, int64(target.PageID())*pageSize); err != nil {
		t.Fatal(err)
	}
	decoded, err := btree.DecodeNode(data, target.PageID(), pageSize, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(btree.EncodeWALNode(decoded), btree.EncodeWALNode(target)) {
		t.Fatal("checkpoint did not preserve the final node")
	}
	if records, err := log.ReadRecords(); err != nil || len(records) != 0 {
		t.Fatalf("WAL after checkpoint: got %d records, error %v", len(records), err)
	}
}

// referencePagePatchRanges is the original byte-by-byte range builder.
// The word-based builder must produce the same ranges.
func referencePagePatchRanges(patch []byte, baseOffset int, base, target []byte) []byte {
	for index := 0; index < len(target); {
		for index < len(target) && index < len(base) && base[index] == target[index] {
			index++
		}
		if index == len(target) {
			break
		}
		start := index
		for index < len(target) {
			if index >= len(base) || base[index] != target[index] {
				index++
				continue
			}
			equalStart := index
			for index < len(target) && index < len(base) && base[index] == target[index] {
				index++
			}
			if index-equalStart > pagePatchRangeHeaderSize {
				index = equalStart
				break
			}
		}
		patch = binary.LittleEndian.AppendUint32(patch, uint32(baseOffset+start))
		patch = binary.LittleEndian.AppendUint32(patch, uint32(index-start))
		patch = append(patch, target[start:index]...)
	}
	return patch
}

func TestAppendChangedPagePatchRanges_MatchesByteScan(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	for trial := range 20000 {
		size := random.IntN(80)
		if trial%10 == 0 {
			size = random.IntN(4096)
		}
		base := make([]byte, size)
		for index := range base {
			base[index] = byte(random.IntN(4))
		}
		target := bytes.Clone(base)
		// Change runs of random length so that equal sections of every length near the header size occur.
		for index := 0; index < size; {
			index += random.IntN(20)
			length := 1 + random.IntN(12)
			for at := index; at < min(size, index+length); at++ {
				target[at] ^= byte(1 + random.IntN(255))
			}
			index += length
		}
		shortBase := base
		if trial%7 == 0 && size > 0 {
			shortBase = base[:random.IntN(size)]
		}
		offset := random.IntN(1000)
		want := referencePagePatchRanges(nil, offset, shortBase, target)
		got := appendChangedPagePatchRanges(nil, offset, shortBase, target)
		if !bytes.Equal(got, want) {
			t.Fatalf("trial %d: size %d, base size %d: got %x, want %x", trial, size, len(shortBase), got, want)
		}
	}
}

// BenchmarkAppendChangedPagePatchRanges measures range building for one changed value field.
func BenchmarkAppendChangedPagePatchRanges(b *testing.B) {
	cases := []struct {
		name   string
		size   int
		change func([]byte)
	}{
		{"all-changed/bytes=128", 128, func(value []byte) {
			for index := range value {
				value[index] ^= 0x5a
			}
		}},
		{"all-changed/bytes=4000", 4000, func(value []byte) {
			for index := range value {
				value[index] ^= 0x5a
			}
		}},
		{"one-word-changed/bytes=4000", 4000, func(value []byte) { value[2000] ^= 1 }},
		{"every-16th-byte/bytes=4000", 4000, func(value []byte) {
			for index := 0; index < len(value); index += 16 {
				value[index] ^= 1
			}
		}},
	}
	for _, test := range cases {
		b.Run(test.name, func(b *testing.B) {
			base := bytes.Repeat([]byte{7}, test.size)
			target := bytes.Clone(base)
			test.change(target)
			patch := make([]byte, 0, 2*test.size+64)
			b.SetBytes(int64(test.size))
			b.ReportAllocs()
			for b.Loop() {
				patch = appendChangedPagePatchRanges(patch[:0], 64, base, target)
			}
		})
	}
}
