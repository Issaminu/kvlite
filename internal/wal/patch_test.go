package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	if _, err := log.Commit([]NodeRecord{{Original: base, Final: target}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	records, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Header.Type != RecordTypeLeafDelete {
		t.Fatalf("WAL records: got %+v, want leaf delete and commit", records)
	}
	if got := records[0].Payload; !bytes.Equal(got, []byte{5, 0, 0, 0, 3, 0, 0, 0, 1, 0, 'b', 1, 0, 'd'}) {
		t.Fatalf("deleted keys: got %v, want b and d", got)
	}
	image, err := ApplyLeafDeletes(btree.EncodeWALNode(base), records[0].Payload, base.PageID())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(image, btree.EncodeWALNode(target)) {
		t.Fatal("leaf delete did not rebuild the final node")
	}
	image, err = ApplyLeafDeletes(btree.EncodeWALNode(target), records[0].Payload, base.PageID())
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
