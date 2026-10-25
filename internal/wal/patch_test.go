package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

func makeBytePatch(base, target []byte) []byte {
	if len(base) != len(target) {
		panic("byte patch test images have different sizes")
	}
	return appendChangedBytePatchRanges(nil, 0, base, target)
}

func TestBytePatch_RoundTripsChangedRanges(t *testing.T) {
	base := []byte("0123456789abcdefghijkl")
	target := bytes.Clone(base)
	copy(target[4:6], "AB")
	copy(target[14:17], "XYZ")
	patch := makeBytePatch(base, target)

	got, err := ApplyBytePatch(base, patch, 64)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, target) {
		t.Fatalf("patched page: got %q, want %q", got, target)
	}
}

func TestBytePatch_ReplayChainRestoresLatestImage(t *testing.T) {
	base := []byte("0123456789")
	first := []byte("01AA456789")
	latest := []byte("01AA45ZZ89")
	firstPatch := makeBytePatch(base, first)
	latestPatch := makeBytePatch(first, latest)

	for _, start := range [][]byte{base, latest} {
		got, err := ApplyBytePatch(start, firstPatch, 64)
		if err != nil {
			t.Fatal(err)
		}
		got, err = ApplyBytePatch(got, latestPatch, 64)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, latest) {
			t.Fatalf("replayed page from %q: got %q, want %q", start, got, latest)
		}
	}
}

func TestApplyBytePatch_RejectsInvalidRange(t *testing.T) {
	patch := makeBytePatch([]byte("base"), []byte("targ"))
	binary.LittleEndian.PutUint32(patch[4:8], ^uint32(0))

	if _, err := ApplyBytePatch([]byte("base"), patch, 64); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("invalid range error: got %v, want ErrInvalid", err)
	}
}

func TestApplyBytePatch_RejectsIncompleteRangeHeader(t *testing.T) {
	patch := append(makeBytePatch([]byte("base"), []byte("targ")), 0)

	if _, err := ApplyBytePatch([]byte("base"), patch, 64); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("incomplete range header error: got %v, want ErrInvalid", err)
	}
}

func TestApplyBytePatch_RejectsBaseLargerThanPage(t *testing.T) {
	patch := makeBytePatch([]byte("base"), []byte("targ"))

	if _, err := ApplyBytePatch([]byte("base"), patch, 3); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("large base error: got %v, want ErrInvalid", err)
	}
}

// leafFromState builds one sorted leaf from a key-to-value state.
// A value of nine bytes that starts with 'B' becomes a nested bucket entry with the other eight bytes as its root ID.
func leafFromState(t *testing.T, id page.ID, state map[string]string) *btree.Node {
	t.Helper()
	node := btree.NewLeafNode(id)
	for key, value := range state {
		flags := uint32(0)
		if len(value) == page.IDSize+1 && value[0] == 'B' {
			flags = btree.BucketLeafFlag
			value = value[1:]
		}
		if err := node.InsertEntry(btree.NewEntry(flags, []byte(key), []byte(value))); err != nil {
			t.Fatal(err)
		}
	}
	return node
}

// branchFromState builds one branch with first child 100 and one right child for each separator key.
func branchFromState(id page.ID, state map[string]page.ID) *btree.Node {
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	node := btree.NewRootNode(id, btree.NewLeafNode(100), btree.NewLeafNode(state[keys[0]]), []byte(keys[0]))
	for _, key := range keys[1:] {
		if err := node.InsertOrReplaceSeparator([]byte(key), state[key]); err != nil {
			panic(err)
		}
	}
	return node
}

func mustEntryPatch(t *testing.T, base, target *btree.Node) []byte {
	t.Helper()
	patch, smaller := appendEntryPatch(nil, base, target, 1<<30)
	if !smaller {
		t.Fatal("entry patch was not created")
	}
	return patch
}

// replayEntryPatches applies every patch, in order, to a decoded copy of start and returns the compact result.
func replayEntryPatches(t *testing.T, start *btree.Node, patches [][]byte) []byte {
	t.Helper()
	node, err := btree.DecodeWALNode(btree.EncodeWALNode(start), start.PageID(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, patch := range patches {
		if err := ApplyEntryPatch(node, patch); err != nil {
			t.Fatal(err)
		}
	}
	return btree.EncodeWALNode(node)
}

func TestEntryPatch_LeafSetsAndDeletesByKey(t *testing.T) {
	base := leafFromState(t, 2, map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"})
	target := leafFromState(t, 2, map[string]string{"a": "1", "b": "two", "bb": "new", "d": "4"})
	patch := mustEntryPatch(t, base, target)

	want := appendEntryPatchSet(nil, 0, []byte("b"), []byte("two"))
	want = appendEntryPatchSet(want, 0, []byte("bb"), []byte("new"))
	want = appendEntryPatchDelete(want, []byte("c"))
	if !bytes.Equal(patch, want) {
		t.Fatalf("leaf entry patch: got %v, want %v", patch, want)
	}
	if got := replayEntryPatches(t, base, [][]byte{patch}); !bytes.Equal(got, btree.EncodeWALNode(target)) {
		t.Fatal("leaf entry patch did not rebuild the final leaf")
	}
}

func TestEntryPatch_BranchSetsAndDeletesSeparators(t *testing.T) {
	base := branchFromState(2, map[string]page.ID{"d": 10, "h": 11, "m": 12})
	target := branchFromState(2, map[string]page.ID{"d": 10, "f": 13, "h": 14})
	patch := mustEntryPatch(t, base, target)
	if got := replayEntryPatches(t, base, [][]byte{patch}); !bytes.Equal(got, btree.EncodeWALNode(target)) {
		t.Fatal("branch entry patch did not rebuild the final branch")
	}

	changedFirst := base.Clone()
	changedFirst.Children[0] = 99
	if _, smaller := appendEntryPatch(nil, base, changedFirst, 1<<30); smaller {
		t.Fatal("branch entry patch changed the first child")
	}
	if _, smaller := appendEntryPatch(nil, base, leafFromState(t, 2, map[string]string{"d": "1"}), 1<<30); smaller {
		t.Fatal("entry patch changed a branch into a leaf")
	}
}

// TestEntryPatch_RandomChainsRebuildFinalNode replays chains of random changes from their first state.
// The changes insert, replace, and delete entries, and they replace values with nested buckets and the reverse.
func TestEntryPatch_RandomChainsRebuildFinalNode(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5))
	for trial := range 200 {
		leafState := map[string]string{}
		branchState := map[string]page.ID{"k10": 10}
		leaves := []*btree.Node{leafFromState(t, 2, leafState)}
		branches := []*btree.Node{branchFromState(3, branchState)}
		for range 12 {
			for range 1 + rng.IntN(4) {
				key := fmt.Sprintf("k%02d", rng.IntN(20))
				switch rng.IntN(4) {
				case 0:
					delete(leafState, key)
					if len(branchState) > 1 {
						delete(branchState, key)
					}
				case 1:
					var root [page.IDSize]byte
					binary.LittleEndian.PutUint64(root[:], rng.Uint64())
					leafState[key] = "B" + string(root[:])
					branchState[key] = page.ID(10 + rng.IntN(50))
				default:
					leafState[key] = string(bytes.Repeat([]byte{byte('a' + rng.IntN(26))}, rng.IntN(12)))
					branchState[key] = page.ID(10 + rng.IntN(50))
				}
			}
			leaves = append(leaves, leafFromState(t, 2, leafState))
			branches = append(branches, branchFromState(3, branchState))
		}
		for _, states := range [][]*btree.Node{leaves, branches} {
			patches := make([][]byte, 0, len(states)-1)
			for index := 1; index < len(states); index++ {
				patches = append(patches, mustEntryPatch(t, states[index-1], states[index]))
			}
			if got := replayEntryPatches(t, states[0], patches); !bytes.Equal(got, btree.EncodeWALNode(states[len(states)-1])) {
				t.Fatalf("trial %d: replay did not give the final node", trial)
			}
		}
	}
}

func TestApplyEntryPatch_RejectsInvalidOperations(t *testing.T) {
	leaf := leafFromState(t, 2, map[string]string{"a": "value"})
	branch := branchFromState(3, map[string]page.ID{"m": 10})
	var metaChild [page.IDSize]byte
	for name, test := range map[string]struct {
		node  *btree.Node
		patch []byte
	}{
		"unknown operation": {leaf, []byte{9, 1, 0, 'a'}},
		"short header":      {leaf, []byte{entryPatchDelete, 1}},
		"empty key":         {leaf, []byte{entryPatchDelete, 0, 0}},
		"short key":         {leaf, []byte{entryPatchDelete, 4, 0, 'a'}},
		"key order":         {leaf, appendEntryPatchDelete(appendEntryPatchDelete(nil, []byte("b")), []byte("a"))},
		"duplicate key":     {leaf, appendEntryPatchDelete(appendEntryPatchDelete(nil, []byte("a")), []byte("a"))},
		"missing leaf key":  {leaf, appendEntryPatchDelete(nil, []byte("b"))},
		"short value":       {leaf, appendEntryPatchSet(nil, 0, []byte("a"), []byte("value"))[:12]},
		"leaf flags":        {leaf, appendEntryPatchSet(nil, 2, []byte("a"), []byte("value"))},
		"bucket value size": {leaf, appendEntryPatchSet(nil, btree.BucketLeafFlag, []byte("a"), []byte("short"))},
		"branch flags":      {branch, appendEntryPatchSet(nil, 1, []byte("m"), make([]byte, page.IDSize))},
		"branch child size": {branch, appendEntryPatchSet(nil, 0, []byte("m"), []byte("short"))},
		"branch meta child": {branch, appendEntryPatchSet(nil, 0, []byte("m"), metaChild[:])},
		"missing separator": {branch, appendEntryPatchDelete(nil, []byte("n"))},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ApplyEntryPatch(test.node.Clone(), test.patch); !errors.Is(err, page.ErrInvalid) {
				t.Fatalf("invalid entry patch: got %v, want ErrInvalid", err)
			}
		})
	}
}

func TestWALCommit_EncodesLeafDeletesAsEntryPatch(t *testing.T) {
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
		if err := target.DeleteKey([]byte(key)); err != nil {
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
	if len(records) != 4 || records[0].Header.Type != RecordTypeNode || records[2].Header.Type != RecordTypeEntryPatch {
		t.Fatalf("WAL records: got %+v, want a complete image followed by an entry patch", records)
	}
	if got := records[2].Payload; !bytes.Equal(got, []byte{entryPatchDelete, 1, 0, 'b', entryPatchDelete, 1, 0, 'd'}) {
		t.Fatalf("deleted keys: got %v, want b and d", got)
	}
	if got := replayEntryPatches(t, base, [][]byte{records[2].Payload}); !bytes.Equal(got, btree.EncodeWALNode(target)) {
		t.Fatal("entry patch did not rebuild the final leaf")
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
	setNativeSpan(base, pageSize)
	target := base.Clone()
	target.SetStoragePageCount(1)
	if err := target.DeleteKey([]byte("b")); err != nil {
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

// referenceBytePatchRanges is the original byte-by-byte range builder.
// The word-based builder must produce the same ranges.
func referenceBytePatchRanges(patch []byte, baseOffset int, base, target []byte) []byte {
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
			if index-equalStart > bytePatchRangeHeaderSize {
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

func TestAppendChangedBytePatchRanges_MatchesByteScan(t *testing.T) {
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
		want := referenceBytePatchRanges(nil, offset, shortBase, target)
		got := appendChangedBytePatchRanges(nil, offset, shortBase, target)
		if !bytes.Equal(got, want) {
			t.Fatalf("trial %d: size %d, base size %d: got %x, want %x", trial, size, len(shortBase), got, want)
		}
	}
}

// BenchmarkAppendChangedBytePatchRanges measures range building for one changed value field.
// BenchmarkAppendEntryPatch measures the entry patch of a node that gets one new entry, as after an insert or a child split.
// Most of its time is the merge over the entries that did not change.
func BenchmarkAppendEntryPatch(b *testing.B) {
	key := func(index int) []byte { return fmt.Appendf(nil, "key-%011d", index) }
	run := func(b *testing.B, base, target *btree.Node) {
		patch := make([]byte, 0, 4096)
		b.ReportAllocs()
		for b.Loop() {
			var smaller bool
			if patch, smaller = appendEntryPatch(patch[:0], base, target, 1<<30); !smaller || len(patch) == 0 {
				b.Fatal("no entry patch")
			}
		}
	}
	b.Run("branch/separators=150", func(b *testing.B) {
		base := btree.NewRootNode(2, btree.NewLeafNode(100), btree.NewLeafNode(1000), key(0))
		for index := 1; index < 150; index++ {
			if err := base.InsertOrReplaceSeparator(key(index*2), page.ID(1000+index)); err != nil {
				b.Fatal(err)
			}
		}
		target := base.Clone()
		if err := target.InsertOrReplaceSeparator(key(151), 5000); err != nil {
			b.Fatal(err)
		}
		run(b, base, target)
	})
	b.Run("leaf/entries=100", func(b *testing.B) {
		base := btree.NewLeafNode(2)
		for index := range 100 {
			if err := base.InsertEntry(btree.NewEntry(0, key(index*2), bytes.Repeat([]byte{byte(index)}, 32))); err != nil {
				b.Fatal(err)
			}
		}
		target := base.Clone()
		if err := target.InsertEntry(btree.NewEntry(0, key(101), bytes.Repeat([]byte{0xff}, 32))); err != nil {
			b.Fatal(err)
		}
		run(b, base, target)
	})
}

// BenchmarkAppendLeafBytePatch measures the scan of a leaf with 100 entries in which one value changed, as in a typical update.
// Most of its time is the loop over unchanged entries.
func BenchmarkAppendLeafBytePatch(b *testing.B) {
	base := btree.NewLeafNode(3)
	for index := range 100 {
		if err := base.InsertEntry(btree.NewEntry(0, fmt.Appendf(nil, "key-%011d", index), bytes.Repeat([]byte{byte(index)}, 128))); err != nil {
			b.Fatal(err)
		}
	}
	target := base.Clone()
	if err := target.InsertEntry(btree.NewEntry(0, fmt.Appendf(nil, "key-%011d", 50), bytes.Repeat([]byte{0xff}, 128))); err != nil {
		b.Fatal(err)
	}
	patch := make([]byte, 0, 4096)
	b.ReportAllocs()
	for b.Loop() {
		patch = patch[:0]
		if _, compatible := appendLeafBytePatch(&patch, base, target, 4096); !compatible {
			b.Fatal("leaf layout changed")
		}
	}
}

func BenchmarkAppendChangedBytePatchRanges(b *testing.B) {
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
				patch = appendChangedBytePatchRanges(patch[:0], 64, base, target)
			}
		})
	}
}

func TestWALCommit_EncodesAllocationBytePatch(t *testing.T) {
	const pageSize = 128
	file, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	log := New(Config{File: file, PageSize: pageSize, CheckpointThresholdBytes: 1 << 20})
	first := bytes.Repeat([]byte{1}, pageSize)
	second := bytes.Clone(first)
	second[40], second[41] = 2, 3
	for _, payload := range [][]byte{first, second} {
		if _, err := log.Commit(nil, []AllocationRecord{{PageID: 2, Payload: payload}}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	records, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	// The first change after a checkpoint needs a complete image. The second change patches it.
	if len(records) != 4 || records[0].Header.Type != RecordTypeAllocation || records[2].Header.Type != RecordTypeBytePatch {
		t.Fatalf("WAL records: got %+v, want a complete allocation image followed by a byte patch", records)
	}
	patched, err := ApplyBytePatch(first, records[2].Payload, pageSize)
	if err != nil || !bytes.Equal(patched, second) {
		t.Fatalf("byte patch did not rebuild the final allocation page: %v", err)
	}
	if committed, ok := log.Lookup(2); !ok || committed.Header.Type != RecordTypeAllocation || !bytes.Equal(committed.Payload, second) {
		t.Fatalf("committed allocation page: got %+v, want the final image", committed)
	}
}

func TestLimitedByteRangesEqualSuffixAndBudget(t *testing.T) {
	for _, test := range []struct {
		name    string
		edits   []int
		limit   int
		all     bool
		wantFit bool
	}{
		{name: "unchanged", limit: 64, wantFit: true},
		{name: "early", edits: []int{0}, limit: 64, wantFit: true},
		{name: "late", edits: []int{65535}, limit: 64, wantFit: true},
		{name: "multiple", edits: []int{0, 32768, 65535}, limit: 64, wantFit: true},
		{name: "small-budget", edits: []int{0}, limit: 9},
		{name: "small-edit-fits", edits: []int{0}, limit: 20, wantFit: true},
		{name: "budget-after-first-edit", edits: []int{0, 32}, limit: 18},
		{name: "full-replacement", all: true, limit: 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := bytes.Repeat([]byte{'v'}, 65536)
			target := bytes.Clone(base)
			for _, at := range test.edits {
				target[at] ^= 0xff
			}
			if test.all {
				for i := range target {
					target[i] ^= 0xff
				}
			}
			patch, fits := appendLimitedByteRanges(nil, 128, base, target, test.limit)
			if fits != test.wantFit {
				t.Fatalf("fits=%v, want %v", fits, test.wantFit)
			}
			if !bytes.Equal(base, bytes.Repeat([]byte{'v'}, len(base))) {
				t.Fatal("patch generation changed its base")
			}
			if !fits {
				return
			}
			before := append(make([]byte, 128), base...)
			want := append(make([]byte, 128), target...)
			got, err := ApplyBytePatch(before, patch, int64(len(before)))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("patch replay differs from target: %v", err)
			}
		})
	}
}
