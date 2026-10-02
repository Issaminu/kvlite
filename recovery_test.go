package kvlite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

func TestOpenIgnoresDroppedAllocationSegmentInWAL(t *testing.T) {
	for _, test := range []struct {
		name     string
		readOnly bool
		badPage  bool
	}{
		{"read-only", true, false},
		{"writable", false, false},
		{"invalid allocation page", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			initial, err := Open(path, 0600, &Options{Synchronous: SyncNone})
			if err != nil {
				t.Fatal(err)
			}
			meta := *initial.meta
			if err := initial.Close(); err != nil {
				t.Fatal(err)
			}

			bitmap := newAllocationBitmap(meta.PageSize())
			bitmap.addSegment()
			segmentPageID := bitmap.segmentPageID(1)
			initialLastPage := meta.LastPage()
			recordPageID := segmentPageID
			if test.badPage {
				recordPageID = firstTreePageID + 1
			}
			walFile, err := os.OpenFile(path+"-wal", os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				t.Fatal(err)
			}
			log := wal.New(wal.Config{File: walFile, PageSize: meta.PageSize()})
			meta.SetLastPage(segmentPageID)
			meta.AdvanceGeneration()
			meta.RefreshChecksum()
			if _, err := log.Commit(nil, []wal.AllocationRecord{{PageID: recordPageID, Payload: bitmap.encodeSegment(1)}}, page.EncodeMeta(&meta), nil); err != nil {
				t.Fatal(err)
			}
			meta.SetLastPage(initialLastPage)
			meta.AdvanceGeneration()
			meta.RefreshChecksum()
			if _, err := log.Commit(nil, nil, page.EncodeMeta(&meta), nil); err != nil {
				t.Fatal(err)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := Open(path, 0600, &Options{ReadOnly: test.readOnly, Synchronous: SyncNone})
			if test.badPage {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("open with invalid allocation page: got %v, want %v", err, ErrInvalid)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if _, found := reopened.wal.CommittedRecord(segmentPageID); found {
				t.Fatalf("recovery kept dropped allocation page %d", segmentPageID)
			}
		})
	}
}

func TestRecoverySkipsOldPatchesAfterFreeOrFullNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	root := btree.NewLeafNode(firstTreePageID)
	fullRoot := btree.EncodeWALNode(root)
	records := []wal.WALRecord{
		{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: firstTreePageID + 1, TxID: 1}, Payload: btree.EncodeWALNode(btree.NewLeafNode(firstTreePageID + 1))},
		{Header: wal.RecordHeader{Type: wal.RecordTypePatch, PageID: firstTreePageID + 1, TxID: 1}, Payload: []byte{0xff}},
		{Header: wal.RecordHeader{Type: wal.RecordTypePatch, PageID: firstTreePageID, TxID: 1}, Payload: []byte{0xff}},
		{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: firstTreePageID, TxID: 2}, Payload: fullRoot},
	}
	if err := db.loadCommittedIntoOverlay(records); err != nil {
		t.Fatal(err)
	}
	if _, ok := db.wal.CommittedRecord(firstTreePageID + 1); ok {
		t.Fatal("recovery kept a freed page")
	}
	got, ok := db.wal.CommittedRecord(firstTreePageID)
	if !ok || got.Header.Type != wal.RecordTypeNode || !bytes.Equal(got.Payload, fullRoot) {
		t.Fatalf("recovered root: found=%t type=%d", ok, got.Header.Type)
	}
}

func TestMetaFromCommittedRecords_RejectsInvalidPageSize(t *testing.T) {
	encoded := page.EncodeMeta(page.NewMeta(4096))
	binary.LittleEndian.PutUint64(encoded[8:16], uint64(MaxValueSize+1))
	meta, err := page.DecodeMeta(encoded)
	if err != nil {
		t.Fatal(err)
	}
	meta.RefreshChecksum()

	const txID wal.TxID = 1
	records := []wal.WALRecord{
		{
			Header:  wal.RecordHeader{Type: wal.RecordTypeMeta, PageID: page.Meta0ID, TxID: txID},
			Payload: page.EncodeMeta(meta),
		},
		{Header: wal.RecordHeader{Type: wal.RecordTypeCommit, TxID: txID}},
	}

	committed, err := committedWALRecords(records)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metaFromCommittedRecords(committed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("metadata error: got %v, want ErrInvalid", err)
	}
}

func TestMetaFromCommittedRecords_NoMetadata(t *testing.T) {
	meta, err := metaFromCommittedRecords([]wal.WALRecord{{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: 2, TxID: 1}}})
	if err != nil || meta != nil {
		t.Fatalf("metadata without metadata record: meta=%v err=%v", meta, err)
	}
}

func TestLoadCommittedIntoOverlay_RejectsInvalidNodeDirectory(t *testing.T) {
	node := btree.NewLeafNode(2)
	if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	image := btree.EncodeWALNode(node)
	const firstLeafEntryOffsetField = btree.NodeHeaderSize + 4
	binary.LittleEndian.PutUint32(image[firstLeafEntryOffsetField:firstLeafEntryOffsetField+4], ^uint32(0))
	const txID wal.TxID = 1
	records := []wal.WALRecord{
		{
			Header:  wal.RecordHeader{Type: wal.RecordTypeNode, PageID: node.PageID(), TxID: txID},
			Payload: image,
		},
		{Header: wal.RecordHeader{Type: wal.RecordTypeCommit, TxID: txID}},
	}
	db := &DB{wal: wal.New(wal.Config{})}
	committed, err := committedWALRecords(records)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.loadCommittedIntoOverlay(committed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("read-only recovery error: got %v, want ErrInvalid", err)
	}
	if db.wal.Stats().CommittedRecordCount != 0 {
		t.Fatal("read-only recovery published an invalid node")
	}
}
