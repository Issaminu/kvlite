package kvlite

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

func TestMetaFromCommittedWAL_RejectsInvalidPageSize(t *testing.T) {
	encoded := page.EncodeMeta(page.NewMeta(4096))
	binary.LittleEndian.PutUint64(encoded[8:16], uint64(MaxValueSize+1))
	meta, err := page.DecodeMeta(encoded)
	if err != nil {
		t.Fatal(err)
	}
	meta.RefreshChecksum()

	const txID wal.TxID = 1
	records := []wal.Record{
		{
			Header:      wal.RecordHeader{Type: wal.RecordTypeMeta, PageID: page.Meta0ID, TxID: txID},
			PageContent: page.EncodeMeta(meta),
		},
		{Header: wal.RecordHeader{Type: wal.RecordTypeCommit, TxID: txID}},
	}

	if _, err := metaFromCommittedWAL(records); !errors.Is(err, ErrInvalid) {
		t.Fatalf("metadata error: got %v, want ErrInvalid", err)
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
	records := []wal.Record{
		{
			Header:      wal.RecordHeader{Type: wal.RecordTypeData, PageID: node.PageID(), TxID: txID},
			PageContent: image,
		},
		{Header: wal.RecordHeader{Type: wal.RecordTypeCommit, TxID: txID}},
	}
	db := &DB{wal: wal.New(wal.Config{})}

	if err := db.loadCommittedIntoOverlay(records); !errors.Is(err, ErrInvalid) {
		t.Fatalf("read-only recovery error: got %v, want ErrInvalid", err)
	}
	if db.wal.Stats().CommittedRecordCount != 0 {
		t.Fatal("read-only recovery published an invalid node")
	}
}
