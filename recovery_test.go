package kvlite

import (
	"encoding/binary"
	"errors"
	"testing"

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
