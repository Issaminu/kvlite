package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

type checkpointWrite struct {
	offset int64
	data   []byte
}

type checkpointWriter struct {
	writes []checkpointWrite
}

func TestWriteCheckpointRecordRuns_SealsDataPage(t *testing.T) {
	const pageSize int64 = 64
	node := btree.NewLeafNode(1)
	if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	records := []CommittedPage{{
		Header: RecordHeader{Type: RecordTypeNode, PageID: node.PageID()},
		Node:   node,
	}}
	writer := &checkpointWriter{}

	if err := writeCheckpointRecordRuns(writer, records, pageSize); err != nil {
		t.Fatal(err)
	}
	if len(writer.writes) != 1 {
		t.Fatalf("checkpoint writes: got %d, want 1", len(writer.writes))
	}
	pageData := writer.writes[0].data
	if _, err := btree.DecodeNode(pageData, node.PageID(), pageSize); err != nil {
		t.Fatalf("decode checkpoint page: %v", err)
	}
	pageData[len(pageData)-1] ^= 0xff
	if _, err := btree.DecodeNode(pageData, node.PageID(), pageSize); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("decode corrupted checkpoint page: got %v, want ErrChecksum", err)
	}
}

func TestWriteCheckpointRecordRuns_ValidatesAllNodesBeforeWriting(t *testing.T) {
	const pageSize int64 = 600_000
	records := make([]CommittedPage, 0, 3)
	for pageID := page.ID(2); pageID <= 4; pageID++ {
		node := btree.NewLeafNode(pageID)
		if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("value"))); err != nil {
			t.Fatal(err)
		}
		records = append(records, CommittedPage{
			Header:  RecordHeader{Type: RecordTypeNode, PageID: pageID},
			Payload: btree.EncodeWALNode(node),
		})
	}
	const firstLeafEntryOffsetField = btree.NodeHeaderSize + 4
	binary.LittleEndian.PutUint32(records[2].Payload[firstLeafEntryOffsetField:firstLeafEntryOffsetField+4], ^uint32(0))

	writer := &checkpointWriter{}
	if err := writeCheckpointRecordRuns(writer, records, pageSize); !errors.Is(err, btree.ErrInvalid) {
		t.Fatalf("checkpoint error: got %v, want ErrInvalid", err)
	}
	if len(writer.writes) != 0 {
		t.Fatalf("checkpoint wrote %d runs before full validation", len(writer.writes))
	}
}

func (writer *checkpointWriter) WriteAt(data []byte, offset int64) (int, error) {
	writer.writes = append(writer.writes, checkpointWrite{offset: offset, data: slices.Clone(data)})
	return len(data), nil
}

func TestWriteCheckpointRecordRuns_CombinesAdjacentPages(t *testing.T) {
	records := []CommittedPage{
		{Header: RecordHeader{Type: RecordTypeMeta, PageID: 1}, Payload: []byte("a")},
		{Header: RecordHeader{Type: RecordTypeMeta, PageID: 2}, Payload: []byte("bb")},
		{Header: RecordHeader{Type: RecordTypeMeta, PageID: 4}, Payload: []byte("d")},
		{Header: RecordHeader{Type: RecordTypeMeta, PageID: 5}, Payload: []byte("ee")},
	}
	writer := &checkpointWriter{}

	if err := writeCheckpointRecordRuns(writer, records, 4); err != nil {
		t.Fatal(err)
	}

	want := []checkpointWrite{
		{offset: int64(page.ID(1)) * 4, data: []byte{'a', 0, 0, 0, 'b', 'b', 0, 0}},
		{offset: int64(page.ID(4)) * 4, data: []byte{'d', 0, 0, 0, 'e', 'e', 0, 0}},
	}
	if len(writer.writes) != len(want) {
		t.Fatalf("checkpoint writes: got %d, want %d", len(writer.writes), len(want))
	}
	for index := range want {
		if writer.writes[index].offset != want[index].offset || !bytes.Equal(writer.writes[index].data, want[index].data) {
			t.Fatalf("write %d: got offset=%d data=%v, want offset=%d data=%v", index, writer.writes[index].offset, writer.writes[index].data, want[index].offset, want[index].data)
		}
	}
}

func TestWriteCheckpointRecordRuns_LimitsContiguousWriteSize(t *testing.T) {
	const pageSize int64 = 4096
	records := make([]CommittedPage, 257)
	for index := range records {
		records[index].Header.Type = RecordTypeMeta
		records[index].Header.PageID = page.ID(index + 1)
	}
	writer := &checkpointWriter{}

	if err := writeCheckpointRecordRuns(writer, records, pageSize); err != nil {
		t.Fatal(err)
	}

	writeSizes := make([]int, len(writer.writes))
	for index := range writer.writes {
		writeSizes[index] = len(writer.writes[index].data)
	}
	want := []int{1 << 20, int(pageSize)}
	if !slices.Equal(writeSizes, want) {
		t.Fatalf("checkpoint write sizes: got %v, want %v", writeSizes, want)
	}
}
