package wal

import (
	"bytes"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

type checkpointWrite struct {
	offset int64
	data   []byte
}

type checkpointWriter struct {
	writes []checkpointWrite
}

func (writer *checkpointWriter) WriteAt(data []byte, offset int64) (int, error) {
	writer.writes = append(writer.writes, checkpointWrite{offset: offset, data: slices.Clone(data)})
	return len(data), nil
}

func TestWriteCheckpointRecordRuns_CombinesAdjacentPages(t *testing.T) {
	records := []Record{
		{Header: RecordHeader{PageID: 1}, PageContent: []byte("a")},
		{Header: RecordHeader{PageID: 2}, PageContent: []byte("bb")},
		{Header: RecordHeader{PageID: 4}, PageContent: []byte("d")},
		{Header: RecordHeader{PageID: 5}, PageContent: []byte("ee")},
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
	records := make([]Record, 257)
	for index := range records {
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
