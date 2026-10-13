package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func TestNodePageRecordsRoundTripThroughWALAndCheckpoint(t *testing.T) {
	const pageSize int64 = 64
	for _, size := range []int{0, 56, 57, 58, 1000} {
		for _, branch := range []bool{false, true} {
			name := "leaf"
			if branch {
				name = "branch"
			}
			name = fmt.Sprintf("%s-%d", name, size)
			t.Run(name, func(t *testing.T) {
				key := []byte("key")
				value := make([]byte, size)
				if branch {
					key = append([]byte{'k'}, value...)
				}
				node := btree.NewLeafNode(3)
				if branch {
					node = btree.NewRootNode(3, btree.NewLeafNode(4), btree.NewLeafNode(5), key)
				} else if err := node.InsertEntry(btree.NewEntry(0, key, value)); err != nil {
					t.Fatal(err)
				}
				ids := make([]page.ID, btree.NodePageCount(node, pageSize))
				for index := range ids {
					// Gaps and descending IDs must not change logical body order.
					if index == 0 {
						ids[index] = node.PageID()
					} else {
						ids[index] = page.ID(400 - index*7)
					}
				}
				node.SetOverflowPages(ids[1:])
				pages, err := btree.EncodeNodePages(node, pageSize, ids)
				if err != nil {
					t.Fatal(err)
				}
				walPages, err := btree.EncodeWALNodePages(node, pageSize, ids)
				if err != nil {
					t.Fatal(err)
				}
				bodyBytes := 0
				for index, data := range walPages {
					fixed, crcOffset := btree.NodeHeaderSize, btree.NodeHeaderSize-4
					if len(pages) > 1 {
						if index == 0 {
							fixed += 8 // Body length and next-page count.
						} else {
							fixed, crcOffset = btree.ContinuationHeaderSize, btree.ContinuationHeaderSize-8
						}
						bodyBytes -= int(binary.LittleEndian.Uint32(data[fixed-4:fixed])) * page.IDSize
					}
					bodyBytes += len(data) - fixed
					if binary.LittleEndian.Uint32(data[crcOffset:crcOffset+4]) != 0 {
						t.Fatal("WAL payload contains a page checksum")
					}
				}
				if bodyBytes != node.EncodedSize()-btree.NodeHeaderSize {
					t.Fatalf("WAL body has %d bytes, want %d", bodyBytes, node.EncodedSize()-btree.NodeHeaderSize)
				}
				if !branch && size == 57 && len(walPages[len(walPages)-1]) != int(pageSize) {
					t.Fatal("WAL shortened an exactly full last page")
				}
				path := filepath.Join(t.TempDir(), "pages-wal")
				file, err := os.Create(path)
				if err != nil {
					t.Fatal(err)
				}
				log := New(Config{File: file, PageSize: pageSize})
				if _, err := log.Commit([]NodeRecord{{Final: node}}, nil, nil, nil); err != nil {
					t.Fatal(err)
				}
				if len(ids) > 1 {
					// Reusing the transaction buffer must not change committed pages.
					if _, err := log.encodeTransaction(nil, nil, []byte("meta"), log.nextTxid); err != nil {
						t.Fatal(err)
					}
					for index, id := range ids {
						record, ok := log.Lookup(id)
						if !ok || record.Header.PageID != id || record.Header.TxID != 1 || record.Node != nil || !bytes.Equal(record.Payload, walPages[index]) {
							t.Fatalf("committed page %d changed", id)
						}
					}
				}
				firstRecord, ok := log.Lookup(node.PageID())
				if !ok {
					t.Fatal("first WAL page is missing")
				}
				entry, found, child, err := log.LookupCommittedPage(&firstRecord, key, true)
				if err != nil || (branch && (found || child != node.Children[1])) || (!branch && (!found || child != 0 || !bytes.Equal(entry.Value(), value))) {
					t.Fatalf("direct WAL lookup: found=%t child=%d error=%v", found, child, err)
				}
				walNode, err := log.CommittedPageToNode(&firstRecord)
				if err != nil || walNode.EntryCount() != 1 || !bytes.Equal(walNode.EntryAt(0).Key(), key) || !slices.Equal(walNode.Children, node.Children) || !slices.Equal(walNode.OverflowPages(), ids[1:]) {
					t.Fatalf("decode live WAL node: %v", err)
				}
				if !branch && !bytes.Equal(walNode.EntryAt(0).Value(), value) {
					t.Fatal("WAL decode changed the leaf value")
				}
				if err := log.Close(); err != nil {
					t.Fatal(err)
				}
				file, err = os.OpenFile(path, os.O_RDWR, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				log = New(Config{File: file, PageSize: pageSize})
				records, err := log.ReadRecords()
				if err != nil || len(records) != len(pages)+1 {
					t.Fatalf("reopened WAL: got %d records, %v", len(records), err)
				}
				if !IsCommitMarker(&records[len(pages)]) || records[len(pages)].Header.TxID != 1 {
					t.Fatal("page records lack their final commit marker")
				}
				for index, record := range records[:len(pages)] {
					if record.Header.Type != RecordTypeNode || record.Header.PageID != ids[index] || record.Header.TxID != 1 || !bytes.Equal(record.Payload, walPages[index]) {
						t.Fatalf("page record %d changed", index)
					}
					log.LoadCommittedRecord(record)
				}
				firstRecord, _ = log.Lookup(node.PageID())
				entry, found, child, err = log.LookupCommittedPage(&firstRecord, key, true)
				if err != nil || (branch && (found || child != node.Children[1])) || (!branch && (!found || child != 0 || !bytes.Equal(entry.Value(), value))) {
					t.Fatalf("direct WAL lookup: found=%t child=%d error=%v", found, child, err)
				}
				walNode, err = log.CommittedPageToNode(&firstRecord)
				if err != nil || !bytes.Equal(walNode.EntryAt(0).Key(), key) || !slices.Equal(walNode.Children, node.Children) || !slices.Equal(walNode.OverflowPages(), ids[1:]) || (!branch && !bytes.Equal(walNode.EntryAt(0).Value(), value)) {
					t.Fatalf("decode reopened WAL node: %v", err)
				}
				main, err := os.Create(filepath.Join(t.TempDir(), "main"))
				if err != nil {
					t.Fatal(err)
				}
				defer main.Close()
				targetSize := int64(slices.Max(ids)+1) * pageSize
				if err := log.Checkpoint(main, targetSize, true); err != nil {
					t.Fatal(err)
				}
				readPage := func(id page.ID) ([]byte, error) {
					data := make([]byte, pageSize)
					_, err := main.ReadAt(data, int64(id)*pageSize)
					return data, err
				}
				for index, id := range ids {
					data, err := readPage(id)
					if err != nil || !bytes.Equal(data, pages[index]) {
						t.Fatalf("checkpoint page %d changed: %v", id, err)
					}
				}
				first, err := readPage(node.PageID())
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := btree.DecodeNode(first, node.PageID(), pageSize, readPage)
				if err != nil || decoded.EntryCount() != 1 || !bytes.Equal(decoded.EntryAt(0).Key(), key) || !slices.Equal(decoded.Children, node.Children) || !slices.Equal(decoded.OverflowPages(), ids[1:]) {
					t.Fatalf("decode checkpointed node: %v", err)
				}
				if !branch && !bytes.Equal(decoded.EntryAt(0).Value(), value) {
					t.Fatal("checkpoint changed the leaf value")
				}
			})
		}
	}
}

func TestCheckpointRejectsInvalidNodePageBeforeWriting(t *testing.T) {
	const pageSize int64 = 64
	first := btree.NewLeafNode(3)
	if err := first.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	continuation := make([]byte, btree.ContinuationHeaderSize+4)
	binary.LittleEndian.PutUint16(continuation[:2], uint16(btree.NodeTypeContinuation))
	binary.LittleEndian.PutUint64(continuation[2:10], 7)
	copy(continuation[btree.ContinuationHeaderSize:], "body")
	for _, mode := range []string{"checksum", "short", "page-id", "page-type", "id-array", "short-array", "decoded-node"} {
		t.Run(mode, func(t *testing.T) {
			bad := CommittedPage{Header: RecordHeader{Type: RecordTypeNode, PageID: 7}, Payload: slices.Clone(continuation)}
			switch mode {
			case "checksum":
				binary.LittleEndian.PutUint32(bad.Payload[10:14], 1)
			case "short":
				bad.Payload = bad.Payload[:btree.ContinuationHeaderSize-1]
			case "page-id":
				bad.Header.PageID++
			case "page-type":
				binary.LittleEndian.PutUint16(bad.Payload[:2], 99)
			case "id-array":
				binary.LittleEndian.PutUint32(bad.Payload[14:18], 99)
			case "short-array":
				binary.LittleEndian.PutUint32(bad.Payload[14:18], 1)
			case "decoded-node":
				bad.Node = first
			}
			records := []CommittedPage{{Header: RecordHeader{Type: RecordTypeNode, PageID: 3}, Payload: btree.EncodeWALNode(first)}, bad}
			writer := &checkpointWriter{}
			wantErr := page.ErrInvalid
			if err := writeCheckpointRecordRuns(writer, records, pageSize, true); !errors.Is(err, wantErr) {
				t.Fatalf("checkpoint: got %v, want %v", err, wantErr)
			}
			if len(writer.writes) != 0 {
				t.Fatal("checkpoint wrote pages before it found the invalid page")
			}
		})
	}
}

func TestCompactContinuationRecordRejectsDamagedWALBytes(t *testing.T) {
	data := make([]byte, btree.ContinuationHeaderSize+3)
	binary.LittleEndian.PutUint16(data[:2], uint16(btree.NodeTypeContinuation))
	binary.LittleEndian.PutUint64(data[2:10], 7)
	data[btree.ContinuationHeaderSize] = 'b'
	record := WALRecord{Header: RecordHeader{Type: RecordTypeNode, PageID: 7, TxID: 1}, Payload: data}
	encoded, err := EncodeWALRecord(&record, 64)
	if err != nil {
		t.Fatal(err)
	}
	encoded[HeaderSize+len(data)-1] ^= 1
	if _, err := DecodeWALRecord(bytes.NewReader(encoded), 64); !errors.Is(err, page.ErrChecksum) {
		t.Fatalf("damaged WAL body: got %v, want checksum error", err)
	}
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

	if err := writeCheckpointRecordRuns(writer, records, pageSize, true); err != nil {
		t.Fatal(err)
	}
	if len(writer.writes) != 1 {
		t.Fatalf("checkpoint writes: got %d, want 1", len(writer.writes))
	}
	pageData := writer.writes[0].data
	if _, err := btree.DecodeNode(pageData, node.PageID(), pageSize, nil); err != nil {
		t.Fatalf("decode checkpoint page: %v", err)
	}
	pageData[len(pageData)-1] ^= 0xff
	if _, err := btree.DecodeNode(pageData, node.PageID(), pageSize, nil); !errors.Is(err, page.ErrChecksum) {
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
	if err := writeCheckpointRecordRuns(writer, records, pageSize, true); !errors.Is(err, btree.ErrInvalid) {
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

	if err := writeCheckpointRecordRuns(writer, records, 4, true); err != nil {
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

	if err := writeCheckpointRecordRuns(writer, records, pageSize, true); err != nil {
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
