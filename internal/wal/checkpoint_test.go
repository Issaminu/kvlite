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

func TestWALCommit_KeepsFullImageUntilCheckpoint(t *testing.T) {
	const pageSize = 128
	file, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	main, err := os.CreateTemp(t.TempDir(), "main")
	if err != nil {
		t.Fatal(err)
	}
	defer main.Close()
	log := New(Config{File: file, PageSize: pageSize, SyncOnCommit: true, SyncOnCheckpoint: true})
	base := btree.NewLeafNode(2)
	if err := base.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("start"))); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		value            string
		wantType         RecordType
		checkpoint       bool
		failedCheckpoint bool
	}{
		{value: "first", wantType: RecordTypeNode},
		{value: "later", wantType: RecordTypePatch, failedCheckpoint: true},
		{value: "reset", wantType: RecordTypeNode, checkpoint: true},
		{value: "after", wantType: RecordTypePatch},
	} {
		if step.failedCheckpoint {
			readOnly, err := os.Open(main.Name())
			if err != nil {
				t.Fatal(err)
			}
			checkpointErr := log.Checkpoint(readOnly, 3*pageSize, false)
			if err := readOnly.Close(); err != nil {
				t.Fatal(err)
			}
			if checkpointErr == nil {
				t.Fatal("checkpoint to a read-only file succeeded")
			}
		}
		if step.checkpoint {
			if err := log.Checkpoint(main, 3*pageSize, false); err != nil {
				t.Fatal(err)
			}
		}
		target := base.Clone()
		if err := target.InsertEntry(btree.NewEntry(0, []byte("key"), []byte(step.value))); err != nil {
			t.Fatal(err)
		}
		if _, err := log.Commit([]NodeRecord{{Original: base, Final: target}}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		records, err := log.ReadRecords()
		if err != nil {
			t.Fatal(err)
		}
		if len(records) < 2 || records[len(records)-2].Header.Type != step.wantType {
			t.Fatalf("record for %q: want type %d, got %+v", step.value, step.wantType, records)
		}
		base = target
	}
}

func TestWALCheckpoint_MainSyncFailureKeepsWAL(t *testing.T) {
	main, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer main.Close()
	if err := main.Sync(); err == nil {
		t.Skip("the null device does not report sync failures on this platform")
	}
	if _, err := main.WriteAt(make([]byte, 128), 0); err != nil {
		t.Skip("the null device does not support writes at an offset")
	}
	file, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	log := New(Config{File: file, PageSize: 128, SyncOnCheckpoint: true})
	node := btree.NewLeafNode(2)
	if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Commit([]NodeRecord{{Final: node}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Checkpoint(main, 0, false); err == nil {
		t.Fatal("checkpoint skipped the main-file sync failure")
	}
	after, err := os.ReadFile(file.Name())
	if err != nil || !bytes.Equal(after, before) || len(log.overlay) != 1 {
		t.Fatalf("main-file sync failure discarded recovery data: %v", err)
	}
}

func TestWALCheckpoint_ReusesBuffersAfterWriteFailure(t *testing.T) {
	for _, checksums := range []bool{false, true} {
		t.Run(fmt.Sprint(checksums), func(t *testing.T) {
			const pageSize = 4096
			dir := t.TempDir()
			mainPath := filepath.Join(dir, "main")
			main, err := os.Create(mainPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = main.Close() }()
			file, err := os.Create(filepath.Join(dir, "wal"))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			log := New(Config{File: file, PageSize: pageSize})
			var nodes []NodeRecord
			for id := page.ID(3); id <= 5; id++ {
				node := btree.NewLeafNode(id)
				if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), bytes.Repeat([]byte{0xff}, 4000))); err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, NodeRecord{Final: node})
			}
			if _, err := log.Commit(nodes, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			if err := log.Checkpoint(main, 6*pageSize, checksums); err != nil {
				t.Fatal(err)
			}
			buffer := log.checkpointBuffers.data[:cap(log.checkpointBuffers.data)]
			records := log.checkpointBuffers.records[:cap(log.checkpointBuffers.records)]
			short := btree.NewLeafNode(3)
			if err := short.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("x"))); err != nil {
				t.Fatal(err)
			}
			if _, err := log.Commit([]NodeRecord{{Final: short}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			if err := main.Close(); err != nil {
				t.Fatal(err)
			}
			if err := log.Checkpoint(main, 6*pageSize, checksums); err == nil {
				t.Fatal("checkpoint to a closed file succeeded")
			}
			if len(log.overlay) != 1 {
				t.Fatal("failed checkpoint discarded committed data")
			}
			for _, record := range records {
				if record.Header != (RecordHeader{}) || record.Node != nil || record.Payload != nil {
					t.Fatal("checkpoint scratch records retained committed references")
				}
			}
			main, err = os.OpenFile(mainPath, os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if err := log.Checkpoint(main, 6*pageSize, checksums); err != nil {
				t.Fatal(err)
			}
			if &buffer[0] != &log.checkpointBuffers.data[:1][0] || &records[0] != &log.checkpointBuffers.records[:1][0] {
				t.Fatal("checkpoint replaced reusable storage")
			}
			data := make([]byte, pageSize)
			if _, err := main.ReadAt(data, 3*pageSize); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data[btree.WALNodeEncodedSize(short):], make([]byte, pageSize-btree.WALNodeEncodedSize(short))) {
				t.Fatal("reused buffer left old value bytes in page padding")
			}
			decoded, err := btree.DecodeNode(data, 3, pageSize, nil, checksums)
			if err != nil || !bytes.Equal(decoded.EntryAt(0).Value(), []byte("x")) {
				t.Fatalf("read retried checkpoint: %v", err)
			}
			if len(log.overlay) != 0 || log.Stats().CheckpointCount != 2 {
				t.Fatal("successful retry did not finish checkpoint cleanup")
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
			if log.checkpointBuffers.data != nil || log.checkpointBuffers.records != nil {
				t.Fatal("closed WAL retained checkpoint storage")
			}
		})
	}
}

func TestCheckpointBuffers_BoundsRetainedStorage(t *testing.T) {
	var buffers checkpointBuffers
	pages := make(map[page.ID]CommittedPage, maxRetainedCheckpointRecords+1)
	for i := range maxRetainedCheckpointRecords + 1 {
		id := page.ID(3 + i)
		pages[id] = CommittedPage{Header: RecordHeader{Type: RecordTypeAllocation, PageID: id}, Payload: make([]byte, 64)}
	}
	if err := buffers.writeCommittedPages(&checkpointWriter{}, pages, 64, false); err != nil {
		t.Fatal(err)
	}
	if buffers.records != nil {
		t.Fatal("checkpoint retained an oversized record array")
	}
	pages = map[page.ID]CommittedPage{3: {Header: RecordHeader{Type: RecordTypeAllocation, PageID: 3}}}
	if err := buffers.writeCommittedPages(&checkpointWriter{}, pages, 2*checkpointWriteBatchBytes, false); err != nil {
		t.Fatal(err)
	}
	if int64(cap(buffers.data)) > checkpointWriteBatchBytes {
		t.Fatal("checkpoint retained an oversized page buffer")
	}
}

func TestWALCommit_MixedContinuedImagesKeepOwnedBytes(t *testing.T) {
	const pageSize = 64
	file, err := os.Create(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	log := New(Config{File: file, PageSize: pageSize, CheckpointThresholdBytes: ^uint64(0)})
	// Warm reusable storage before a transaction with owned continuation images.
	log.encodingBuffer = make([]byte, 0, 4096)
	nodes := []NodeRecord{
		{Final: btree.NewLeafNode(3)},
		{Final: btree.NewRootNode(100, btree.NewLeafNode(4), btree.NewLeafNode(5), make([]byte, 200))},
		{Final: btree.NewLeafNode(200)},
		{Final: btree.NewLeafNode(300)},
	}
	if err := nodes[2].Final.InsertEntry(btree.NewEntry(0, []byte("key"), make([]byte, 200))); err != nil {
		t.Fatal(err)
	}
	allocation := []AllocationRecord{{PageID: 400, Payload: make([]byte, pageSize)}}
	meta := []byte("meta")
	want := make(map[page.ID][]byte)
	var expectedRecords []WALRecord
	for _, record := range nodes {
		ids := make([]page.ID, btree.NodePageCount(record.Final, pageSize))
		for i := range ids {
			ids[i] = record.Final.PageID() + page.ID(i)
		}
		record.Final.SetOverflowPages(ids[1:])
		images, err := btree.EncodeWALNodePages(record.Final, pageSize, ids)
		if err != nil {
			t.Fatal(err)
		}
		for i, data := range images {
			want[ids[i]] = data
			expectedRecords = append(expectedRecords, WALRecord{Header: RecordHeader{Type: RecordTypeNode, PageID: ids[i], TxID: 1}, Payload: data})
		}
	}
	expectedRecords = append(expectedRecords, WALRecord{Header: RecordHeader{Type: RecordTypeAllocation, PageID: 400, TxID: 1}, Payload: allocation[0].Payload})
	for _, id := range []page.ID{page.Meta0ID, page.Meta1ID} {
		expectedRecords = append(expectedRecords, WALRecord{Header: RecordHeader{Type: RecordTypeMeta, PageID: id, TxID: 1}, Payload: meta})
	}
	expectedRecords = append(expectedRecords, WALRecord{Header: RecordHeader{Type: RecordTypeCommit, TxID: 1}})
	if _, err := log.Commit(nodes, allocation, meta, nil); err != nil {
		t.Fatal(err)
	}
	// Record checksums use the salt that the commit selected.
	var expected []byte
	for index := range expectedRecords {
		expected = AppendEncodedWALRecord(expected, &expectedRecords[index], log.checksumSeed)
	}
	got, err := os.ReadFile(file.Name())
	if err != nil || !bytes.Equal(got[walFileHeaderSize:], expected) {
		t.Fatalf("mixed WAL transaction changed: %v", err)
	}
	// A later large transaction must not reuse any retained payload storage.
	other := btree.NewRootNode(500, btree.NewLeafNode(4), btree.NewLeafNode(5), bytes.Repeat([]byte{0xff}, 1000))
	ids := make([]page.ID, btree.NodePageCount(other, pageSize)-1)
	for i := range ids {
		ids[i] = page.ID(501 + i)
	}
	other.SetOverflowPages(ids)
	if _, err := log.Commit([]NodeRecord{{Final: other}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	clear(log.encodingBuffer[:cap(log.encodingBuffer)])
	for id, expected := range want {
		record, ok := log.Lookup(id)
		if !ok {
			t.Fatalf("committed page %d is missing", id)
		}
		if record.Node != nil {
			continue
		}
		if !bytes.Equal(record.Payload, expected) || cap(record.Payload) != len(record.Payload) {
			t.Fatalf("committed page %d changed after a later commit", id)
		}
	}
	if _, err := log.ReadRecords(); err != nil {
		t.Fatalf("read mixed WAL records: %v", err)
	}
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
					if _, _, err := log.encodeTransaction(nil, nil, []byte("meta"), log.nextTxid); err != nil {
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
				decoded, err := btree.DecodeNode(first, node.PageID(), pageSize, readPage, true)
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
			if err := writeCheckpointRecordRuns(writer, records, pageSize, true, nil); !errors.Is(err, wantErr) {
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
	encoded, err := EncodeWALRecord(&record, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	encoded[HeaderSize+len(data)-1] ^= 1
	if _, err := DecodeWALRecord(bytes.NewReader(encoded), 64, 0); !errors.Is(err, page.ErrChecksum) {
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

	if err := writeCheckpointRecordRuns(writer, records, pageSize, true, nil); err != nil {
		t.Fatal(err)
	}
	if len(writer.writes) != 1 {
		t.Fatalf("checkpoint writes: got %d, want 1", len(writer.writes))
	}
	pageData := writer.writes[0].data
	if _, err := btree.DecodeNode(pageData, node.PageID(), pageSize, nil, true); err != nil {
		t.Fatalf("decode checkpoint page: %v", err)
	}
	pageData[len(pageData)-1] ^= 0xff
	if _, err := btree.DecodeNode(pageData, node.PageID(), pageSize, nil, true); !errors.Is(err, page.ErrChecksum) {
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
	if err := writeCheckpointRecordRuns(writer, records, pageSize, true, nil); !errors.Is(err, btree.ErrInvalid) {
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

	if err := writeCheckpointRecordRuns(writer, records, 4, true, nil); err != nil {
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

	if err := writeCheckpointRecordRuns(writer, records, pageSize, true, nil); err != nil {
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
