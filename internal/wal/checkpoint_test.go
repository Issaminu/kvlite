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
		{value: "later", wantType: RecordTypeBytePatch, failedCheckpoint: true},
		{value: "reset", wantType: RecordTypeNode, checkpoint: true},
		{value: "after", wantType: RecordTypeBytePatch},
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
		pages[id] = CommittedPage{Header: RecordHeader{Type: RecordTypeNode, PageID: id}, Node: btree.NewLeafNode(id)}
	}
	if err := buffers.writeCommittedPages(&checkpointWriter{}, pages, 64, false); err != nil {
		t.Fatal(err)
	}
	if buffers.records != nil {
		t.Fatal("checkpoint retained an oversized record array")
	}
	pages = map[page.ID]CommittedPage{3: {Header: RecordHeader{Type: RecordTypeNode, PageID: 3}, Node: btree.NewLeafNode(3)}}
	if err := buffers.writeCommittedPages(&checkpointWriter{}, pages, 2*checkpointWriteBatchBytes, false); err != nil {
		t.Fatal(err)
	}
	if int64(cap(buffers.data)) > checkpointWriteBatchBytes {
		t.Fatal("checkpoint retained an oversized page buffer")
	}
}

func TestWriteCheckpointRecordRuns_SealsDataPage(t *testing.T) {
	const pageSize int64 = 64
	node := btree.NewLeafNode(3)
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
	const pageSize = 64
	var records []CommittedPage
	for _, id := range []page.ID{3, 4, 6, 7} {
		node := nativeLeaf(t, id, 1, pageSize)
		records = append(records, CommittedPage{Header: RecordHeader{Type: RecordTypeNode, PageID: id}, Node: node})
	}
	writer := &checkpointWriter{}
	if err := writeCheckpointRecordRuns(writer, records, pageSize, false, nil); err != nil {
		t.Fatal(err)
	}
	if len(writer.writes) != 2 {
		t.Fatalf("writes=%d", len(writer.writes))
	}
	for index, write := range writer.writes {
		var want []byte
		for _, record := range records[index*2 : index*2+2] {
			image := make([]byte, pageSize)
			copy(image, btree.EncodeWALNode(record.Node))
			want = append(want, image...)
		}
		if write.offset != int64(records[index*2].Header.PageID)*pageSize || !bytes.Equal(write.data, want) {
			t.Fatal("adjacent write changed a gap or payload")
		}
	}
}

func TestWriteCheckpointRecordRuns_LimitsContiguousWriteSize(t *testing.T) {
	const pageSize int64 = 4096
	records := make([]CommittedPage, 257)
	for index := range records {
		records[index].Header.Type = RecordTypeNode
		records[index].Header.PageID = page.ID(index + 3)
		records[index].Node = btree.NewLeafNode(records[index].Header.PageID)
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

func TestNativeSpanCheckpointAndBackupImages(t *testing.T) {
	const pageSize = 4096
	for _, size := range []int{65536, 3<<20 + 37} {
		for _, checksums := range []bool{false, true} {
			node := nativeLeaf(t, 20, size, pageSize)
			for _, encoded := range []bool{false, true} {
				record := CommittedPage{Header: RecordHeader{Type: RecordTypeNode, PageID: node.PageID()}, Node: node}
				if encoded {
					record.Node = nil
					record.Payload = btree.EncodeWALNode(node)
				}
				before := bytes.Clone(record.Payload)
				file, err := os.CreateTemp(t.TempDir(), "main")
				if err != nil {
					t.Fatal(err)
				}
				if err := WriteCommittedPages(file, map[page.ID]CommittedPage{node.PageID(): record}, pageSize, checksums); err != nil {
					t.Fatal(err)
				}
				span := make([]byte, btree.NodePageCount(node, pageSize)*pageSize)
				if _, err := file.ReadAt(span, int64(node.PageID())*pageSize); err != nil {
					t.Fatal(err)
				}
				got, err := btree.DecodeNode(span, node.PageID(), pageSize, nil, checksums)
				if err != nil || !bytes.Equal(got.EntryAt(0).Value(), node.EntryAt(0).Value()) {
					t.Fatalf("whole span decode: size=%d checksums=%t encoded=%t err=%v", size, checksums, encoded, err)
				}
				if !bytes.Equal(before, record.Payload) {
					t.Fatal("checkpoint changed a retained compact image")
				}
				if checksums {
					span[len(span)-1] ^= 1
					if err := btree.CheckNodeChecksum(span, node.PageID()); err == nil {
						t.Fatal("tail padding was outside the checksum")
					}
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

func TestNativeSpanCheckpointRejectsOverlapBeforeWrite(t *testing.T) {
	const pageSize = 4096
	first := nativeLeaf(t, 20, 65536, pageSize)
	second := nativeLeaf(t, 21, 128, pageSize)
	writer := &checkpointWriter{}
	err := WriteCommittedPages(writer, map[page.ID]CommittedPage{
		20: {Header: RecordHeader{Type: RecordTypeNode, PageID: 20}, Node: first},
		21: {Header: RecordHeader{Type: RecordTypeNode, PageID: 21}, Node: second},
	}, pageSize, false)
	if !errors.Is(err, page.ErrInvalid) || len(writer.writes) != 0 {
		t.Fatalf("overlap reached storage: %v, writes=%d", err, len(writer.writes))
	}
}

func TestNativeSpanCheckpointRejectsInvalidHeadBeforeWrite(t *testing.T) {
	for _, id := range []page.ID{page.Meta0ID, page.Meta1ID, ^page.ID(0)} {
		node := nativeLeaf(t, id, 1, 4096)
		for _, encoded := range []bool{false, true} {
			record := CommittedPage{Header: RecordHeader{Type: RecordTypeNode, PageID: id}, Node: node}
			if encoded {
				record.Node = nil
				record.Payload = btree.EncodeWALNode(node)
			}
			writer := &checkpointWriter{}
			err := WriteCommittedPages(writer, map[page.ID]CommittedPage{id: record}, 4096, false)
			if !errors.Is(err, page.ErrInvalid) || len(writer.writes) != 0 {
				t.Fatalf("invalid head %d encoded=%t reached storage: error=%v writes=%d", id, encoded, err, len(writer.writes))
			}
		}
	}
}
