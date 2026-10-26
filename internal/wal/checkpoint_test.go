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
	"github.com/Issaminu/kvlite/internal/fileio"
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

type directSizeFile struct {
	fileio.File
	stats, truncates               int
	writeErr, statErr, truncateErr error
}

func (file *directSizeFile) Stat() (os.FileInfo, error) {
	file.stats++
	if file.statErr != nil {
		return nil, file.statErr
	}
	return file.File.Stat()
}

func (file *directSizeFile) WriteAt(data []byte, offset int64) (int, error) {
	if file.writeErr != nil {
		return 0, file.writeErr
	}
	return file.File.WriteAt(data, offset)
}

func (file *directSizeFile) Truncate(size int64) error {
	file.truncates++
	if file.truncateErr != nil {
		return file.truncateErr
	}
	return file.File.Truncate(size)
}

func TestWALWriteDirectChecksSizeChanges(t *testing.T) {
	const pageSize int64 = 128
	main, err := os.CreateTemp(t.TempDir(), "main")
	if err != nil {
		t.Fatal(err)
	}
	defer main.Close()
	if err := main.Truncate(9 * pageSize); err != nil {
		t.Fatal(err)
	}
	file := &directSizeFile{File: main}
	log := New(Config{PageSize: pageSize})
	log.fileKnownEmpty = true
	if _, err := log.ReadRecords(); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		head             page.ID
		pages            int64
		stats, truncates int
	}{{2, 3, 1, 1}, {2, 3, 1, 1}, {3, 4, 2, 1}, {3, 4, 2, 1}, {2, 3, 3, 2}, {2, 3, 3, 2}} {
		node := btree.NewLeafNode(step.head)
		if err := log.WriteDirect(file, step.pages*pageSize, false, []NodeRecord{{Final: node}}, nil, nil); err != nil {
			t.Fatal(err)
		}
		if file.stats != step.stats || file.truncates != step.truncates || log.directTargetSize != step.pages*pageSize {
			t.Fatalf("target %d: stats=%d truncates=%d saved=%d", step.pages, file.stats, file.truncates, log.directTargetSize)
		}
	}
	if info, err := main.Stat(); err != nil || info.Size() != 3*pageSize {
		t.Fatalf("final size: %v, %v", info, err)
	}
	fresh := New(Config{PageSize: pageSize})
	fresh.fileKnownEmpty = true
	if _, err := fresh.ReadRecords(); err != nil {
		t.Fatal(err)
	}
	if err := fresh.WriteDirect(file, 3*pageSize, false, []NodeRecord{{Final: btree.NewLeafNode(2)}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if file.stats != 4 {
		t.Fatal("new handle reused a previous file-size assumption")
	}
}

func TestWALWriteDirectFailureDropsKnownSize(t *testing.T) {
	const pageSize int64 = 128
	for _, fault := range []string{"write", "stat", "truncate"} {
		t.Run(fault, func(t *testing.T) {
			main, err := os.CreateTemp(t.TempDir(), "main")
			if err != nil {
				t.Fatal(err)
			}
			defer main.Close()
			file := &directSizeFile{File: main}
			log := New(Config{PageSize: pageSize})
			log.fileKnownEmpty = true
			if _, err := log.ReadRecords(); err != nil {
				t.Fatal(err)
			}
			if err := log.WriteDirect(file, 4*pageSize, false, []NodeRecord{{Final: btree.NewLeafNode(3)}}, nil, nil); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("injected " + fault + " failure")
			head, target := page.ID(3), 4*pageSize
			switch fault {
			case "write":
				file.writeErr = failure
			case "stat":
				head, target, file.statErr = 4, 5*pageSize, failure
			case "truncate":
				head, target, file.truncateErr = 2, 3*pageSize, failure
			}
			if err := log.WriteDirect(file, target, false, []NodeRecord{{Final: btree.NewLeafNode(head)}}, nil, nil); !errors.Is(err, failure) {
				t.Fatalf("fault error: %v", err)
			}
			if log.directTargetSize != 0 {
				t.Fatal("failed direct write kept a size assumption")
			}
			file.writeErr, file.statErr, file.truncateErr = nil, nil, nil
			before := file.stats
			if err := log.WriteDirect(file, target, false, []NodeRecord{{Final: btree.NewLeafNode(head)}}, nil, nil); err != nil {
				t.Fatal(err)
			}
			if file.stats != before+1 || log.directTargetSize != target {
				t.Fatal("retry did not check the file size")
			}
		})
	}
}

func TestWALWriteDirect(t *testing.T) {
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
	log := New(Config{File: file, PageSize: pageSize})
	if _, err := log.ReadRecords(); err != nil {
		t.Fatal(err)
	}
	node := btree.NewLeafNode(2)
	value := bytes.Repeat([]byte("x"), 300)
	if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), value)); err != nil {
		t.Fatal(err)
	}
	node.SetStoragePageCount(btree.NodePageCount(node, pageSize))
	size := int64(2+node.StoragePageCount()) * pageSize
	if err := log.WriteDirect(main, size, true, []NodeRecord{{Final: node}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(main.Name())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := btree.DecodeNode(data[2*pageSize:], 2, pageSize, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.EntryCount() != 1 || !bytes.Equal(decoded.EntryAt(0).Value(), value) {
		t.Fatal("direct node value differs")
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 || len(log.overlay) != 0 || log.Stats().TotalBytesWritten != 0 || log.Stats().CommitCount != 0 {
		t.Fatal("direct write used WAL")
	}
	overlap := btree.NewLeafNode(3)
	if err := log.WriteDirect(main, size, true, []NodeRecord{{Final: node}, {Final: overlap}}, nil, nil); err == nil {
		t.Fatal("overlapping spans accepted")
	}
	after, err := os.ReadFile(main.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, after) {
		t.Fatal("invalid spans changed main file")
	}
	log.LoadCommittedRecord(WALRecord{Header: RecordHeader{Type: RecordTypeNode, PageID: node.PageID()}, Payload: btree.EncodeWALNode(node)})
	if err := log.WriteDirect(main, size, true, []NodeRecord{{Final: node}}, nil, nil); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("stale overlay accepted: %v", err)
	}
}

type handledCheckpointWriter struct {
	calls int
	err   error
}

func (writer *handledCheckpointWriter) WriteNode(*btree.Node, int64, bool) (bool, error) {
	writer.calls++
	return true, writer.err
}

func TestWriteCheckpointRecordRuns_HandledNodesNeedNoBuffer(t *testing.T) {
	const pageSize int64 = 4096
	node := nativeLeaf(t, 50, 1<<20, pageSize)
	node.CacheEncodedSize()
	records := []CommittedPage{{Header: RecordHeader{Type: RecordTypeNode, PageID: node.PageID()}, Node: node}}
	writer := &handledCheckpointWriter{}
	var reusable []byte
	if allocations := testing.AllocsPerRun(3, func() {
		if err := writeCheckpointRecordRuns(writer, records, pageSize, false, &reusable); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("handled node allocated encoding storage: %.0f allocations", allocations)
	}
	if reusable != nil {
		t.Fatal("handled node grew retained encoding storage")
	}
	reusable = make([]byte, 0, 1)
	before := &reusable[:1][0]
	if err := writeCheckpointRecordRuns(writer, records, pageSize, false, &reusable); err != nil || cap(reusable) != 1 || &reusable[:1][0] != before {
		t.Fatal("handled node replaced an unused retained buffer")
	}
	writer.err = errors.New("node write failed")
	if err := writeCheckpointRecordRuns(writer, records, pageSize, false, &reusable); !errors.Is(err, writer.err) || cap(reusable) != 1 {
		t.Fatal("node error allocated encoding storage or changed the error")
	}
	writer.err, writer.calls = nil, 0
	overlap := append(slices.Clone(records), CommittedPage{Header: RecordHeader{Type: RecordTypeNode, PageID: node.PageID() + 1}, Node: btree.NewLeafNode(node.PageID() + 1)})
	if err := writeCheckpointRecordRuns(writer, overlap, pageSize, false, &reusable); err == nil || writer.calls != 0 {
		t.Fatal("overlap reached the direct node writer")
	}
	invalid := slices.Clone(records)
	invalid[0].Header.PageID++
	if err := writeCheckpointRecordRuns(writer, invalid, pageSize, false, &reusable); err == nil || writer.calls != 0 {
		t.Fatal("invalid node reached the direct node writer")
	}
}

type mixedCheckpointWriter struct {
	checkpointWriter
	handled map[page.ID]bool
	calls   []page.ID
}

func TestWriteCheckpointRecordRuns_MixedWritersKeepOrderAndReuseBuffer(t *testing.T) {
	const pageSize int64 = 64
	records := []CommittedPage{
		{Header: RecordHeader{Type: RecordTypeMeta, PageID: 0}, Payload: []byte("meta zero")},
		{Header: RecordHeader{Type: RecordTypeMeta, PageID: 1}, Payload: []byte("meta one")},
		{Header: RecordHeader{Type: RecordTypeAllocation, PageID: 2}, Payload: bytes.Repeat([]byte{2}, int(pageSize))},
		{Header: RecordHeader{Type: RecordTypeNode, PageID: 3}, Node: btree.NewLeafNode(3)},
		{Header: RecordHeader{Type: RecordTypeAllocation, PageID: 4}, Payload: bytes.Repeat([]byte{4}, int(pageSize))},
		{Header: RecordHeader{Type: RecordTypeAllocation, PageID: 5}, Payload: bytes.Repeat([]byte{5}, int(pageSize))},
		{Header: RecordHeader{Type: RecordTypeNode, PageID: 6}, Node: btree.NewLeafNode(6)},
		{Header: RecordHeader{Type: RecordTypeNode, PageID: 7}, Node: btree.NewLeafNode(7)},
		{Header: RecordHeader{Type: RecordTypeAllocation, PageID: 8}, Payload: bytes.Repeat([]byte{8}, int(pageSize))},
	}
	plain := &checkpointWriter{}
	if err := writeCheckpointRecordRuns(plain, records, pageSize, true, nil); err != nil {
		t.Fatal(err)
	}
	var reusable []byte
	var retained *byte
	for range 2 {
		writer := &mixedCheckpointWriter{handled: map[page.ID]bool{3: true, 7: true}}
		if err := writeCheckpointRecordRuns(writer, records, pageSize, true, &reusable); err != nil {
			t.Fatal(err)
		}
		if retained != nil && &reusable[:1][0] != retained {
			t.Fatal("mixed writes replaced a reusable buffer")
		}
		retained = &reusable[:1][0]
		var got []byte
		for _, write := range writer.writes {
			if write.offset != int64(len(got)) {
				t.Fatalf("write offset %d does not follow %d", write.offset, len(got))
			}
			got = append(got, write.data...)
		}
		if len(writer.writes) != 1 || len(writer.writes[0].data) != len(records)*int(pageSize) || len(plain.writes) != 1 || !bytes.Equal(got, plain.writes[0].data) {
			t.Fatal("mixed writers changed encoded bytes, order, or adjacent-page coalescing")
		}
	}
}

func (*handledCheckpointWriter) WriteAt([]byte, int64) (int, error) {
	return 0, errors.New("handled node reached buffered writer")
}

func (writer *mixedCheckpointWriter) WriteNode(node *btree.Node, pageSize int64, checksums bool) (bool, error) {
	writer.calls = append(writer.calls, node.PageID())
	if !writer.handled[node.PageID()] {
		return false, nil
	}
	data := make([]byte, int64(btree.NodePageCount(node, pageSize))*pageSize)
	if err := btree.EncodeNodeInto(data, node, pageSize, checksums); err != nil {
		return true, err
	}
	_, err := writer.WriteAt(data, int64(node.PageID())*pageSize)
	return true, err
}

func TestWriteCheckpointRecordRuns_MixesDirectAndBufferedNodes(t *testing.T) {
	const pageSize int64 = 64
	var records []CommittedPage
	for _, id := range []page.ID{3, 4, 5, 7} {
		records = append(records, CommittedPage{Header: RecordHeader{Type: RecordTypeNode, PageID: id}, Node: btree.NewLeafNode(id)})
	}
	plain := &checkpointWriter{}
	if err := writeCheckpointRecordRuns(plain, records, pageSize, true, nil); err != nil {
		t.Fatal(err)
	}
	writer := &mixedCheckpointWriter{handled: map[page.ID]bool{3: true, 5: true, 7: true}}
	if err := writeCheckpointRecordRuns(writer, records, pageSize, true, nil); err != nil {
		t.Fatal(err)
	}
	// Node 4 starts a buffered run. Node 5 must stay in that run even though
	// the direct writer could handle it. The gap before node 7 ends the run.
	if !slices.Equal(writer.calls, []page.ID{3, 4, 7}) || len(writer.writes) != 3 {
		t.Fatalf("direct calls=%v, file writes=%d", writer.calls, len(writer.writes))
	}
	images := func(writes []checkpointWrite) []byte {
		image := make([]byte, 8*pageSize)
		for _, write := range writes {
			copy(image[write.offset:], write.data)
		}
		return image
	}
	if !bytes.Equal(images(writer.writes), images(plain.writes)) {
		t.Fatal("direct and buffered output differ")
	}
}
