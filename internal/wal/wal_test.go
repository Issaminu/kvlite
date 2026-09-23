package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

func TestWALCommit_DoesNotUseCurrentFilePosition(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	first := []byte("first")
	if _, err := log.Commit(nil, first); err != nil {
		t.Fatal(err)
	}
	firstSize, err := walFile.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}

	const unrelatedPosition = int64(3)
	if _, err := walFile.Seek(unrelatedPosition, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	second := []byte("second")
	if _, err := log.Commit(nil, second); err != nil {
		t.Fatal(err)
	}

	position, err := walFile.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if position != unrelatedPosition {
		t.Fatalf("file position after commit: got %d, want %d", position, unrelatedPosition)
	}
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= firstSize {
		t.Fatalf("WAL size after second commit: got %d, want more than %d", info.Size(), firstSize)
	}
}

func TestWALCommit_SyncFailureRestoresAppendOffset(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		SyncOnCommit:             true,
		CheckpointThresholdBytes: 1 << 20,
	})
	syncErr := errors.New("injected sync failure")
	log.syncFile = func() error { return syncErr }
	encodedMeta := []byte("value")
	if _, err := log.Commit(nil, encodedMeta); !errors.Is(err, syncErr) {
		t.Fatalf("first commit error: got %v, want %v", err, syncErr)
	}
	if log.appendOffset != 0 {
		t.Fatalf("append offset after failed commit: got %d, want 0", log.appendOffset)
	}
	if got := log.Stats().TotalBytesWritten; got != 0 {
		t.Fatalf("total WAL bytes after failed commit: got %d, want 0", got)
	}

	log.syncFile = nil
	if _, err := log.Commit(nil, encodedMeta); err != nil {
		t.Fatal(err)
	}
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != log.appendOffset {
		t.Fatalf("WAL size after retry: got %d, append offset %d", info.Size(), log.appendOffset)
	}
	if got := log.Stats().TotalBytesWritten; got != uint64(info.Size()) {
		t.Fatalf("total WAL bytes after retry: got %d, want %d", got, info.Size())
	}
}

func TestWALCommit_SyncFailureDoesNotUseFilePosition(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		SyncOnCommit:             true,
		CheckpointThresholdBytes: 1 << 20,
	})
	syncErr := errors.New("injected sync failure")
	log.syncFile = func() error { return syncErr }
	const unrelatedPosition = int64(3)
	if _, err := walFile.Seek(unrelatedPosition, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	encodedMeta := []byte("value")
	if _, err := log.Commit(nil, encodedMeta); !errors.Is(err, syncErr) {
		t.Fatalf("commit error: got %v, want %v", err, syncErr)
	}
	position, err := walFile.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if position != unrelatedPosition {
		t.Fatalf("file position after rollback: got %d, want %d", position, unrelatedPosition)
	}
}

func TestWALTruncate_ResetsAppendOffset(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	encodedMeta := []byte("value")
	if _, err := log.Commit(nil, encodedMeta); err != nil {
		t.Fatal(err)
	}
	if log.appendOffset == 0 {
		t.Fatal("commit did not advance append offset")
	}

	if err := log.Truncate(); err != nil {
		t.Fatal(err)
	}
	if log.appendOffset != 0 {
		t.Fatalf("append offset after truncate: got %d, want 0", log.appendOffset)
	}
	if _, err := log.Commit(nil, encodedMeta); err != nil {
		t.Fatal(err)
	}
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != log.appendOffset {
		t.Fatalf("WAL size after truncate and commit: got %d, append offset %d", info.Size(), log.appendOffset)
	}
}

func TestWALTruncate_FailureKeepsAppendOffset(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	const appendOffset = int64(73)
	log := &WAL{file: walFile, appendOffset: appendOffset}
	if err := walFile.Close(); err != nil {
		t.Fatal(err)
	}

	if err := log.Truncate(); err == nil {
		t.Fatal("truncate on closed WAL succeeded")
	}
	if log.appendOffset != appendOffset {
		t.Fatalf("append offset after failed truncate: got %d, want %d", log.appendOffset, appendOffset)
	}
}

func TestWALFailedRollbackBlocksLaterCommits(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	log := New(Config{File: walFile, PageSize: 64})
	if err := walFile.Close(); err != nil {
		t.Fatal(err)
	}
	rollbackErr := log.rollbackAppend(0, 0, false)
	if rollbackErr == nil {
		t.Fatal("rollback on closed WAL succeeded")
	}

	if _, err := log.Commit(nil, []byte("value")); !errors.Is(err, rollbackErr) {
		t.Fatalf("commit after failed rollback: got %v, want rollback error %v", err, rollbackErr)
	}
}

func TestWALReadRecords_InitializesAppendOffsetForRecovery(t *testing.T) {
	path := t.TempDir() + "/wal"
	walFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	firstLog := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	first := []byte("first")
	if _, err := firstLog.Commit(nil, first); err != nil {
		t.Fatal(err)
	}
	firstInfo, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := walFile.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	recoveredLog := New(Config{
		File:                     reopenedFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	records, err := recoveredLog.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("recovered record count: got %d, want 3", len(records))
	}
	if recoveredLog.appendOffset != firstInfo.Size() {
		t.Fatalf("recovered append offset: got %d, want %d", recoveredLog.appendOffset, firstInfo.Size())
	}

	second := []byte("second")
	if _, err := recoveredLog.Commit(nil, second); err != nil {
		t.Fatal(err)
	}
	secondInfo, err := reopenedFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if secondInfo.Size() <= firstInfo.Size() {
		t.Fatalf("WAL size after recovered append: got %d, want more than %d", secondInfo.Size(), firstInfo.Size())
	}
}

func TestWALReadRecords_ReturnsStatError(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	log := New(Config{File: walFile, PageSize: 64})
	if err := walFile.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := log.ReadRecords(); err == nil {
		t.Fatal("read records on closed WAL returned no error")
	}
}

func TestWALReadRecords_AllowsMissingReadOnlyWAL(t *testing.T) {
	log := New(Config{PageSize: 4096})
	records, err := log.ReadRecords()
	if err != nil {
		t.Fatalf("read records without WAL file: %v", err)
	}
	if records != nil {
		t.Fatalf("records without WAL file: got %v, want nil", records)
	}
}

func TestWALEncodeTransaction_ReusesBuffer(t *testing.T) {
	log := &WAL{
		pageSize: 64,
		nextTxid: 1,
	}
	encodedMeta := []byte("value")
	if _, err := log.encodeTransaction(nil, encodedMeta, log.nextTxid); err != nil {
		t.Fatal(err)
	}

	allocations := testing.AllocsPerRun(100, func() {
		if _, err := log.encodeTransaction(nil, encodedMeta, log.nextTxid); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per warmed encoding: got %.0f, want 0", allocations)
	}
}

func TestWALEncodeTransaction_ReusesNodePatchBuffers(t *testing.T) {
	base := btree.NewLeafNode(2)
	if err := base.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("old-value"))); err != nil {
		t.Fatal(err)
	}
	target := base.Clone()
	if err := target.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("new-value"))); err != nil {
		t.Fatal(err)
	}

	log := &WAL{pageSize: 128, nextTxid: 1}
	nodes := []NodeRecord{{Original: base, Final: target}}
	if _, err := log.encodeTransaction(nodes, nil, log.nextTxid); err != nil {
		t.Fatal(err)
	}

	allocations := testing.AllocsPerRun(100, func() {
		if _, err := log.encodeTransaction(nodes, nil, log.nextTxid); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per warmed node-patch encoding: got %.0f, want 0", allocations)
	}
}

func TestWALCommit_EncodesSmallerPagePatchAndPublishesFinalNode(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	base := btree.NewLeafNode(2)
	if err := base.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("old-value"))); err != nil {
		t.Fatal(err)
	}
	target := btree.NewLeafNode(2)
	if err := target.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("new-value"))); err != nil {
		t.Fatal(err)
	}
	log := New(Config{File: walFile, PageSize: 128, CheckpointThresholdBytes: 1 << 20})
	nodes := []NodeRecord{{
		Original: base,
		Final:    target,
	}}
	if _, err := log.Commit(nodes, nil); err != nil {
		t.Fatal(err)
	}

	decoded, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 || decoded[0].Header.Type != RecordTypePatch {
		t.Fatalf("decoded WAL records: got %+v, want one page patch and one commit marker", decoded)
	}
	patched, err := ApplyPagePatch(btree.EncodeWALNode(base), decoded[0].Payload, 128)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(patched, btree.EncodeWALNode(target)) {
		t.Fatal("page patch did not rebuild the final node")
	}
	published, ok := log.Lookup(target.PageID())
	if !ok || published.Node != target || published.Payload != nil {
		t.Fatal("committed overlay did not keep only the final node")
	}
}

func TestWALCommit_UsesFullImageWhenEntryCountChanges(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	base := btree.NewLeafNode(2)
	if err := base.InsertEntry(btree.NewEntry(0, []byte("key"), []byte("old"))); err != nil {
		t.Fatal(err)
	}
	target := base.Clone()
	if err := target.InsertEntry(btree.NewEntry(0, []byte("next"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	log := New(Config{File: walFile, PageSize: 128, CheckpointThresholdBytes: 1 << 20})
	if _, err := log.Commit([]NodeRecord{{
		Original: base,
		Final:    target,
	}}, nil); err != nil {
		t.Fatal(err)
	}

	decoded, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 || decoded[0].Header.Type != RecordTypeNode {
		t.Fatalf("decoded WAL records: got %+v, want one full image and one commit marker", decoded)
	}
	if err := btree.ValidateWALNode(decoded[0].Payload, target.PageID()); err != nil {
		t.Fatalf("validate complete node image: %v", err)
	}
}

func TestWALCommit_EncodesPatchForBranchNode(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	base := btree.NewRootNode(2, btree.NewLeafNode(3), btree.NewLeafNode(4), []byte("middle"))
	target := base.Clone()
	target.Children[1] = 5
	log := New(Config{File: walFile, PageSize: 128, CheckpointThresholdBytes: 1 << 20})
	if _, err := log.Commit([]NodeRecord{{Original: base, Final: target}}, nil); err != nil {
		t.Fatal(err)
	}

	decoded, err := log.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 || decoded[0].Header.Type != RecordTypePatch {
		t.Fatalf("decoded WAL records: got %+v, want one page patch and one commit marker", decoded)
	}
	patched, err := ApplyPagePatch(btree.EncodeWALNode(base), decoded[0].Payload, 128)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(patched, btree.EncodeWALNode(target)) {
		t.Fatal("page patch did not rebuild the final branch node")
	}
}

func TestWALCommit_DoesNotRetainLargeEncodingBuffer(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	const retentionLimit = maxRetainedEncodingBufferBytes
	log := New(Config{
		File:                     walFile,
		PageSize:                 4 << 20,
		CheckpointThresholdBytes: 4 << 20,
	})
	encodedMeta := make([]byte, retentionLimit+1)
	if _, err := log.Commit(nil, encodedMeta); err != nil {
		t.Fatal(err)
	}
	if capacity := cap(log.encodingBuffer); capacity > retentionLimit {
		t.Fatalf("retained encoding capacity: got %d, want at most %d", capacity, retentionLimit)
	}
}

func TestWALCommit_RetainsBenchmarkSizedEncodingBuffer(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	const transactionBytes = 1400 << 10
	log := New(Config{
		File:                     walFile,
		PageSize:                 transactionBytes,
		CheckpointThresholdBytes: 4 << 20,
	})
	const recordOverhead = HeaderSize + ChecksumSize
	encodedMeta := make([]byte, (transactionBytes-3*recordOverhead+1)/2)
	if _, err := log.Commit(nil, encodedMeta); err != nil {
		t.Fatal(err)
	}
	if capacity := cap(log.encodingBuffer); capacity < transactionBytes || capacity > maxRetainedEncodingBufferBytes {
		t.Fatalf("retained encoding capacity: got %d, want %d..%d", capacity, transactionBytes, maxRetainedEncodingBufferBytes)
	}
}

func TestWALCommit_ReusesAllTransactionStorage(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: ^uint64(0),
	})
	encodedMeta := []byte("value")
	if _, err := log.Commit(nil, encodedMeta); err != nil {
		t.Fatal(err)
	}

	allocations := testing.AllocsPerRun(100, func() {
		if _, err := log.Commit(nil, encodedMeta); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per warmed commit: got %.0f, want 0", allocations)
	}
}

func TestWALCommit_PublishesBothMetadataPages(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	log := New(Config{
		File:                     walFile,
		PageSize:                 64,
		CheckpointThresholdBytes: 1 << 20,
	})
	encodedMeta := []byte("metadata")

	needsCheckpoint, err := log.Commit(nil, encodedMeta)
	if err != nil {
		t.Fatal(err)
	}
	if needsCheckpoint {
		t.Fatal("small transaction reached the checkpoint threshold")
	}
	stats := log.Stats()
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalBytesWritten != uint64(info.Size()) {
		t.Fatalf("total WAL bytes written: got %d, want %d", stats.TotalBytesWritten, info.Size())
	}
	if got := stats.CommittedRecordCount; got != 2 {
		t.Fatalf("committed record count: got %d, want 2", got)
	}
	for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
		record, ok := log.CommittedRecord(pageID)
		if !ok || record.Header.Type != RecordTypeMeta || !bytes.Equal(record.Payload, encodedMeta) {
			t.Fatalf("metadata page %d: found=%t record=%+v", pageID, ok, record)
		}
	}
}

func TestWALCheckpoint_WritesCommittedPagesToMainFile(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()
	mainFile, err := os.CreateTemp(t.TempDir(), "main")
	if err != nil {
		t.Fatal(err)
	}
	defer mainFile.Close()

	log := New(Config{File: walFile, PageSize: 64})
	nodes := make([]NodeRecord, 0, 32)
	for index := range 32 {
		pageID := page.ID(33 - index)
		node := btree.NewLeafNode(pageID)
		if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte{byte(pageID)})); err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, NodeRecord{Final: node})
	}
	if _, err := log.Commit(nodes, nil); err != nil {
		t.Fatal(err)
	}
	writtenBytes := log.Stats().TotalBytesWritten

	if err := log.Checkpoint(mainFile); err != nil {
		t.Fatal(err)
	}
	stats := log.Stats()
	if stats.TotalBytesWritten != writtenBytes {
		t.Fatalf("total WAL bytes after checkpoint: got %d, want %d", stats.TotalBytesWritten, writtenBytes)
	}
	if got := stats.CheckpointCount; got != 1 {
		t.Fatalf("checkpoint count: got %d, want 1", got)
	}
	if err := log.Checkpoint(mainFile); err != nil {
		t.Fatal(err)
	}
	if got := log.Stats().CheckpointCount; got != 1 {
		t.Fatalf("checkpoint count after empty checkpoint: got %d, want 1", got)
	}
	for pageID := page.ID(2); pageID <= 33; pageID++ {
		pageContent := make([]byte, 64)
		if _, err := mainFile.ReadAt(pageContent, int64(pageID)*64); err != nil {
			t.Fatal(err)
		}
		node, err := btree.DecodeNode(pageContent, pageID, 64)
		if err != nil {
			t.Fatalf("decode checkpoint page %d: %v", pageID, err)
		}
		entry, found, err := node.FindEntryRef([]byte("key"))
		if err != nil || !found || !bytes.Equal(entry.Value(), []byte{byte(pageID)}) {
			t.Fatalf("checkpoint page %d entry: found=%t value=%v err=%v", pageID, found, entry.Value(), err)
		}
	}
}

func TestWALRecordCodec_UsesFixedLittleEndianLayout(t *testing.T) {
	record := &WALRecord{
		Header: RecordHeader{
			Type:   RecordTypeNode,
			PageID: page.ID(1),
			TxID:   TxID(2),
		},
		Payload: []byte("xy"),
	}
	// Layout: record type, page ID, transaction ID, content length, content,
	// and the CRC32C checksum of all preceding bytes.
	want := []byte{
		2,
		1, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0,
		'x', 'y',
	}
	wantChecksum := crc32.Checksum(want, crc32.MakeTable(crc32.Castagnoli))
	want = binary.LittleEndian.AppendUint32(want, wantChecksum)

	encoded, err := EncodeWALRecord(record, int64(len(record.Payload)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encode WAL record: got %x, want %x", encoded, want)
	}

	decoded, err := DecodeWALRecord(bytes.NewReader(encoded), int64(len(record.Payload)))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header != record.Header || !bytes.Equal(decoded.Payload, record.Payload) {
		t.Fatalf("decode WAL record: got %+v, want %+v", decoded, record)
	}
}

func TestWAL_ByteCounterDoesNotWrapAtMaxInt64(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	const largestInt64 = 1<<63 - 1
	log := &WAL{
		file:                     walFile,
		bytesSinceCheckpoint:     largestInt64 - 1,
		checkpointThresholdBytes: largestInt64,
	}

	// Two bytes move the counter from one byte below the signed boundary to one
	// byte above it.
	needsCheckpoint, err := log.appendTransaction([]byte{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if !needsCheckpoint {
		t.Fatal("WAL counter crossed MaxInt64 without starting a checkpoint")
	}
	if log.bytesSinceCheckpoint <= largestInt64 {
		t.Fatalf("WAL byte counter wrapped to %d", log.bytesSinceCheckpoint)
	}
}

func TestWAL_EncodesTransactionWithoutDatabase(t *testing.T) {
	const (
		pageSize      int64 = 64 // The record content must fit inside this page size.
		transactionID TxID  = 1  // A new WAL starts with transaction ID one.
	)

	log := &WAL{
		pageSize: pageSize,
		nextTxid: transactionID,
	}
	transaction, err := log.encodeTransaction(nil, []byte("meta"), transactionID)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(transaction)
	firstMeta, err := DecodeWALRecord(reader, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	secondMeta, err := DecodeWALRecord(reader, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	commitMarker, err := DecodeWALRecord(reader, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if firstMeta.Header.TxID != transactionID || secondMeta.Header.TxID != transactionID || commitMarker.Header.TxID != transactionID {
		t.Fatalf("transaction IDs: first metadata=%d second metadata=%d commit=%d, want %d", firstMeta.Header.TxID, secondMeta.Header.TxID, commitMarker.Header.TxID, transactionID)
	}
	if firstMeta.Header.Type != RecordTypeMeta || firstMeta.Header.PageID != page.Meta0ID || secondMeta.Header.Type != RecordTypeMeta || secondMeta.Header.PageID != page.Meta1ID {
		t.Fatalf("metadata records: first=%+v second=%+v", firstMeta.Header, secondMeta.Header)
	}
	if !IsCommitMarker(commitMarker) {
		t.Fatal("encoded transaction does not end with a commit marker")
	}
}
