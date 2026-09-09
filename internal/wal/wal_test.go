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
	first := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 1},
		PageContent: []byte("first"),
	}}
	if _, err := log.Commit(first); err != nil {
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
	second := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 2},
		PageContent: []byte("second"),
	}}
	if _, err := log.Commit(second); err != nil {
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
	records := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 1},
		PageContent: []byte("value"),
	}}
	if _, err := log.Commit(records); !errors.Is(err, syncErr) {
		t.Fatalf("first commit error: got %v, want %v", err, syncErr)
	}
	if log.appendOffset != 0 {
		t.Fatalf("append offset after failed commit: got %d, want 0", log.appendOffset)
	}

	log.syncFile = nil
	if _, err := log.Commit(records); err != nil {
		t.Fatal(err)
	}
	info, err := walFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != log.appendOffset {
		t.Fatalf("WAL size after retry: got %d, append offset %d", info.Size(), log.appendOffset)
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
	records := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 1},
		PageContent: []byte("value"),
	}}
	if _, err := log.Commit(records); !errors.Is(err, syncErr) {
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
	records := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 1},
		PageContent: []byte("value"),
	}}
	if _, err := log.Commit(records); err != nil {
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
	if _, err := log.Commit(records); err != nil {
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

	records := []Record{{Header: RecordHeader{Type: RecordTypeData, PageID: 1}, PageContent: []byte("value")}}
	if _, err := log.Commit(records); !errors.Is(err, rollbackErr) {
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
	first := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 1},
		PageContent: []byte("first"),
	}}
	if _, err := firstLog.Commit(first); err != nil {
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
	if len(records) != 2 {
		t.Fatalf("recovered record count: got %d, want 2", len(records))
	}
	if recoveredLog.appendOffset != firstInfo.Size() {
		t.Fatalf("recovered append offset: got %d, want %d", recoveredLog.appendOffset, firstInfo.Size())
	}

	second := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 2},
		PageContent: []byte("second"),
	}}
	if _, err := recoveredLog.Commit(second); err != nil {
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

func TestWALEncodeRecords_ReusesTransactionBuffer(t *testing.T) {
	log := &WAL{
		pageSize: 64,
		nextTxid: 1,
	}
	records := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 1},
		PageContent: []byte("value"),
	}}
	if _, err := log.encodeRecords(records, log.nextTxid); err != nil {
		t.Fatal(err)
	}

	allocations := testing.AllocsPerRun(100, func() {
		if _, err := log.encodeRecords(records, log.nextTxid); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per warmed encoding: got %.0f, want 0", allocations)
	}
}

func TestWALCommit_DoesNotRetainLargeEncodingBuffer(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	const retentionLimit = 1 << 20
	log := New(Config{
		File:                     walFile,
		PageSize:                 2 << 20,
		CheckpointThresholdBytes: 4 << 20,
	})
	records := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 1},
		PageContent: make([]byte, retentionLimit+1),
	}}
	if _, err := log.Commit(records); err != nil {
		t.Fatal(err)
	}
	if capacity := cap(log.encodingBuffer); capacity > retentionLimit {
		t.Fatalf("retained encoding capacity: got %d, want at most %d", capacity, retentionLimit)
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
	records := []Record{{
		Header:      RecordHeader{Type: RecordTypeData, PageID: 1},
		PageContent: []byte("value"),
	}}
	if _, err := log.Commit(records); err != nil {
		t.Fatal(err)
	}

	allocations := testing.AllocsPerRun(100, func() {
		if _, err := log.Commit(records); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per warmed commit: got %.0f, want 0", allocations)
	}
}

func TestWALCommit_PublishesOneRecordForEachChangedPage(t *testing.T) {
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
	records := []Record{
		{Header: RecordHeader{Type: RecordTypeData, PageID: 2}, PageContent: []byte("second")},
		{Header: RecordHeader{Type: RecordTypeData, PageID: 1}, PageContent: []byte("first")},
	}

	needsCheckpoint, err := log.Commit(records)
	if err != nil {
		t.Fatal(err)
	}
	if needsCheckpoint {
		t.Fatal("small transaction reached the checkpoint threshold")
	}
	if got := log.Stats().CommittedRecordCount; got != 2 {
		t.Fatalf("committed record count: got %d, want 2", got)
	}
	record, ok := log.CommittedRecord(2)
	if !ok || !bytes.Equal(record.PageContent, []byte("second")) {
		t.Fatalf("page 2 record: found=%t content=%q", ok, record.PageContent)
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
	for index := range 32 {
		pageID := page.ID(32 - index)
		node := btree.NewLeafNode(pageID)
		if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), []byte{byte(pageID)})); err != nil {
			t.Fatal(err)
		}
		log.LoadCommittedRecord(Record{
			Header:      RecordHeader{Type: RecordTypeData, PageID: pageID},
			PageContent: btree.EncodeWALNode(node),
		})
	}

	if err := log.Checkpoint(mainFile); err != nil {
		t.Fatal(err)
	}
	for pageID := page.ID(1); pageID <= 32; pageID++ {
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

func TestRecordCodec_UsesFixedLittleEndianLayout(t *testing.T) {
	record := &Record{
		Header: RecordHeader{
			Type:   RecordTypeData,
			PageID: page.ID(1),
			TxID:   TxID(2),
		},
		PageContent: []byte("xy"),
	}
	// Layout: record type, page ID, transaction ID, content length, content,
	// and the CRC32C checksum of all preceding bytes.
	want := []byte{
		0,
		1, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0,
		'x', 'y',
	}
	wantChecksum := crc32.Checksum(want, crc32.MakeTable(crc32.Castagnoli))
	want = binary.LittleEndian.AppendUint32(want, wantChecksum)

	encoded, err := EncodeRecord(record, int64(len(record.PageContent)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encode WAL record: got %x, want %x", encoded, want)
	}

	decoded, err := DecodeRecord(bytes.NewReader(encoded), int64(len(record.PageContent)))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header != record.Header || !bytes.Equal(decoded.PageContent, record.PageContent) {
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
		pageSize      int64   = 64 // The record content must fit inside this page size.
		firstPageID   page.ID = 2  // Pages zero and one contain metadata, so node pages start at two.
		transactionID TxID    = 1  // A new WAL starts with transaction ID one.
	)

	log := &WAL{
		pageSize: pageSize,
		nextTxid: transactionID,
	}
	records := []Record{
		{
			Header:      RecordHeader{Type: RecordTypeData, PageID: firstPageID},
			PageContent: []byte("x"),
		},
	}

	transaction, err := log.encodeRecords(records, transactionID)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(transaction)
	dataRecord, err := DecodeRecord(reader, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	commitMarker, err := DecodeRecord(reader, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if dataRecord.Header.TxID != transactionID || commitMarker.Header.TxID != transactionID {
		t.Fatalf("transaction IDs: data=%d commit=%d, want %d", dataRecord.Header.TxID, commitMarker.Header.TxID, transactionID)
	}
	if !IsCommitMarker(commitMarker) {
		t.Fatal("encoded transaction does not end with a commit marker")
	}
}
