package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

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
		entry, found, err := node.FindEntry([]byte("key"))
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
		firstPageID   page.ID = 1  // Page zero contains metadata, so node pages start at one.
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
