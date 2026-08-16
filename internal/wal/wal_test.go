package wal

import (
	"bytes"
	"os"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

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
	// and the FNV-1a checksum of all preceding bytes.
	want := []byte{
		0,
		1, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0,
		'x', 'y',
		0xf1, 0x3c, 0x9c, 0xc5, 0x52, 0x74, 0xd3, 0x4f,
	}

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

func TestWAL_ByteCounterDoesNotWrapAtFourGiB(t *testing.T) {
	walFile, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	// The largest uint32 value is the boundary that the old WAL byte counter
	// could not cross.
	const largestUint32 = 1<<32 - 1
	log := &WAL{
		file:                     walFile,
		bytesSinceCheckpoint:     largestUint32 - 1,
		checkpointThresholdBytes: largestUint32,
	}

	// Two bytes move the counter from one byte below the boundary to one byte
	// above it.
	needsCheckpoint, err := log.appendTransaction([]byte{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if !needsCheckpoint {
		t.Fatal("WAL counter crossed 4 GiB without starting a checkpoint")
	}
	if got, want := uint64(log.bytesSinceCheckpoint), uint64(largestUint32)+1; got != want {
		t.Fatalf("WAL byte counter: got %d, want %d", got, want)
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
		collectedRecords: map[page.ID]Record{
			firstPageID: {
				Header:      RecordHeader{Type: RecordTypeData, PageID: firstPageID},
				PageContent: []byte("x"),
			},
		},
		nextTxid: transactionID,
	}

	transaction, err := log.encodeCollectedRecords()
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
