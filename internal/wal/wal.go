package wal

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

// maxRetainedEncodingBufferBytes is the largest transaction encoding buffer kept across commits.
// Larger buffers are dropped after [WAL.Commit] returns so one large transaction does not retain memory.
const maxRetainedEncodingBufferBytes = 1 << 20

type WAL struct {
	path                     string
	file                     *os.File
	pageSize                 int64
	syncOnCommit             bool
	checkpointThresholdBytes uint64
	bytesSinceCheckpoint     uint64
	overlay                  map[page.ID]Record // Committed pages that are not yet checkpointed.
	nextTxid                 TxID               // sequence number stamped on the next committed transaction
	hasUnsyncedWrites        bool               // true when WAL bytes were appended after the last successful sync
	appendOffset             int64              // byte offset of the next WAL append; independent of the file read/write cursor
	encodingBuffer           []byte             // reusable transaction encoding buffer; capacity may be cleared after large commits
	appendFailure            error              // set when rollback truncate fails; blocks later commits until [WAL.Truncate] succeeds
	syncFile                 func() error       // syncFile is an hook used exclusively for tests to determine deterministic sync failures and call counts.
}

// Commit appends one transaction and publishes each changed page to the committed overlay.
// It returns whether the committed WAL size reached the checkpoint threshold.
//
// Appends use [WAL.appendOffset], not the WAL file read/write cursor, so unrelated seeks on the file cannot corrupt the log.
//
// On failure, Commit truncates the failed append and restores the pre-append byte and synchronization state without updating the overlay.
// If that rollback truncate fails, Commit stores the truncate error and later Commit calls return it until [WAL.Truncate] succeeds.
func (wal *WAL) Commit(records []Record) (bool, error) {
	if wal.appendFailure != nil {
		return false, fmt.Errorf("WAL append is disabled after failed rollback: %w", wal.appendFailure)
	}
	if len(records) == 0 {
		return false, nil
	}

	transaction, err := wal.encodeRecords(records, wal.nextTxid)
	if err != nil {
		return false, err
	}
	defer wal.releaseEncodingBuffer()
	startOffset := wal.appendOffset
	bytesBefore := wal.bytesSinceCheckpoint
	unsyncedBefore := wal.hasUnsyncedWrites

	needsCheckpoint, err := wal.appendTransaction(transaction)
	if err != nil {
		rollbackErr := wal.rollbackAppend(startOffset, bytesBefore, unsyncedBefore)
		return false, errors.Join(err, rollbackErr)
	}

	for _, record := range records {
		wal.overlay[record.Header.PageID] = record
	}
	wal.nextTxid++
	return needsCheckpoint, nil
}

// releaseEncodingBuffer drops an oversized transaction encoding buffer after Commit returns.
func (wal *WAL) releaseEncodingBuffer() {
	if cap(wal.encodingBuffer) > maxRetainedEncodingBufferBytes {
		wal.encodingBuffer = nil
	}
}

func (wal *WAL) encodeRecords(records []Record, txid TxID) ([]byte, error) {
	transactionSize := HeaderSize + ChecksumSize
	for index := range records {
		records[index].Header.TxID = txid
		size, err := EncodedRecordSize(&records[index], wal.pageSize)
		if err != nil {
			return nil, err
		}
		transactionSize += size
	}

	transaction := wal.encodingBuffer[:0]
	if cap(transaction) < transactionSize {
		transaction = make([]byte, 0, transactionSize)
	}
	for index := range records {
		transaction = AppendEncodedRecord(transaction, &records[index])
	}
	commitMarker := Record{Header: RecordHeader{Type: RecordTypeCommit, TxID: txid}}
	transaction = AppendEncodedRecord(transaction, &commitMarker)
	wal.encodingBuffer = transaction[:0]
	return transaction, nil
}

// rollbackAppend removes bytes from a failed commit and restores the counters that describe the retained WAL prefix.
// A truncate failure leaves [WAL.appendFailure] set and blocks later appends until [WAL.Truncate] succeeds.
func (wal *WAL) rollbackAppend(offset int64, bytesSinceCheckpoint uint64, hasUnsyncedWrites bool) error {
	if err := wal.file.Truncate(offset); err != nil {
		wal.appendFailure = fmt.Errorf("truncate failed WAL transaction: %w", err)
		return wal.appendFailure
	}
	wal.bytesSinceCheckpoint = bytesSinceCheckpoint
	wal.hasUnsyncedWrites = hasUnsyncedWrites
	wal.appendOffset = offset
	return nil
}

// ReadRecords decodes every record currently stored in the WAL file.
// It returns nil, nil when no WAL file is configured or the file is empty.
// On success it sets [WAL.appendOffset] to the file size so a later [WAL.Commit] appends after the recovered prefix.
func (wal *WAL) ReadRecords() ([]Record, error) {
	if wal.file == nil {
		return nil, nil
	}
	info, err := wal.file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, nil
	}
	wal.appendOffset = info.Size()

	var records []Record
	for {
		record, err := DecodeRecord(wal.file, wal.pageSize)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			// if we ever encouner an ErrChecksum, it is returned as an err
			return nil, err
		}
		records = append(records, *record)
		if record.Header.TxID >= wal.nextTxid {
			wal.nextTxid = record.Header.TxID + 1
		}
	}

	return records, nil
}

func (wal *WAL) Sync() error {
	if !wal.hasUnsyncedWrites {
		return nil
	}

	var err error
	if wal.syncFile != nil {
		err = wal.syncFile()
	} else {
		// Production leaves syncFile nil and syncs the WAL file directly.
		err = wal.file.Sync()
	}
	if err != nil {
		return err
	}
	wal.hasUnsyncedWrites = false
	return nil
}

// Truncate clears the WAL file.
// It resets [WAL.appendOffset] and clears [WAL.appendFailure] so appends can resume after a failed rollback truncate.
//
// Only truncate after the WAL contents have been ingested into the database.
func (wal *WAL) Truncate() error {
	if err := wal.file.Truncate(0); err != nil {
		return err
	}
	wal.appendOffset = 0
	wal.appendFailure = nil
	return nil
}

// Delete the WAL file
// Important: only delete the file when running db.Close()
func (wal *WAL) Delete() error {
	// close the file before deletion
	err := wal.file.Close()
	if err != nil {
		return err
	}
	// delete the file from the disk
	err = os.Remove(wal.path)
	return err
}

// appendTransaction persists transaction with [fileio.WriteFullAt] at [WAL.appendOffset] and advances WAL size counters.
func (wal *WAL) appendTransaction(transaction []byte) (bool, error) {
	if err := fileio.WriteFullAt(wal.file, transaction, wal.appendOffset); err != nil {
		return false, fmt.Errorf("persist multiple records: %w", err)
	}
	wal.appendOffset += int64(len(transaction))
	wal.hasUnsyncedWrites = true
	wal.bytesSinceCheckpoint += uint64(len(transaction))

	needsCheckpoint := wal.reachedCheckpointThreshold()
	if wal.syncOnCommit || needsCheckpoint {
		if err := wal.Sync(); err != nil {
			return false, err
		}
	}

	return needsCheckpoint, nil
}

func (wal *WAL) reachedCheckpointThreshold() bool {
	return wal.bytesSinceCheckpoint >= wal.checkpointThresholdBytes
}

func RecordToNode(record *Record) (*btree.Node, error) {
	if record.PageContent == nil {
		return nil, fmt.Errorf("record has no page content")
	}

	node, err := btree.DecodeWALNode(record.PageContent, record.Header.PageID)

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node: %w", err)
	}

	return node, nil
}
