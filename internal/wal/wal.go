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
	syncFile                 func() error       // syncFile is an hook used exclusively for tests to determine deterministic sync failures and call counts.
}

// Commit appends one transaction and publishes each changed page to the committed overlay.
// It returns whether the committed WAL size reached the checkpoint threshold.
//
// On failure, Commit truncates the failed append and restores the pre-append byte
// and synchronization state.
func (wal *WAL) Commit(records []Record) (bool, error) {
	if len(records) == 0 {
		return false, nil
	}

	transaction, err := wal.encodeRecords(records, wal.nextTxid)
	if err != nil {
		return false, err
	}
	startOffset, err := wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return false, err
	}
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

	transaction := make([]byte, 0, transactionSize)
	for index := range records {
		transaction = AppendEncodedRecord(transaction, &records[index])
	}
	commitMarker := Record{Header: RecordHeader{Type: RecordTypeCommit, TxID: txid}}
	return AppendEncodedRecord(transaction, &commitMarker), nil
}

// rollbackAppend removes bytes from a failed commit and restores the counters that describe the retained WAL prefix.
func (wal *WAL) rollbackAppend(offset int64, bytesSinceCheckpoint uint64, hasUnsyncedWrites bool) error {
	wal.bytesSinceCheckpoint = bytesSinceCheckpoint
	wal.hasUnsyncedWrites = hasUnsyncedWrites
	if err := wal.file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate failed WAL transaction: %w", err)
	}
	if _, err := wal.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("rewind after failed WAL transaction: %w", err)
	}
	return nil
}

func (wal *WAL) ReadRecords() ([]Record, error) {
	if !wal.hasRecords() {
		return nil, nil
	}

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

func (wal *WAL) hasRecords() bool {
	fi, err := wal.file.Stat()
	if err != nil {
		return false
	}
	return fi.Size() > 0
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

// Truncate the WAL file.
// Important: only truncate the file after making sure that it's content has been ingested to the database
func (wal *WAL) Truncate() error {
	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := wal.file.Truncate(0); err != nil {
		_, seekErr := wal.file.Seek(0, io.SeekEnd)
		return errors.Join(err, seekErr)
	}
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

func (wal *WAL) appendTransaction(transaction []byte) (bool, error) {
	if err := fileio.WriteFull(wal.file, transaction); err != nil {
		return false, fmt.Errorf("persist multiple records: %w", err)
	}
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

	node, err := btree.DecodeNode(record.PageContent)

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node: %w", err)
	}

	return node, nil
}
