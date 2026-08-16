package kvlite

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	walrecord "github.com/Issaminu/kvlite/internal/wal"
)

type WAL struct {
	path                     string
	file                     *os.File
	pageSize                 int64
	syncOnCommit             bool
	checkpointThresholdBytes int64
	bytesSinceCheckpoint     int64
	collectedRecords         map[page.ID]walrecord.Record // Mapping Page ID to it's corresponding record. Only used temporarily within the current transaction to aggregate records that happen within a write operation, then flush at once
	overlay                  map[page.ID]walrecord.Record // Mapping that committed-but-not-yet-checkpointed pages, it's content comes from collectedRecords. This mapping lives beyond a single transaction
	nextTxid                 walrecord.TxID               // sequence number stamped on the next committed transaction
	hasUnsyncedWrites        bool                         // true when WAL bytes were appended after the last successful sync
	syncFile                 func() error                 // syncFile is an hook used exclusively for tests to determine deterministic sync failures and call counts.
}

func (wal *WAL) insertNodeRecord(node *Node) {
	record := walrecord.Record{
		Header:      walrecord.RecordHeader{Type: walrecord.RecordTypeData, PageID: node.PageID()},
		PageContent: btree.EncodeNode(node),
	}

	wal.collectRecord(&record)
}

func (wal *WAL) insertMetaRecord(meta *page.Meta) {
	meta.RefreshChecksum()

	record := walrecord.Record{
		Header:      walrecord.RecordHeader{Type: walrecord.RecordTypeMeta, PageID: page.MetaID},
		PageContent: page.EncodeMeta(meta),
	}

	wal.collectRecord(&record)
}

func (wal *WAL) collectRecord(record *walrecord.Record) {
	wal.collectedRecords[record.Header.PageID] = *record
}

func (wal *WAL) readRecords() ([]walrecord.Record, error) {
	if !wal.hasRecords() {
		return nil, nil
	}

	var records []walrecord.Record
	for {
		record, err := walrecord.DecodeRecord(wal.file, wal.pageSize)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			// if we ever encouner an ErrChecksum, it is returned as an err
			return nil, err
		}
		records = append(records, *record)
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

func (wal *WAL) sync() error {
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
func (wal *WAL) delete() error {
	// close the file before deletion
	err := wal.file.Close()
	if err != nil {
		return err
	}
	// delete the file from the disk
	err = os.Remove(wal.path)
	return err
}

func (wal *WAL) persistCollectedRecords() (bool, error) {
	if len(wal.collectedRecords) == 0 {
		return false, nil
	}

	transaction, err := wal.encodeCollectedRecords()
	if err != nil {
		return false, err
	}

	needsCheckpoint, err := wal.appendTransaction(transaction)
	if err != nil {
		return false, err
	}

	wal.moveCollectedRecordsToOverlay()
	return needsCheckpoint, nil
}

func (wal *WAL) encodeCollectedRecords() ([]byte, error) {
	transactionSize := walrecord.HeaderSize + walrecord.ChecksumSize // The commit marker has no page content.
	for _, record := range wal.collectedRecords {
		size, err := walrecord.EncodedRecordSize(&record, wal.pageSize)
		if err != nil {
			return nil, err
		}
		transactionSize += size
	}

	txid := wal.nextTxid
	wal.nextTxid++

	transaction := make([]byte, 0, transactionSize)

	for pgid, record := range wal.collectedRecords {
		record.Header.TxID = txid
		transaction = walrecord.AppendEncodedRecord(transaction, &record)

		wal.collectedRecords[pgid] = record
	}

	commitMarker := &walrecord.Record{
		Header:      walrecord.RecordHeader{Type: walrecord.RecordTypeCommit, PageID: 0, TxID: txid},
		PageContent: nil,
	}
	transaction = walrecord.AppendEncodedRecord(transaction, commitMarker)

	return transaction, nil
}

func (wal *WAL) appendTransaction(transaction []byte) (bool, error) {
	if err := fileio.WriteFull(wal.file, transaction); err != nil {
		return false, fmt.Errorf("persist multiple records: %w", err)
	}
	wal.hasUnsyncedWrites = true
	wal.bytesSinceCheckpoint += int64(len(transaction))

	needsCheckpoint := wal.reachedCheckpointThreshold()
	if wal.syncOnCommit || needsCheckpoint {
		if err := wal.sync(); err != nil {
			return false, err
		}
	}

	return needsCheckpoint, nil
}

func (wal *WAL) moveCollectedRecordsToOverlay() {
	for key := range wal.collectedRecords {
		wal.overlay[key] = wal.collectedRecords[key]
	}
	clear(wal.collectedRecords)
}

func (wal *WAL) reachedCheckpointThreshold() bool {
	return wal.bytesSinceCheckpoint >= wal.checkpointThresholdBytes
}

func recordToNode(record *walrecord.Record) (*Node, error) {
	if record.PageContent == nil {
		return nil, fmt.Errorf("record has no page content")
	}

	node, err := btree.DecodeNode(record.PageContent)

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node: %w", err)
	}

	return node, nil
}
