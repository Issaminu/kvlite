package wal

import (
	"fmt"
	"io"
	"maps"
	"os"

	"github.com/Issaminu/kvlite/internal/page"
)

type Config struct {
	Path                     string
	File                     *os.File
	PageSize                 int64
	SyncOnCommit             bool
	CheckpointThresholdBytes int64
}

func New(config Config) *WAL {
	return &WAL{
		path:                     config.Path,
		file:                     config.File,
		pageSize:                 config.PageSize,
		syncOnCommit:             config.SyncOnCommit,
		checkpointThresholdBytes: config.CheckpointThresholdBytes,
		collectedRecords:         make(map[page.ID]Record),
		overlay:                  make(map[page.ID]Record),
		nextTxid:                 1,
	}
}

type Snapshot struct {
	bytesSinceCheckpoint int64
	collectedRecords     map[page.ID]Record
	overlay              map[page.ID]Record
	fileOffset           int64
	nextTxid             TxID
	hasUnsyncedWrites    bool
}

func (wal *WAL) Snapshot() (Snapshot, error) {
	offset, err := wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{
		bytesSinceCheckpoint: wal.bytesSinceCheckpoint,
		collectedRecords:     maps.Clone(wal.collectedRecords),
		overlay:              maps.Clone(wal.overlay),
		fileOffset:           offset,
		nextTxid:             wal.nextTxid,
		hasUnsyncedWrites:    wal.hasUnsyncedWrites,
	}, nil
}

func (wal *WAL) Restore(snapshot Snapshot) error {
	wal.bytesSinceCheckpoint = snapshot.bytesSinceCheckpoint
	wal.collectedRecords = snapshot.collectedRecords
	wal.overlay = snapshot.overlay
	wal.nextTxid = snapshot.nextTxid
	if err := wal.file.Truncate(snapshot.fileOffset); err != nil {
		return fmt.Errorf("truncate failed WAL transaction: %w", err)
	}
	if _, err := wal.file.Seek(snapshot.fileOffset, io.SeekStart); err != nil {
		return fmt.Errorf("rewind after failed WAL transaction: %w", err)
	}
	wal.hasUnsyncedWrites = snapshot.hasUnsyncedWrites
	return nil
}

func (wal *WAL) Checkpoint(applyRecord func(*Record) error, syncMainFile func() error) error {
	if len(wal.overlay) == 0 {
		return nil
	}
	if err := wal.Sync(); err != nil {
		return err
	}
	for _, record := range wal.overlay {
		if err := applyRecord(&record); err != nil {
			return err
		}
	}
	if err := syncMainFile(); err != nil {
		return err
	}
	if err := wal.Truncate(); err != nil {
		// The main file is already durable. Keep the overlay so cleanup can be retried.
		return nil
	}
	clear(wal.overlay)
	wal.bytesSinceCheckpoint = 0
	return nil
}

func (wal *WAL) Lookup(pageID page.ID) (Record, bool) {
	record, ok := wal.collectedRecords[pageID]
	if !ok {
		record, ok = wal.overlay[pageID]
	}
	return record, ok
}

func (wal *WAL) LoadCommittedRecord(record Record) {
	wal.overlay[record.Header.PageID] = record
}

func (wal *WAL) CommittedRecord(pageID page.ID) (Record, bool) {
	record, ok := wal.overlay[pageID]
	return record, ok
}

func (wal *WAL) CollectedRecord(pageID page.ID) (Record, bool) {
	record, ok := wal.collectedRecords[pageID]
	return record, ok
}

type Stats struct {
	CheckpointThresholdBytes int64
	BytesSinceCheckpoint     int64
	CollectedRecordCount     int
	CommittedRecordCount     int
	NextTxID                 TxID
}

func (wal *WAL) Stats() Stats {
	return Stats{
		CheckpointThresholdBytes: wal.checkpointThresholdBytes,
		BytesSinceCheckpoint:     wal.bytesSinceCheckpoint,
		CollectedRecordCount:     len(wal.collectedRecords),
		CommittedRecordCount:     len(wal.overlay),
		NextTxID:                 wal.nextTxid,
	}
}

func (wal *WAL) CommittedRecords() map[page.ID]Record {
	return maps.Clone(wal.overlay)
}

func (wal *WAL) SetCheckpointThresholdBytes(threshold int64) {
	wal.checkpointThresholdBytes = threshold
}

// SetSyncFileForTesting replaces the WAL sync operation so tests can inject
// deterministic sync failures and count sync calls. Production code does not use it.
func (wal *WAL) SetSyncFileForTesting(syncFile func() error) {
	wal.syncFile = syncFile
}

// FileForTesting exposes the WAL file for tests that inject file-operation failures.
// Production code must use WAL methods instead.
func (wal *WAL) FileForTesting() *os.File {
	return wal.file
}

// ReplaceFileForTesting replaces the WAL file for tests that inject file-operation
// failures. Production code should not use it.
func (wal *WAL) ReplaceFileForTesting(file *os.File) {
	wal.file = file
}

// ClearCommittedRecordsForTesting removes the in-memory committed records so a
// recovery test can rebuild them from the WAL file. Production code should not use it.
func (wal *WAL) ClearCommittedRecordsForTesting() {
	clear(wal.overlay)
}

func (wal *WAL) Close() error {
	if wal.file == nil {
		return nil
	}
	return wal.file.Close()
}
