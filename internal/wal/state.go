package wal

import (
	"io"
	"maps"
	"os"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

// checkpointWriteBatchBytes limits temporary checkpoint memory and the size of one main-file write.
// A database page larger than this limit still requires one complete-page write.
const checkpointWriteBatchBytes int64 = 1 << 20

type Config struct {
	Path                     string
	File                     *os.File
	PageSize                 int64
	SyncOnCommit             bool
	SyncOnCheckpoint         bool
	CheckpointThresholdBytes uint64
}

func New(config Config) *WAL {
	return &WAL{
		path:                     config.Path,
		file:                     config.File,
		pageSize:                 config.PageSize,
		syncOnCommit:             config.SyncOnCommit,
		syncOnCheckpoint:         config.SyncOnCheckpoint,
		checkpointThresholdBytes: config.CheckpointThresholdBytes,
		overlay:                  make(map[page.ID]Record),
		nextTxid:                 1,
	}
}

// Checkpoint copies the committed overlay into mainFile and then clears the WAL contents.
// It borrows mainFile and does not close it.
// When [Config.SyncOnCheckpoint] is true, Checkpoint syncs the WAL before the copy and syncs mainFile before cleanup. When it is false, Checkpoint does not issue these sync calls.
// A main-file write or required sync failure leaves the overlay and WAL available for a later retry.
// A WAL truncate failure leaves the overlay in memory for cleanup retry but does not make the committed transaction fail.
func (wal *WAL) Checkpoint(mainFile *os.File) error {
	if len(wal.overlay) == 0 {
		return nil
	}
	if wal.syncOnCheckpoint {
		if err := wal.Sync(); err != nil {
			return err
		}
	}
	records := make([]Record, 0, len(wal.overlay))
	for _, record := range wal.overlay {
		records = append(records, record)
	}
	// Sorting lets the writer combine adjacent pages into one I/O operation.
	slices.SortFunc(records, func(left, right Record) int {
		switch {
		case left.Header.PageID < right.Header.PageID:
			return -1
		case left.Header.PageID > right.Header.PageID:
			return 1
		default:
			return 0
		}
	})
	if err := writeCheckpointRecordRuns(mainFile, records, wal.pageSize); err != nil {
		return err
	}
	if wal.syncOnCheckpoint {
		if err := fileio.SyncData(mainFile); err != nil {
			return err
		}
	}
	if err := wal.Truncate(); err != nil {
		// Keep the overlay so WAL cleanup can be retried.
		return nil
	}
	clear(wal.overlay)
	wal.bytesSinceCheckpoint = 0
	return nil
}

// writeCheckpointRecordRuns combines adjacent, page-ID-ordered records into bounded writes.
// records must contain unique page IDs in ascending order.
// It pads each WAL image to pageSize because the WAL omits unused page bytes but the main file stores fixed-size pages.
func writeCheckpointRecordRuns(mainFile io.WriterAt, records []Record, pageSize int64) error {
	if len(records) == 0 {
		return nil
	}
	for index := range records {
		record := &records[index]
		if int64(len(record.PageContent)) > pageSize {
			return page.ErrInvalid
		}
		if record.Header.Type == RecordTypeData {
			if err := btree.ValidateWALNode(record.PageContent, record.Header.PageID); err != nil {
				return err
			}
		}
	}

	pagesPerBatch := max(checkpointWriteBatchBytes/pageSize, 1)
	if pagesPerBatch > int64(len(records)) {
		pagesPerBatch = int64(len(records))
	}
	buffer := make([]byte, 0, pagesPerBatch*pageSize)
	startPageID := records[0].Header.PageID
	previousPageID := startPageID

	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		offset := int64(startPageID) * pageSize
		if err := fileio.WriteFull(io.NewOffsetWriter(mainFile, offset), buffer); err != nil {
			return err
		}
		buffer = buffer[:0]
		return nil
	}

	for index := range records {
		record := &records[index]
		pageID := record.Header.PageID
		// A page-ID gap ends the run. Writing across the gap would replace unchanged pages with zeros.
		// The capacity check bounds a long contiguous run without changing its file offsets.
		if len(buffer) > 0 && (pageID != previousPageID+1 || int64(len(buffer))+pageSize > int64(cap(buffer))) {
			if err := flush(); err != nil {
				return err
			}
		}
		if len(buffer) == 0 {
			startPageID = pageID
		}

		pageStart := len(buffer)
		pageEnd := pageStart + int(pageSize)

		// resize buffer to fit the new pageContent
		buffer = buffer[:pageEnd]

		pageData := buffer[pageStart:pageEnd]
		clear(pageData)
		copy(pageData, record.PageContent)
		if record.Header.Type == RecordTypeData {
			if err := btree.VerifyNodeIDAndSetChecksum(pageData, pageID); err != nil {
				return err
			}
		}
		previousPageID = pageID
	}
	return flush()
}

func (wal *WAL) Lookup(pageID page.ID) (Record, bool) {
	record, ok := wal.overlay[pageID]
	return record, ok
}

func (wal *WAL) LoadCommittedRecord(record Record) {
	wal.overlay[record.Header.PageID] = record
}

func (wal *WAL) CommittedRecord(pageID page.ID) (Record, bool) {
	record, ok := wal.overlay[pageID]
	return record, ok
}

type Stats struct {
	CheckpointThresholdBytes uint64
	BytesSinceCheckpoint     uint64
	CommittedRecordCount     int
	NextTxID                 TxID
}

func (wal *WAL) Stats() Stats {
	return Stats{
		CheckpointThresholdBytes: wal.checkpointThresholdBytes,
		BytesSinceCheckpoint:     wal.bytesSinceCheckpoint,
		CommittedRecordCount:     len(wal.overlay),
		NextTxID:                 wal.nextTxid,
	}
}

func (wal *WAL) CommittedRecords() map[page.ID]Record {
	return maps.Clone(wal.overlay)
}

func (wal *WAL) SetCheckpointThresholdBytes(threshold uint64) {
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

// ReplaceFileForTesting replaces the WAL file for tests that inject file-operation failures.
// Production code should not use it.
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
