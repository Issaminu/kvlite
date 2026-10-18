package wal

import (
	"io"
	"maps"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

// checkpointWriteBatchBytes limits temporary checkpoint memory and the size of one main-file write.
// A database page larger than this limit still requires one complete-page write.
const checkpointWriteBatchBytes int64 = 1 << 20

// maxRetainedCheckpointRecords bounds the record array kept between checkpoints.
const maxRetainedCheckpointRecords = 4096

// checkpointBuffers belongs to one WAL. Backup uses separate local storage.
// records must not retain node or payload references between calls.
type checkpointBuffers struct {
	records []CommittedPage
	data    []byte
}

type Config struct {
	Path                     string
	File                     fileio.File
	FS                       fileio.FS // FS removes the WAL file in [WAL.Delete]. Nil uses [fileio.OSFS].
	PageSize                 int64
	SyncOnCommit             bool
	SyncOnCheckpoint         bool
	CheckpointThresholdBytes uint64
	SyncDirectory            func() error // SyncDirectory makes the names of the database and WAL files durable. The first successful [WAL.Sync] calls it once. Nil means that the WAL does not sync the directory.
}

// CommittedPage is the latest committed state of one page that is not yet checkpointed.
// A data page can hold Node or Payload. An allocation or metadata page holds Payload.
type CommittedPage struct {
	Header RecordHeader
	// Payload holds a recovered node image, a complete allocation page, or encoded metadata.
	Payload []byte
	// Node holds a data node from the current process. A page must not hold both Node and Payload.
	Node *btree.Node
}

func New(config Config) *WAL {
	fs := config.FS
	if fs == nil {
		fs = fileio.OSFS{}
	}
	return &WAL{
		path:                     config.Path,
		file:                     config.File,
		fs:                       fs,
		pageSize:                 config.PageSize,
		syncOnCommit:             config.SyncOnCommit,
		syncOnCheckpoint:         config.SyncOnCheckpoint,
		checkpointThresholdBytes: config.CheckpointThresholdBytes,
		syncDirectory:            config.SyncDirectory,
		overlay:                  make(map[page.ID]CommittedPage),
		nextTxid:                 1,
	}
}

// Checkpoint copies the committed overlay into mainFile and then clears the WAL contents.
// dataPageChecksums selects checksum calculation for data pages only.
// It borrows mainFile and does not close it.
// targetSize is the byte length through the last allocated database page.
// Checkpoint removes a free suffix before it syncs mainFile and clears the WAL.
// When [Config.SyncOnCheckpoint] is true, Checkpoint syncs the WAL before the copy and syncs mainFile before cleanup. When it is false, Checkpoint does not issue these sync calls.
// A main-file write, truncate, or required sync failure leaves the overlay and WAL available for a later retry.
// A WAL truncate failure returns an error and keeps the overlay for cleanup retry.
func (wal *WAL) Checkpoint(mainFile fileio.File, targetSize int64, dataPageChecksums bool) error {
	if len(wal.overlay) == 0 {
		return nil
	}
	if wal.pageSize <= 0 || targetSize < 0 || targetSize%wal.pageSize != 0 {
		return page.ErrInvalid
	}
	if wal.syncOnCheckpoint {
		if err := wal.Sync(); err != nil {
			return err
		}
	}
	if err := wal.checkpointBuffers.writeCommittedPages(mainFile, wal.overlay, wal.pageSize, dataPageChecksums); err != nil {
		return err
	}
	info, err := mainFile.Stat()
	if err != nil {
		return err
	}
	if info.Size() < targetSize {
		return page.ErrInvalid
	}
	if info.Size() > targetSize {
		if err := mainFile.Truncate(targetSize); err != nil {
			return err
		}
	}
	if wal.syncOnCheckpoint {
		if err := fileio.SyncData(mainFile); err != nil {
			return err
		}
	}
	if err := wal.Truncate(); err != nil {
		// Keep the overlay so WAL cleanup can be retried.
		return err
	}
	clear(wal.overlay)
	wal.bytesSinceCheckpoint = 0
	wal.checkpointCount++
	return nil
}

// WriteCommittedPages writes complete page images to dst without changing the WAL.
// dataPageChecksums selects checksum calculation for data pages only.
// The caller must keep pages, their nodes, and their payloads unchanged until it returns.
func WriteCommittedPages(dst io.WriterAt, pages map[page.ID]CommittedPage, pageSize int64, dataPageChecksums bool) error {
	var buffers checkpointBuffers
	return buffers.writeCommittedPages(dst, pages, pageSize, dataPageChecksums)
}

func (buffers *checkpointBuffers) writeCommittedPages(dst io.WriterAt, pages map[page.ID]CommittedPage, pageSize int64, dataPageChecksums bool) error {
	if pageSize <= 0 {
		return page.ErrInvalid
	}
	records := buffers.records[:0]
	if cap(records) < len(pages) {
		records = make([]CommittedPage, 0, len(pages))
	}
	defer func() {
		clear(records)
		if cap(records) <= maxRetainedCheckpointRecords {
			buffers.records = records[:0]
		} else {
			buffers.records = nil
		}
	}()
	for _, record := range pages {
		records = append(records, record)
	}
	// Sorting lets the writer combine adjacent pages into one I/O operation.
	slices.SortFunc(records, func(left, right CommittedPage) int {
		switch {
		case left.Header.PageID < right.Header.PageID:
			return -1
		case left.Header.PageID > right.Header.PageID:
			return 1
		default:
			return 0
		}
	})
	return writeCheckpointRecordRuns(dst, records, pageSize, dataPageChecksums, &buffers.data)
}

// writeCheckpointRecordRuns combines adjacent, page-ID-ordered records into bounded writes.
// records must contain unique page IDs in ascending order.
// It pads each WAL image to pageSize because the WAL omits unused page bytes.
// reusableBuffer can be nil. Otherwise it keeps at most checkpointWriteBatchBytes.
// The writer must not retain the supplied write buffers after WriteAt returns.
func writeCheckpointRecordRuns(mainFile io.WriterAt, records []CommittedPage, pageSize int64, dataPageChecksums bool, reusableBuffer *[]byte) error {
	if len(records) == 0 {
		return nil
	}
	for index := range records {
		record := &records[index]
		payloadSize := len(record.Payload)
		if record.Node != nil {
			if record.Header.Type != RecordTypeNode || record.Header.PageID != record.Node.PageID() || record.Payload != nil {
				return page.ErrInvalid
			}
			payloadSize = btree.WALNodeEncodedSize(record.Node)
		}
		if int64(payloadSize) > pageSize {
			return page.ErrInvalid
		}
		if record.Header.Type == RecordTypeNode && record.Node == nil {
			if err := btree.ValidateWALNode(record.Payload, record.Header.PageID); err != nil {
				return err
			}
		}
	}

	pagesPerBatch := max(checkpointWriteBatchBytes/pageSize, 1)
	if pagesPerBatch > int64(len(records)) {
		pagesPerBatch = int64(len(records))
	}
	bufferSize := int(pagesPerBatch * pageSize)
	var buffer []byte
	if reusableBuffer != nil && int64(bufferSize) <= checkpointWriteBatchBytes {
		if cap(*reusableBuffer) < bufferSize {
			*reusableBuffer = make([]byte, 0, bufferSize)
		}
		buffer = (*reusableBuffer)[:0:bufferSize]
	} else {
		// A page larger than the write limit needs temporary storage for one page.
		buffer = make([]byte, 0, bufferSize)
	}
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

		// Resize the buffer to hold the next page.
		buffer = buffer[:pageEnd]

		pageData := buffer[pageStart:pageEnd]
		clear(pageData)
		if record.Node != nil {
			btree.AppendEncodedWALNode(pageData[:0], record.Node)
		} else {
			copy(pageData, record.Payload)
		}
		if record.Header.Type == RecordTypeNode {
			if err := btree.VerifyNodeIDAndSetChecksum(pageData, pageID, dataPageChecksums); err != nil {
				return err
			}
		}
		previousPageID = pageID
	}
	return flush()
}

func (wal *WAL) Lookup(pageID page.ID) (CommittedPage, bool) {
	record, ok := wal.overlay[pageID]
	return record, ok
}

func (wal *WAL) LoadCommittedRecord(record WALRecord) {
	wal.overlay[record.Header.PageID] = CommittedPage{Header: record.Header, Payload: record.Payload}
}

func (wal *WAL) CommittedRecord(pageID page.ID) (CommittedPage, bool) {
	record, ok := wal.overlay[pageID]
	return record, ok
}

type Stats struct {
	CheckpointThresholdBytes uint64
	BytesSinceCheckpoint     uint64
	TotalBytesWritten        uint64
	CommitCount              uint64
	CheckpointCount          uint64
	CommittedRecordCount     int
	NextTxID                 TxID
}

func (wal *WAL) Stats() Stats {
	return Stats{
		CheckpointThresholdBytes: wal.checkpointThresholdBytes,
		BytesSinceCheckpoint:     wal.bytesSinceCheckpoint,
		TotalBytesWritten:        wal.totalBytesWritten,
		CommitCount:              wal.commitCount,
		CheckpointCount:          wal.checkpointCount,
		CommittedRecordCount:     len(wal.overlay),
		NextTxID:                 wal.nextTxid,
	}
}

// Size returns the current WAL file size in bytes.
// It returns zero when a read-only database has no WAL file.
func (wal *WAL) Size() (int64, error) {
	if wal.file == nil {
		return 0, nil
	}
	info, err := wal.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (wal *WAL) CommittedRecords() map[page.ID]CommittedPage {
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
func (wal *WAL) FileForTesting() fileio.File {
	return wal.file
}

// ReplaceFileForTesting replaces the WAL file for tests that inject file-operation failures.
// Production code should not use it.
func (wal *WAL) ReplaceFileForTesting(file fileio.File) {
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
	err := wal.file.Close()
	if err == nil {
		wal.checkpointBuffers = checkpointBuffers{}
	}
	return err
}
