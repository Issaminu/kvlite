package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

// checkpointWriteBatchBytes bounds combined writes and retained checkpoint buffers.
// A larger single node can use temporary storage or several writes.
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

// CommittedPage is the latest committed image at a node head, allocation page, or metadata page.
// A node image can cover several consecutive physical pages. It holds Node or Payload.
// Allocation and metadata pages hold Payload.
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
		// A new handle does not know what an earlier process left in storage.
		headerSyncNeeded: true,
	}
}

// Checkpoint copies the committed overlay into mainFile and then clears the WAL contents.
// dataPageChecksums selects checksum calculation for data pages only.
// It borrows mainFile and does not close it.
// targetSize is the byte length through the last allocated database page.
// Checkpoint removes a free suffix before it syncs mainFile and clears the WAL.
// When [Config.SyncOnCheckpoint] is true, Checkpoint syncs the WAL before the copy and syncs mainFile before cleanup. When it is false, Checkpoint does not issue these sync calls.
// A main-file write, truncate, or sync failure leaves the overlay and WAL available for a later retry.
// A WAL sync failure stops the WAL like a failed append (see [WAL.Commit]), and every later Checkpoint returns that error and writes nothing.
// A WAL truncate failure returns an error, but the main file then holds every committed page, so the overlay goes away (see [WAL.Truncate]).
func (wal *WAL) Checkpoint(mainFile fileio.File, targetSize int64, dataPageChecksums bool) error {
	if len(wal.overlay) == 0 {
		return nil
	}
	if wal.appendFailure != nil {
		return wal.appendFailure
	}
	if wal.pageSize <= 0 || targetSize < 0 || targetSize%wal.pageSize != 0 {
		return page.ErrInvalid
	}
	if wal.syncOnCheckpoint {
		if err := wal.Sync(); err != nil {
			// After a failed sync, the file can lose unsynced records for good, as Linux can after a writeback error.
			// Then no WAL can repair a torn main-file write, so stop this WAL like a failed append.
			wal.appendFailure = fmt.Errorf("%w: %w", ErrWriteFailed, err)
			return wal.appendFailure
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
	err = wal.Truncate()
	clear(wal.overlay)
	wal.bytesSinceCheckpoint = 0
	if err != nil {
		return err
	}
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

// WriteDirect writes complete node spans directly to the main file.
// It appends no WAL records and keeps no committed overlay.
// The caller excludes backups and stops its database handle after an error.
// Recovery must have cleared the WAL before this method can run.
// Each call on a WAL must use the same main file. No other code can change its size.
func (wal *WAL) WriteDirect(mainFile fileio.File, targetSize int64, checksums bool, nodes []NodeRecord, allocationPages []AllocationRecord, encodedMeta []byte) error {
	if wal.appendFailure != nil {
		return wal.appendFailure
	}
	if wal.syncOnCommit || wal.syncOnCheckpoint || len(wal.overlay) != 0 || wal.appendOffset != 0 || !wal.fileKnownEmpty || wal.pageSize <= 0 || targetSize < 0 || targetSize%wal.pageSize != 0 {
		return page.ErrInvalid
	}
	defer wal.releaseEncodingBuffer()
	wal.pending = wal.pending[:0]
	for _, change := range nodes {
		if change.Final == nil {
			return ErrNodeRecordMissingFinal
		}
		wal.pending = append(wal.pending, CommittedPage{Header: RecordHeader{Type: RecordTypeNode, PageID: change.Final.PageID()}, Node: change.Final})
	}
	for _, frame := range allocationPages {
		wal.pending = append(wal.pending, CommittedPage{Header: RecordHeader{Type: RecordTypeAllocation, PageID: frame.PageID}, Payload: frame.Payload})
	}
	if len(encodedMeta) != 0 {
		for _, id := range []page.ID{page.Meta0ID, page.Meta1ID} {
			wal.pending = append(wal.pending, CommittedPage{Header: RecordHeader{Type: RecordTypeMeta, PageID: id}, Payload: encodedMeta})
		}
	}
	if len(wal.pending) == 0 {
		return nil
	}
	slices.SortFunc(wal.pending, func(left, right CommittedPage) int {
		if left.Header.PageID < right.Header.PageID {
			return -1
		}
		if left.Header.PageID > right.Header.PageID {
			return 1
		}
		return 0
	})
	for index := range wal.pending {
		count, err := checkpointRecordPages(&wal.pending[index], wal.pageSize)
		if err != nil {
			return err
		}
		if uint64(wal.pending[index].Header.PageID)+uint64(count) > uint64(targetSize/wal.pageSize) {
			return page.ErrInvalid
		}
	}
	knownSize := wal.directTargetSize
	wal.directTargetSize = 0
	if err := writeCheckpointRecordRuns(mainFile, wal.pending, wal.pageSize, checksums, &wal.checkpointBuffers.data); err != nil {
		return err
	}
	if knownSize != targetSize || knownSize == 0 {
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
	}
	wal.directTargetSize = targetSize
	return nil
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

// checkpointRecordPages validates a complete logical image and its storage range.
func checkpointRecordPages(record *CommittedPage, pageSize int64) (int, error) {
	if pageSize <= 0 {
		return 0, page.ErrInvalid
	}
	var count int
	if record.Node != nil {
		if record.Header.Type != RecordTypeNode || page.IsMetaID(record.Header.PageID) || record.Header.PageID != record.Node.PageID() || record.Payload != nil ||
			int64(btree.WALNodeEncodedSize(record.Node)) > MaxNodeImageSize {
			return 0, page.ErrInvalid
		}
		count = btree.NodePageCount(record.Node, pageSize)
		if count == 0 || record.Node.StoragePageCount() != count {
			return 0, page.ErrInvalid
		}
	} else {
		switch record.Header.Type {
		case RecordTypeNode:
			var err error
			count, err = ValidateNodeImage(record.Payload, record.Header.PageID, pageSize)
			if err != nil {
				return 0, err
			}
		case RecordTypeAllocation:
			if int64(len(record.Payload)) != pageSize {
				return 0, page.ErrInvalid
			}
			count = 1
		case RecordTypeMeta:
			if !page.IsMetaID(record.Header.PageID) || int64(len(record.Payload)) > pageSize {
				return 0, page.ErrInvalid
			}
			count = 1
		default:
			return 0, page.ErrInvalid
		}
	}
	maxPages := uint64(int64(^uint64(0)>>1) / pageSize)
	if count <= 0 || uint64(count) > maxPages || uint64(record.Header.PageID) > maxPages-uint64(count) {
		return 0, page.ErrInvalid
	}
	return count, nil
}

// NodeWriter can encode a prepared node directly into its final file span.
// The node's input bytes must stay separate from that destination.
// A false result must write nothing and selects the ordinary buffered writer.
type NodeWriter interface {
	WriteNode(node *btree.Node, pageSize int64, checksums bool) (bool, error)
}

// writeCheckpointRecordRuns combines adjacent complete spans into bounded writes.
// Every record names its head. Trailing pages have no separate header or record.
// All ranges are checked before the first write, including overlap after reuse.
func writeCheckpointRecordRuns(mainFile io.WriterAt, records []CommittedPage, pageSize int64, dataPageChecksums bool, reusableBuffer *[]byte) error {
	if len(records) == 0 {
		return nil
	}
	var previousEnd page.ID
	var totalBytes, largestBufferedSpan int64
	for index := range records {
		record := &records[index]
		count, err := checkpointRecordPages(record, pageSize)
		if err != nil {
			return err
		}
		if index != 0 && record.Header.PageID < previousEnd {
			return fmt.Errorf("overlapping WAL spans at page %d: %w", record.Header.PageID, page.ErrInvalid)
		}
		previousEnd = record.Header.PageID + page.ID(count)
		size := int64(count) * pageSize
		totalBytes = min(checkpointWriteBatchBytes, totalBytes+min(size, checkpointWriteBatchBytes))
		if size <= maxRetainedEncodingBufferBytes {
			largestBufferedSpan = max(largestBufferedSpan, size)
		}
	}
	bufferSize := max(totalBytes, largestBufferedSpan)
	var buffer []byte
	var startPageID page.ID
	previousEnd = 0
	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		if err := fileio.WriteFullAt(mainFile, buffer, int64(startPageID)*pageSize); err != nil {
			return err
		}
		buffer = buffer[:0]
		return nil
	}
	nodeWriter, canWriteNode := mainFile.(NodeWriter)
	for index := range records {
		record := &records[index]
		count, _ := checkpointRecordPages(record, pageSize)
		size := int64(count) * pageSize
		if len(buffer) != 0 && (record.Header.PageID != previousEnd || int64(len(buffer))+size > int64(cap(buffer))) {
			if err := flush(); err != nil {
				return err
			}
		}
		// Keep an unhandled run buffered until a gap or the size limit.
		// Retrying the node writer would split adjacent pages into separate writes.
		if canWriteNode && record.Node != nil && len(buffer) == 0 {
			handled, err := nodeWriter.WriteNode(record.Node, pageSize, dataPageChecksums)
			if err != nil {
				return err
			}
			if handled {
				previousEnd = 0
				continue
			}
		}
		if buffer == nil && size <= bufferSize {
			if reusableBuffer != nil && bufferSize <= checkpointWriteBatchBytes {
				if int64(cap(*reusableBuffer)) < bufferSize {
					*reusableBuffer = make([]byte, 0, int(bufferSize))
				}
				buffer = (*reusableBuffer)[:0:int(bufferSize)]
			} else {
				buffer = make([]byte, 0, int(bufferSize))
			}
		}
		previousEnd = record.Header.PageID + page.ID(count)
		if size > int64(cap(buffer)) {
			if err := writeLargeCheckpointSpan(mainFile, record, pageSize, size, dataPageChecksums); err != nil {
				return err
			}
			continue
		}
		if len(buffer) == 0 {
			startPageID = record.Header.PageID
		}
		start := len(buffer)
		buffer = buffer[:start+int(size)]
		span := buffer[start:]
		clear(span)
		if record.Node != nil {
			btree.AppendEncodedWALNode(span[:0], record.Node)
		} else {
			copy(span, record.Payload)
		}
		if record.Header.Type == RecordTypeNode {
			if err := btree.VerifyNodeIDAndSetChecksum(span, record.Header.PageID, dataPageChecksums); err != nil {
				return err
			}
		}
	}
	return flush()
}

// writeLargeCheckpointSpan keeps padding and checksum storage bounded even for
// a maximum-size node. The compact image stays private and unchanged.
func writeLargeCheckpointSpan(dst io.WriterAt, record *CommittedPage, pageSize, spanSize int64, checksums bool) error {
	image := record.Payload
	if record.Node != nil {
		image = btree.EncodeWALNode(record.Node)
	}
	if record.Header.Type != RecordTypeNode {
		return page.ErrInvalid
	}
	var header [btree.NodeHeaderSize]byte
	copy(header[:], image)
	var zeros [4096]byte
	var sum uint32
	if checksums {
		sum = checksum.Sum32(image[:14], image[18:])
		for remaining := spanSize - int64(len(image)); remaining > 0; {
			n := min(remaining, int64(len(zeros)))
			sum = checksum.Update32(sum, zeros[:n])
			remaining -= n
		}
		if sum == 0 {
			sum = 1
		}
	}
	binary.LittleEndian.PutUint32(header[14:18], sum)
	writer := io.NewOffsetWriter(dst, int64(record.Header.PageID)*pageSize)
	if err := fileio.WriteFull(writer, header[:]); err != nil {
		return err
	}
	if err := fileio.WriteFull(writer, image[btree.NodeHeaderSize:]); err != nil {
		return err
	}
	for remaining := spanSize - int64(len(image)); remaining > 0; {
		n := min(remaining, int64(len(zeros)))
		if err := fileio.WriteFull(writer, zeros[:n]); err != nil {
			return err
		}
		remaining -= n
	}
	return nil
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

// CommittedPageCount counts the physical pages covered by the latest committed images.
// A node includes all its tail pages. An allocation or metadata image counts as one page.
// It scans the committed images without reading the file or decoding node bodies.
// Stats keeps its constant-cost record count for write and cursor checks.
func (wal *WAL) CommittedPageCount() int {
	count := 0
	for _, record := range wal.overlay {
		switch record.Header.Type {
		case RecordTypeNode:
			if record.Node != nil {
				count += btree.NodePageCount(record.Node, wal.pageSize)
			} else if pages, err := btree.NodeSpan(record.Payload, record.Header.PageID, wal.pageSize); err == nil {
				count += pages
			}
		case RecordTypeAllocation, RecordTypeMeta:
			count++
		}
	}
	return count
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

// Close closes the WAL file.
// A second call does nothing, so [WAL.Delete] can retry the removal after a failure.
func (wal *WAL) Close() error {
	if wal.file == nil || wal.closed {
		return nil
	}
	err := wal.file.Close()
	if err == nil {
		wal.closed = true
		wal.checkpointBuffers = checkpointBuffers{}
	}
	return err
}
