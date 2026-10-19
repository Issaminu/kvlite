package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

const (
	// maxRetainedEncodingBufferBytes is the largest transaction encoding buffer kept across commits.
	// After [WAL.Commit] returns, a larger buffer moves to a [sync.Pool].
	// The next large transaction can use it again, and the garbage collector can free it.
	// So one large transaction does not retain memory.
	maxRetainedEncodingBufferBytes = 2 << 20
	// maxRetainedPendingPages is the largest pending-page list kept across commits, about 2 MiB of CommittedPage values.
	maxRetainedPendingPages = 1 << 15

	walFileMagic      uint32 = 0x4c57564b // The little-endian bytes spell "KVWL".
	walFileHeaderSize        = 20
)

// A non-empty WAL file starts with this header:
//
//	[0:4]   magic number
//	[4:8]   database format version
//	[8:16]  salt of this WAL generation
//	[16:20] CRC32C checksum of bytes [0:16]
//
// A generation starts when a commit writes at offset zero, after the WAL is created or cleared.
// Each generation selects a new random salt, and its checksum seed differs from the previous generation.
// Every record checksum covers that salt.
// A crash can keep new records but lose the truncate that cleared the WAL.
// Then the earlier records after them fail their checksums, and recovery stops before them.
// A power failure can also lose the new header.
// So a new generation syncs its header before its first record, unless the truncate before it is already synced (see [WAL.SyncTruncate]).

type WAL struct {
	path                     string
	file                     fileio.File
	fs                       fileio.FS
	pageSize                 int64
	syncOnCommit             bool
	syncOnCheckpoint         bool
	checkpointThresholdBytes uint64
	bytesSinceCheckpoint     uint64
	totalBytesWritten        uint64
	commitCount              uint64
	checkpointCount          uint64
	overlay                  map[page.ID]CommittedPage // Committed pages that are not yet checkpointed.
	nextTxid                 TxID                      // sequence number stamped on the next committed transaction
	hasUnsyncedWrites        bool                      // true when WAL bytes need a storage sync
	appendOffset             int64                     // byte offset of the next WAL append; independent of the file read/write cursor
	encodingBuffer           []byte                    // reusable transaction encoding buffer; capacity may be cleared after large commits
	largeEncodingBuffers     sync.Pool                 // *[]byte encoding buffers larger than maxRetainedEncodingBufferBytes
	checkpointBuffers        checkpointBuffers         // reusable checkpoint storage; never shared with backup
	patchBuffer              []byte                    // reusable byte patch or entry patch; copied into encodingBuffer before reuse
	pending                  []CommittedPage           // pages of the encoded transaction, in record order; Commit publishes them after the append succeeds
	appendFailure            error                     // set when an append fails after it starts to write; blocks every later commit on this WAL
	salt                     uint64                    // salt of the current generation; written in the file header
	checksumSeed             uint32                    // CRC32C checksum of salt; every record checksum starts from it
	syncFile                 func() error              // syncFile is a test hook for sync failures and call counts.
	syncDirectory            func() error              // set until the first successful Sync syncs the database directory
	fileKnownEmpty           bool                      // true while the file is known to hold no bytes, so Truncate makes no system call
	closed                   bool                      // Close succeeded; a retry of Delete after a failed remove must not close the file again
	headerSyncNeeded         bool                      // storage can still hold records of an earlier generation, so a new generation syncs its header before its records
}

// Commit appends one transaction and publishes each changed page to the committed overlay.
// It returns whether the committed WAL size reached the checkpoint threshold.
//
// nodes contains original and final data-page images.
// Each page's first change after a checkpoint uses a complete image.
// Later changes can use compact records while that image remains in the WAL.
// The caller sets allocated continuation IDs with [btree.Node.SetOverflowPages].
// Commit writes one record per page of a continued node: a complete image, or a byte patch for a later change that keeps the continuation IDs.
// It publishes compact payloads for those pages after the append succeeds.
// allocationPages contains complete allocation segments. A later change to a segment can also use a byte patch.
// encodedMeta contains metadata when it changed. It can be nil or empty when metadata did not change.
// Commit writes changed metadata to both metadata pages.
// When every page equals its image in the WAL, Commit writes nothing.
// retired identifies old pages that leave the in-memory WAL view after the append succeeds.
// The caller must not change committed nodes, allocationPages, or encodedMeta before the next checkpoint.
//
// Appends use [WAL.appendOffset], not the WAL file read/write cursor, so unrelated seeks on the file cannot corrupt the log.
//
// An error before the append starts, such as an oversized payload, changes nothing.
// A write or sync failure returns an error that matches [ErrWriteFailed].
// Commit then truncates the failed append and restores the pre-append byte and synchronization state without updating the overlay.
// The truncate is not synchronized, so the failed transaction can still reach storage.
// Every later Commit on this WAL returns the same error, because a new transaction must not build on that unknown file state.
func (wal *WAL) Commit(nodes []NodeRecord, allocationPages []AllocationRecord, encodedMeta []byte, retired map[page.ID]struct{}) (bool, error) {
	if wal.appendFailure != nil {
		return false, wal.appendFailure
	}
	if len(nodes) == 0 && len(allocationPages) == 0 && len(encodedMeta) == 0 {
		if len(retired) != 0 {
			return false, fmt.Errorf("retired WAL pages without a transaction: %w", page.ErrInvalid)
		}
		return false, nil
	}

	if wal.appendOffset == 0 {
		// Record checksums depend on the salt, so select it before encoding.
		wal.startGeneration()
	}
	transaction, err := wal.encodeTransaction(nodes, allocationPages, encodedMeta, wal.nextTxid)
	if err != nil {
		return false, err
	}
	defer wal.releaseEncodingBuffer()
	if len(transaction) == 0 {
		// Every page equals its image in the WAL, so the WAL already holds this state.
		return false, nil
	}
	startOffset := wal.appendOffset
	bytesBefore := wal.bytesSinceCheckpoint
	unsyncedBefore := wal.hasUnsyncedWrites

	needsCheckpoint, err := wal.appendTransaction(transaction)
	if err != nil {
		rollbackErr := wal.rollbackAppend(startOffset, bytesBefore, unsyncedBefore)
		wal.appendFailure = fmt.Errorf("%w: %w", ErrWriteFailed, errors.Join(err, rollbackErr))
		return false, wal.appendFailure
	}
	wal.totalBytesWritten += uint64(wal.appendOffset - startOffset)

	for pageID := range retired {
		delete(wal.overlay, pageID)
	}
	for _, committed := range wal.pending {
		wal.overlay[committed.Header.PageID] = committed
	}
	wal.commitCount++
	wal.nextTxid++
	return needsCheckpoint, nil
}

// releaseEncodingBuffer clears the pending pages and moves an oversized transaction encoding buffer to the pool after Commit returns.
// The overlay holds the published pages, and a failed append publishes none, so the list keeps only its storage.
func (wal *WAL) releaseEncodingBuffer() {
	clear(wal.pending)
	wal.pending = wal.pending[:0]
	if cap(wal.pending) > maxRetainedPendingPages {
		wal.pending = nil
	}
	if cap(wal.encodingBuffer) > maxRetainedEncodingBufferBytes {
		buffer := wal.encodingBuffer[:0]
		wal.largeEncodingBuffers.Put(&buffer)
		wal.encodingBuffer = nil
	}
	if cap(wal.patchBuffer) > maxRetainedEncodingBufferBytes {
		wal.patchBuffer = nil
	}
}

// newEncodingBuffer returns an empty buffer with room for size bytes.
// A size above maxRetainedEncodingBufferBytes can use a pooled buffer.
func (wal *WAL) newEncodingBuffer(size int) []byte {
	if size > maxRetainedEncodingBufferBytes {
		if pooled, ok := wal.largeEncodingBuffers.Get().(*[]byte); ok && cap(*pooled) >= size {
			return (*pooled)[:0]
		}
	}
	return make([]byte, 0, size)
}

// encodeTransaction encodes changed nodes, allocation pages, optional metadata, and a commit marker after them.
// When every page equals its image in the WAL, there is no record, and it encodes nothing: recovery stops at a commit marker without records.
// It also fills wal.pending with the committed state of each page, in record order, and Commit publishes it only after a successful append.
// The payload of a continued page borrows the returned transaction, or a new image when byte patches describe the node.
func (wal *WAL) encodeTransaction(nodes []NodeRecord, allocationPages []AllocationRecord, encodedMeta []byte, txid TxID) ([]byte, error) {
	transactionSize := HeaderSize + ChecksumSize
	continuedPageCount := 0
	for index := range nodes {
		record := &nodes[index]
		if record.Final == nil {
			return nil, ErrNodeRecordMissingFinal
		}
		if len(record.Final.OverflowPages()) != 0 {
			count := len(record.Final.OverflowPages()) + 1
			if count != btree.NodePageCount(record.Final, wal.pageSize) {
				return nil, btree.ErrInvalid
			}
			transactionSize += count*(HeaderSize+ChecksumSize) + btree.WALNodePagesEncodedSize(record.Final, wal.pageSize)
			continuedPageCount += count
			continue
		}
		// Size for the complete final image: patches are only written when smaller, so this is a safe upper bound for the encoding buffer capacity.
		payloadSize := btree.WALNodeEncodedSize(record.Final)
		if int64(payloadSize) > wal.pageSize {
			return nil, fmt.Errorf("record payload %d bytes, page size %d bytes: %w", payloadSize, wal.pageSize, ErrRecordPayloadTooLarge)
		}
		transactionSize += HeaderSize + payloadSize + ChecksumSize
	}
	for _, record := range allocationPages {
		if int64(len(record.Payload)) != wal.pageSize {
			return nil, fmt.Errorf("allocation page %d has size %d, want %d: %w", record.PageID, len(record.Payload), wal.pageSize, page.ErrInvalid)
		}
		transactionSize += HeaderSize + len(record.Payload) + ChecksumSize
	}
	if len(encodedMeta) > 0 {
		if int64(len(encodedMeta)) > wal.pageSize {
			return nil, fmt.Errorf("metadata payload %d bytes, page size %d bytes: %w", len(encodedMeta), wal.pageSize, ErrRecordPayloadTooLarge)
		}
		transactionSize += 2 * (HeaderSize + len(encodedMeta) + ChecksumSize)
	}

	transaction := wal.encodingBuffer[:0]
	// Continued payloads borrow this transaction after Commit succeeds.
	// Never reuse their storage for another transaction.
	if continuedPageCount != 0 {
		transaction = make([]byte, 0, transactionSize)
	} else if cap(transaction) < transactionSize {
		transaction = wal.newEncodingBuffer(transactionSize)
	}
	// One temporary list holds all continued-page destinations. Ordinary nodes need no list.
	var continuedPages [][]byte
	if continuedPageCount != 0 {
		continuedPages = make([][]byte, continuedPageCount)
	}
	clear(wal.pending)
	wal.pending = wal.pending[:0]
	nextContinuedPage := 0
	for index := range nodes {
		node := &nodes[index]
		var pages [][]byte
		if count := len(node.Final.OverflowPages()); count != 0 {
			pages = continuedPages[nextContinuedPage : nextContinuedPage+count+1]
			nextContinuedPage += len(pages)
		}
		var err error
		transaction, err = wal.appendNodeRecord(transaction, node, txid, pages)
		if err != nil {
			return nil, err
		}
		header := RecordHeader{Type: RecordTypeNode, PageID: node.Final.PageID(), TxID: txid}
		if pages == nil {
			wal.pending = append(wal.pending, CommittedPage{Header: header, Node: node.Final})
			continue
		}
		for _, data := range pages {
			header.PageID = page.ID(binary.LittleEndian.Uint64(data[2:10]))
			wal.pending = append(wal.pending, CommittedPage{Header: header, Payload: data})
		}
	}
	for _, record := range allocationPages {
		header := RecordHeader{Type: RecordTypeAllocation, PageID: record.PageID, TxID: txid}
		if base, ok := wal.patchBase(record.PageID); ok && base.Header.Type == RecordTypeAllocation && len(base.Payload) == len(record.Payload) {
			transaction = wal.appendPageChange(transaction, header, base.Payload, record.Payload)
		} else {
			transaction = appendEncodedPayloadRecord(transaction, header, record.Payload, wal.checksumSeed)
		}
		wal.pending = append(wal.pending, CommittedPage{Header: header, Payload: record.Payload})
	}
	if len(encodedMeta) > 0 {
		header := RecordHeader{Type: RecordTypeMeta, TxID: txid}
		for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
			header.PageID = pageID
			transaction = appendEncodedPayloadRecord(transaction, header, encodedMeta, wal.checksumSeed)
			wal.pending = append(wal.pending, CommittedPage{Header: header, Payload: encodedMeta})
		}
	}
	if len(transaction) != 0 {
		transaction = appendEncodedPayloadRecord(transaction, RecordHeader{Type: RecordTypeCommit, TxID: txid}, nil, wal.checksumSeed)
	}
	if continuedPageCount == 0 {
		wal.encodingBuffer = transaction[:0]
	}
	return transaction, nil
}

// patchBase returns the committed image of a page that a patch can change.
// A page has a patch base only when the current WAL generation already holds an image of it: a complete image, maybe with later patches.
// So the first change to a page after a checkpoint has no patch base and writes a complete image.
// Recovery then never needs a main-file page as the base of a patch, because a partial checkpoint can damage that page.
// A checkpoint clears the overlay only after the WAL truncate succeeds, so a failed checkpoint keeps every patch base.
// Nodes, allocation pages, and the pages of continued nodes all use this rule.
func (wal *WAL) patchBase(pageID page.ID) (CommittedPage, bool) {
	committed, ok := wal.overlay[pageID]
	return committed, ok
}

// appendPageChange appends a record that changes the committed image of one page into image.
// The record is a byte patch when that is smaller, and otherwise the complete image. header names the complete image type.
// It appends nothing when image equals committed: the WAL then already holds the latest image of the page.
// committed must be the patch base of the page (see patchBase), with the size of image.
func (wal *WAL) appendPageChange(transaction []byte, header RecordHeader, committed, image []byte) []byte {
	wal.patchBuffer = appendChangedBytePatchRanges(wal.patchBuffer[:0], 0, committed, image)
	if len(wal.patchBuffer) == 0 {
		return transaction
	}
	if len(wal.patchBuffer) < len(image) {
		header.Type = RecordTypeBytePatch
		return appendEncodedPayloadRecord(transaction, header, wal.patchBuffer, wal.checksumSeed)
	}
	return appendEncodedPayloadRecord(transaction, header, image, wal.checksumSeed)
}

// appendNodeRecord appends a complete node image, a byte patch, or an entry patch.
// Continued nodes encode directly into transaction-owned page images.
//
// The record uses the complete final image in these cases:
//   - Original is nil. A new or reused page has no committed base.
//   - The page has no patch base: this is its first change after a checkpoint. See patchBase.
//   - Final fits in one page, and Original has continuation pages. The two images use different offsets.
//   - No patch is smaller than the complete image.
//
// So each patch changes the image that the earlier WAL records of the page give, and recovery can replay byte patches and entry patches in any order.
// A byte patch needs the same encoded layout in Original and Final. It is the smallest record for changed values and child IDs.
// An entry patch can insert, replace, and delete entries. A branch entry patch cannot change the first child.
// A continued Final uses appendContinuedNodeRecords, which can write a byte patch for each page.
func (wal *WAL) appendNodeRecord(transaction []byte, record *NodeRecord, txid TxID, pages [][]byte) ([]byte, error) {
	targetSize := btree.WALNodeEncodedSize(record.Final)
	header := RecordHeader{Type: RecordTypeNode, PageID: record.Final.PageID(), TxID: txid}
	if record.Original != nil && record.Original.PageID() != header.PageID {
		return nil, fmt.Errorf("WAL node page %d, original page %d: %w", header.PageID, record.Original.PageID(), ErrNodePageMismatch)
	}
	if len(record.Final.OverflowPages()) != 0 {
		return wal.appendContinuedNodeRecords(transaction, record, txid, pages)
	}
	_, hasBase := wal.patchBase(header.PageID)
	if record.Original == nil || !hasBase || len(record.Original.OverflowPages()) != 0 {
		return appendEncodedNodeRecord(transaction, header, record.Final, wal.checksumSeed), nil
	}

	var encodedSize int
	var compatible bool
	wal.patchBuffer = wal.patchBuffer[:0]
	if record.Final.IsLeaf() {
		encodedSize, compatible = appendLeafBytePatch(&wal.patchBuffer, record.Original, record.Final)
	} else {
		encodedSize, compatible = appendBranchBytePatch(&wal.patchBuffer, record.Original, record.Final)
	}
	if compatible && encodedSize == targetSize && len(wal.patchBuffer) < targetSize {
		header.Type = RecordTypeBytePatch
		return appendEncodedPayloadRecord(transaction, header, wal.patchBuffer, wal.checksumSeed), nil
	}
	// A different entry count or field length moves later bytes to new offsets, so a byte patch cannot describe it.
	// An entry patch describes the changed entries.
	var smaller bool
	if wal.patchBuffer, smaller = appendEntryPatch(wal.patchBuffer[:0], record.Original, record.Final, targetSize); smaller {
		header.Type = RecordTypeEntryPatch
		return appendEncodedPayloadRecord(transaction, header, wal.patchBuffer, wal.checksumSeed), nil
	}
	return appendEncodedNodeRecord(transaction, header, record.Final, wal.checksumSeed), nil
}

// appendContinuedNodeRecords leaves record headers and checksums between the
// page destinations. Each published payload borrows its own record's bytes.
// A later change that keeps the continuation IDs can use appendContinuedBytePatches instead.
func (wal *WAL) appendContinuedNodeRecords(transaction []byte, record *NodeRecord, txid TxID, pages [][]byte) ([]byte, error) {
	ids := append([]page.ID{record.Final.PageID()}, record.Final.OverflowPages()...)
	imageBytes := btree.WALNodePagesEncodedSize(record.Final, wal.pageSize)
	if record.Original != nil && slices.Equal(record.Original.OverflowPages(), record.Final.OverflowPages()) {
		var patched bool
		var err error
		if transaction, patched, err = wal.appendContinuedBytePatches(transaction, record.Final, txid, ids, pages, imageBytes); err != nil || patched {
			return transaction, err
		}
	}
	start := len(transaction)
	stride := HeaderSize + int(wal.pageSize) + ChecksumSize
	transaction = transaction[:start+imageBytes+len(ids)*(HeaderSize+ChecksumSize)]
	for i := range pages {
		payloadStart := start + i*stride + HeaderSize
		size := min(int(wal.pageSize), imageBytes-i*int(wal.pageSize))
		end := payloadStart + size
		pages[i] = transaction[payloadStart:end:end]
	}
	if err := btree.EncodeWALNodePagesInto(record.Final, wal.pageSize, ids, pages); err != nil {
		return nil, err
	}
	end := start
	for i, data := range pages {
		headerStart := start + i*stride
		header := RecordHeader{Type: RecordTypeNode, PageID: ids[i], TxID: txid}
		appendRecordHeader(transaction[:headerStart], header, len(data))
		payloadStart := headerStart + HeaderSize
		end = len(appendRecordChecksum(transaction[:payloadStart+len(data)], headerStart, payloadStart, wal.checksumSeed))
	}
	return transaction[:end], nil
}

// appendContinuedBytePatches appends records only for the changed pages of a continued node.
// Each page of final must have a patch base of the same size (see patchBase). Then appendPageChange writes each page.
// It returns false and appends nothing when a page has no such patch base.
// The published payloads borrow one new image of final, because the transaction holds only the changes.
func (wal *WAL) appendContinuedBytePatches(transaction []byte, final *btree.Node, txid TxID, ids []page.ID, pages [][]byte, imageBytes int) ([]byte, bool, error) {
	pageSize := int(wal.pageSize)
	for i, id := range ids {
		base, ok := wal.patchBase(id)
		if !ok || len(base.Payload) != min(pageSize, imageBytes-i*pageSize) {
			return transaction, false, nil
		}
	}
	image := make([]byte, imageBytes)
	for i := range pages {
		end := min((i+1)*pageSize, imageBytes)
		pages[i] = image[i*pageSize : end : end]
	}
	if err := btree.EncodeWALNodePagesInto(final, wal.pageSize, ids, pages); err != nil {
		return nil, false, err
	}
	for i, data := range pages {
		base, _ := wal.patchBase(ids[i])
		transaction = wal.appendPageChange(transaction, RecordHeader{Type: RecordTypeNode, PageID: ids[i], TxID: txid}, base.Payload, data)
	}
	return transaction, true, nil
}

// appendLeafBytePatch scans one leaf once and appends the ranges of changed value bytes to *patch.
// A patch is possible only when flags, keys, and value lengths keep the same encoded layout.
// It returns the encoded size of target, and false when the layout changes.
// patch is a pointer, so the loop does not keep the slice in registers: each bytes.Equal call would then store and load it again for every entry.
func appendLeafBytePatch(patch *[]byte, base, target *btree.Node) (int, bool) {
	if !base.IsLeaf() || base.EntryCount() != target.EntryCount() {
		return 0, false
	}

	offset := target.WALBodyOffset()
	for index := range target.EntryCount() {
		baseEntry := base.EntryAt(index)
		targetEntry := target.EntryAt(index)
		baseKey := baseEntry.Key()
		targetKey := targetEntry.Key()
		baseValue := baseEntry.Value()
		targetValue := targetEntry.Value()
		if baseEntry.Flags() != targetEntry.Flags() || !bytes.Equal(baseKey, targetKey) || len(baseValue) != len(targetValue) {
			return 0, false
		}

		valueOffset := offset + len(targetKey)
		if !bytes.Equal(baseValue, targetValue) {
			*patch = appendChangedBytePatchRanges(*patch, valueOffset, baseValue, targetValue)
		}
		offset = valueOffset + len(targetValue)
	}
	return offset, true
}

// appendBranchBytePatch scans one branch once and appends the ranges of changed child IDs and separator keys to *patch.
// A patch is possible only when the child count and separator-key lengths keep the same encoded layout.
// It returns the encoded size of target, and false when the layout changes. patch is a pointer for the reason in appendLeafBytePatch.
func appendBranchBytePatch(patch *[]byte, base, target *btree.Node) (int, bool) {
	if base.IsLeaf() || base.EntryCount() != target.EntryCount() {
		return 0, false
	}
	if len(base.Children) != base.EntryCount()+1 || len(target.Children) != target.EntryCount()+1 {
		return 0, false
	}

	offset := target.WALBodyOffset()
	var baseChild, targetChild [page.IDSize]byte
	for index := range target.Children {
		if base.Children[index] != target.Children[index] {
			binary.LittleEndian.PutUint64(baseChild[:], uint64(base.Children[index]))
			binary.LittleEndian.PutUint64(targetChild[:], uint64(target.Children[index]))
			*patch = appendChangedBytePatchRanges(*patch, offset, baseChild[:], targetChild[:])
		}
		offset += page.IDSize
	}
	for index := range target.EntryCount() {
		baseKey := base.EntryAt(index).Key()
		targetKey := target.EntryAt(index).Key()
		if len(baseKey) != len(targetKey) {
			return 0, false
		}
		if !bytes.Equal(baseKey, targetKey) {
			*patch = appendChangedBytePatchRanges(*patch, offset, baseKey, targetKey)
		}
		offset += len(targetKey)
	}
	return offset, true
}

// rollbackAppend removes bytes from a failed commit and restores the counters that describe the retained WAL prefix.
// Without a crash, a later open of this WAL then does not recover the failed transaction.
func (wal *WAL) rollbackAppend(offset int64, bytesSinceCheckpoint uint64, hasUnsyncedWrites bool) error {
	if err := wal.file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate failed WAL transaction: %w", err)
	}
	wal.bytesSinceCheckpoint = bytesSinceCheckpoint
	wal.hasUnsyncedWrites = hasUnsyncedWrites
	wal.appendOffset = offset
	return nil
}

// ReadRecords validates the WAL header and reads the valid record prefix.
// It returns nil, nil when no WAL file is configured, the file is empty, or the header is incomplete or fails its checksum.
// Such a header holds no durable commit (see parseFileHeader).
// It stops at the first invalid record, including a record from an earlier generation.
// File I/O failures still return an error.
// After header validation it sets [WAL.appendOffset] to the file size.
// The caller must clear an incomplete tail before a later [WAL.Commit].
func (wal *WAL) ReadRecords() ([]WALRecord, error) {
	if wal.file == nil {
		return nil, nil
	}
	info, err := wal.file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		wal.fileKnownEmpty = true
		return nil, nil
	}

	var data [walFileHeaderSize]byte
	headerData := data[:min(info.Size(), walFileHeaderSize)]
	if _, err := wal.file.ReadAt(headerData, 0); err != nil {
		return nil, fmt.Errorf("read WAL header: %w", err)
	}
	salt, complete, err := parseFileHeader(headerData)
	if err != nil {
		return nil, err
	}
	if !complete {
		return nil, nil
	}
	wal.salt, wal.checksumSeed = salt, saltChecksumSeed(salt)
	wal.appendOffset = info.Size()
	// Recovered bytes can still be in the operating system cache.
	wal.hasUnsyncedWrites = true

	reader := io.NewSectionReader(wal.file, walFileHeaderSize, info.Size()-walFileHeaderSize)
	var records []WALRecord
	for {
		record, err := DecodeWALRecord(reader, wal.pageSize, wal.checksumSeed)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, page.ErrChecksum) {
				return nil, err
			}
			break
		}
		if record.Header.Type > RecordTypeEntryPatch || record.Header.TxID == 0 ||
			(IsCommitMarker(record) && (record.Header.PageID != 0 || len(record.Payload) != 0)) {
			break
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
		// Production leaves syncFile nil and syncs the WAL data directly.
		err = fileio.SyncData(wal.file)
	}
	if err != nil {
		return err
	}
	if wal.syncDirectory != nil {
		// A file sync does not persist a new file name. The first sync of this WAL also syncs its directory.
		if err := wal.syncDirectory(); err != nil {
			return err
		}
		wal.syncDirectory = nil
	}
	wal.hasUnsyncedWrites = false
	// A sync stores every earlier change of the file, a truncate too.
	wal.headerSyncNeeded = false
	return nil
}

// SyncTruncate makes an earlier truncate durable, so the first commit of the next generation need not sync its header.
// The database calls it after an automatic checkpoint, which already does maintenance syncs.
// It makes no system call when no truncate waits for a sync, or when the WAL does not sync.
func (wal *WAL) SyncTruncate() error {
	if !wal.headerSyncNeeded || !wal.syncOnCheckpoint {
		return nil
	}
	wal.hasUnsyncedWrites = true
	return wal.Sync()
}

// Truncate clears the WAL file and resets [WAL.appendOffset].
// It does not clear an append failure. Only a new WAL for a reopened database accepts commits again.
//
// Only truncate after the WAL contents have been ingested into the database.
// Truncate makes no system call when the file is known to be empty, for example after Open finds an empty WAL.
//
// The next commit starts a new generation at offset zero even when the truncate fails, because a failed truncate can still change the size.
// The salt of the new generation makes every old record after it invalid.
// A sync of the truncate (see [WAL.SyncTruncate]) or the header sync of the next generation makes that durable.
// So the old records need not go away.
func (wal *WAL) Truncate() error {
	wal.appendOffset = 0
	if wal.fileKnownEmpty {
		return nil
	}
	wal.headerSyncNeeded = true
	if err := wal.file.Truncate(0); err != nil {
		return err
	}
	wal.fileKnownEmpty = true
	return nil
}

// AppendFailure returns the error that stopped commits after a failed append, or nil.
// The caller must hold the same lock that it holds for [WAL.Commit].
func (wal *WAL) AppendFailure() error {
	return wal.appendFailure
}

// Delete the WAL file
// Important: only delete the file when running db.Close()
func (wal *WAL) Delete() error {
	// close the file before deletion
	err := wal.Close()
	if err != nil {
		return err
	}
	// delete the file from the disk
	err = wal.fs.Remove(wal.path)
	if errors.Is(err, fs.ErrNotExist) {
		// A failed earlier remove can still remove the name.
		// The goal of a retry is a missing file, so this is success.
		return nil
	}
	return err
}

// appendTransaction writes the file header before the first transaction.
// It writes at [WAL.appendOffset] and advances the WAL size counters.
func (wal *WAL) appendTransaction(transaction []byte) (bool, error) {
	writeOffset := wal.appendOffset
	bytesWritten := len(transaction)
	// A write that fails can still leave bytes in the file.
	wal.fileKnownEmpty = false
	if writeOffset == 0 {
		header := encodeWALFileHeader(wal.salt)
		if err := fileio.WriteFullAt(wal.file, header[:], 0); err != nil {
			return false, fmt.Errorf("persist WAL header: %w", err)
		}
		// A checkpoint does not sync its WAL truncate, and an earlier process can have truncated this file without a sync, so storage can still hold old records.
		// Without this sync, a power failure can keep records of the new generation but lose the truncate and the new header.
		// The old header then validates a prefix of the old records, and recovery applies a state older than the main file.
		// SQLite syncs a new WAL header for the same reason (ticket ff5be73dee).
		if wal.headerSyncNeeded && wal.syncOnCheckpoint {
			wal.hasUnsyncedWrites = true
			if err := wal.Sync(); err != nil {
				return false, err
			}
		}
		writeOffset = walFileHeaderSize
		bytesWritten += walFileHeaderSize
	}
	if err := fileio.WriteFullAt(wal.file, transaction, writeOffset); err != nil {
		return false, fmt.Errorf("persist multiple records: %w", err)
	}
	wal.appendOffset = writeOffset + int64(len(transaction))
	wal.hasUnsyncedWrites = true
	wal.bytesSinceCheckpoint += uint64(bytesWritten)

	needsCheckpoint := wal.reachedCheckpointThreshold()
	if wal.syncOnCommit || (needsCheckpoint && wal.syncOnCheckpoint) {
		if err := wal.Sync(); err != nil {
			return false, err
		}
	}

	return needsCheckpoint, nil
}

// startGeneration selects the salt for a WAL that starts again at offset zero.
// Its checksum seed differs from the previous generation, so no record of that generation passes validation.
func (wal *WAL) startGeneration() {
	previous := wal.checksumSeed
	for {
		salt := rand.Uint64()
		seed := saltChecksumSeed(salt)
		if seed != previous {
			wal.salt, wal.checksumSeed = salt, seed
			return
		}
	}
}

// saltChecksumSeed returns the CRC32C checksum of the file encoding of salt.
func saltChecksumSeed(salt uint64) uint32 {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], salt)
	return checksum.Sum32(data[:])
}

func encodeWALFileHeader(salt uint64) [walFileHeaderSize]byte {
	var header [walFileHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], walFileMagic)
	binary.LittleEndian.PutUint32(header[4:8], page.FormatVersion)
	binary.LittleEndian.PutUint64(header[8:16], salt)
	binary.LittleEndian.PutUint32(header[16:20], checksum.Sum32(header[:16]))
	return header
}

// parseFileHeader returns the salt from the header at the start of WAL file data.
// It reports false when data is shorter than a header or fails its checksum, as zeros or a torn header do.
// A crash can tear a header only before the first sync of its generation, so such a WAL holds no durable commit.
// SQLite also ignores a WAL whose header checksum fails.
// A header with a valid checksum but another magic number or format version is an error.
func parseFileHeader(data []byte) (salt uint64, complete bool, err error) {
	if len(data) < walFileHeaderSize || checksum.Sum32(data[:16]) != binary.LittleEndian.Uint32(data[16:20]) {
		return 0, false, nil
	}
	if binary.LittleEndian.Uint32(data[0:4]) != walFileMagic {
		return 0, false, fmt.Errorf("read WAL magic: %w", page.ErrInvalid)
	}
	if version := binary.LittleEndian.Uint32(data[4:8]); version != page.FormatVersion {
		return 0, false, fmt.Errorf("read WAL format version %d: %w", version, page.ErrVersionMismatch)
	}
	return binary.LittleEndian.Uint64(data[8:16]), true, nil
}

// RecordChecksumSeed returns the record checksum seed of the WAL file that starts with data.
// Use it with [EncodeWALRecord] and [DecodeWALRecord].
func RecordChecksumSeed(data []byte) (uint32, error) {
	salt, complete, err := parseFileHeader(data[:min(len(data), walFileHeaderSize)])
	if err != nil {
		return 0, err
	}
	if !complete {
		return 0, fmt.Errorf("WAL header is incomplete: %w", page.ErrInvalid)
	}
	return saltChecksumSeed(salt), nil
}

func (wal *WAL) reachedCheckpointThreshold() bool {
	return wal.bytesSinceCheckpoint >= wal.checkpointThresholdBytes
}

// CommittedPageToNode returns the decoded node for a committed data page.
// It reads all continuation payloads from the committed WAL overlay.
// Each payload must come from a verified WAL record or a successful Commit.
func (wal *WAL) CommittedPageToNode(record *CommittedPage) (*btree.Node, error) {
	if record.Node != nil {
		return record.Node, nil
	}
	if record.Payload == nil {
		return nil, ErrRecordPayloadRequired
	}

	node, err := btree.DecodeWALNode(record.Payload, record.Header.PageID, wal.pageSize, wal.readNodePage)

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node: %w", err)
	}

	return node, nil
}

// LookupCommittedPage searches one committed leaf or routes through one branch.
// A decoded record uses its Node. An encoded record is searched in its page images.
// It reads continuation images from the committed WAL overlay.
// Normal entries borrow their source bytes. Keep source bytes unchanged while using borrowed results.
// A continued leaf owns a copy of its value, and its key is a copy of key.
// Set includeKey to false to omit the key from encoded lookup results.
// Decoded entries still borrow their key and value from the Node.
func (wal *WAL) LookupCommittedPage(record *CommittedPage, key []byte, includeKey bool) (btree.Entry, bool, page.ID, error) {
	if record.Node != nil {
		return btree.LookupDecodedNode(record.Node, key)
	}
	return btree.LookupEncodedWALNode(record.Payload, record.Header.PageID, key, wal.pageSize, wal.readNodePage, includeKey)
}

// VisitCommittedPageReferences visits child nodes and continuation IDs separately.
// Encoded nodes read all their storage pages from this WAL's committed view.
// Each payload must come from a verified WAL record or a successful Commit.
func (wal *WAL) VisitCommittedPageReferences(record *CommittedPage, visit, visitContinuation func(page.ID) error) error {
	if record.Node != nil {
		return btree.VisitNodeReferences(record.Node, visit, visitContinuation)
	}
	if record.Payload == nil {
		return ErrRecordPayloadRequired
	}
	return btree.VisitWALNodeReferences(record.Payload, record.Header.PageID, wal.pageSize, wal.readNodePage, visit, visitContinuation)
}

// readNodePage returns a compact node payload from the committed WAL overlay.
// All continuation pages of a WAL node belong to that overlay. Missing pages
// are errors; a main-file page can belong to an older version of the node.
func (wal *WAL) readNodePage(id page.ID) ([]byte, error) {
	record, ok := wal.Lookup(id)
	if !ok || record.Header.Type != RecordTypeNode || record.Payload == nil {
		return nil, fmt.Errorf("read WAL node page %d: %w", id, ErrRecordPayloadRequired)
	}
	return record.Payload, nil
}
