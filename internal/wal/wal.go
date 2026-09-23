package wal

import (
	"bytes"
	"encoding/binary"
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
const maxRetainedEncodingBufferBytes = 2 << 20

type WAL struct {
	path                     string
	file                     *os.File
	pageSize                 int64
	syncOnCommit             bool
	syncOnCheckpoint         bool
	checkpointThresholdBytes uint64
	bytesSinceCheckpoint     uint64
	totalBytesWritten        uint64
	checkpointCount          uint64
	overlay                  map[page.ID]CommittedPage // Committed pages that are not yet checkpointed.
	nextTxid                 TxID                      // sequence number stamped on the next committed transaction
	hasUnsyncedWrites        bool                      // true when WAL bytes were appended after the last successful sync
	appendOffset             int64                     // byte offset of the next WAL append; independent of the file read/write cursor
	encodingBuffer           []byte                    // reusable transaction encoding buffer; capacity may be cleared after large commits
	pagePatch                PagePatch                 // reusable page patch; encoded into encodingBuffer before reuse
	appendFailure            error                     // set when rollback truncate fails; blocks later commits until [WAL.Truncate] succeeds
	syncFile                 func() error              // syncFile is a test hook for sync failures and call counts.
}

// Commit appends one transaction and publishes each changed page to the committed overlay.
// It returns whether the committed WAL size reached the checkpoint threshold.
//
// nodes contains original and final data-page images.
// encodedMeta contains the encoded metadata when it changed. It can be nil or empty when metadata did not change.
// Commit writes changed metadata to both metadata pages.
// The caller must not change committed nodes or encodedMeta before the next checkpoint.
//
// Appends use [WAL.appendOffset], not the WAL file read/write cursor, so unrelated seeks on the file cannot corrupt the log.
//
// On failure, Commit truncates the failed append and restores the pre-append byte and synchronization state without updating the overlay.
// If that rollback truncate fails, Commit stores the truncate error and later Commit calls return it until [WAL.Truncate] succeeds.
func (wal *WAL) Commit(nodes []NodeRecord, encodedMeta []byte) (bool, error) {
	if wal.appendFailure != nil {
		return false, fmt.Errorf("WAL append is disabled after failed rollback: %w", wal.appendFailure)
	}
	if len(nodes) == 0 && len(encodedMeta) == 0 {
		return false, nil
	}

	transaction, err := wal.encodeTransaction(nodes, encodedMeta, wal.nextTxid)
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
	wal.totalBytesWritten += uint64(len(transaction))

	for _, node := range nodes {
		header := RecordHeader{Type: RecordTypeNode, PageID: node.Final.PageID(), TxID: wal.nextTxid}
		wal.overlay[header.PageID] = CommittedPage{Header: header, Node: node.Final}
	}
	if len(encodedMeta) > 0 {
		header := RecordHeader{Type: RecordTypeMeta, TxID: wal.nextTxid}
		for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
			header.PageID = pageID
			wal.overlay[pageID] = CommittedPage{Header: header, Payload: encodedMeta}
		}
	}
	wal.nextTxid++
	return needsCheckpoint, nil
}

// releaseEncodingBuffer drops an oversized transaction encoding buffer after Commit returns.
func (wal *WAL) releaseEncodingBuffer() {
	if cap(wal.encodingBuffer) > maxRetainedEncodingBufferBytes {
		wal.encodingBuffer = nil
	}
	if cap(wal.pagePatch.ranges) > maxRetainedEncodingBufferBytes {
		wal.pagePatch.ranges = nil
	}
}

// encodeTransaction encodes changed nodes, optional metadata, and one commit marker.
// encodedMeta can be nil or empty when metadata did not change.
func (wal *WAL) encodeTransaction(nodes []NodeRecord, encodedMeta []byte, txid TxID) ([]byte, error) {
	transactionSize := HeaderSize + ChecksumSize
	for index := range nodes {
		if nodes[index].Final == nil {
			return nil, fmt.Errorf("WAL node record has no final node")
		}
		// Size for the complete final image: patches are only written when smaller, so this is a safe upper bound for the encoding buffer capacity.
		payloadSize := btree.WALNodeEncodedSize(nodes[index].Final)
		if int64(payloadSize) > wal.pageSize {
			return nil, fmt.Errorf("record payload exceeds page size (%d > %d)", payloadSize, wal.pageSize)
		}
		transactionSize += HeaderSize + payloadSize + ChecksumSize
	}
	if len(encodedMeta) > 0 {
		if int64(len(encodedMeta)) > wal.pageSize {
			return nil, fmt.Errorf("metadata payload exceeds page size (%d > %d)", len(encodedMeta), wal.pageSize)
		}
		transactionSize += 2 * (HeaderSize + len(encodedMeta) + ChecksumSize)
	}

	transaction := wal.encodingBuffer[:0]
	if cap(transaction) < transactionSize {
		transaction = make([]byte, 0, transactionSize)
	}
	for index := range nodes {
		var err error
		transaction, err = wal.appendNodeRecord(transaction, &nodes[index], txid)
		if err != nil {
			return nil, err
		}
	}
	if len(encodedMeta) > 0 {
		header := RecordHeader{Type: RecordTypeMeta, TxID: txid}
		for _, pageID := range [...]page.ID{page.Meta0ID, page.Meta1ID} {
			header.PageID = pageID
			transaction = appendEncodedPayloadRecord(transaction, header, encodedMeta)
		}
	}
	transaction = appendEncodedPayloadRecord(transaction, RecordHeader{Type: RecordTypeCommit, TxID: txid}, nil)
	wal.encodingBuffer = transaction[:0]
	return transaction, nil
}

// appendNodeRecord appends one complete node image or a smaller page patch to transaction.
func (wal *WAL) appendNodeRecord(transaction []byte, record *NodeRecord, txid TxID) ([]byte, error) {
	targetSize := btree.WALNodeEncodedSize(record.Final)
	header := RecordHeader{Type: RecordTypeNode, PageID: record.Final.PageID(), TxID: txid}
	if record.Original == nil {
		return appendEncodedNodeRecord(transaction, header, record.Final), nil
	}
	if record.Original.PageID() != header.PageID {
		return nil, fmt.Errorf("WAL node record changes page %d from page %d", header.PageID, record.Original.PageID())
	}

	wal.pagePatch.reset()
	var encodedSize int
	var compatible bool
	if record.Final.IsLeaf() {
		encodedSize, compatible = appendLeafNodePatch(&wal.pagePatch, record.Original, record.Final)
	} else {
		encodedSize, compatible = appendBranchNodePatch(&wal.pagePatch, record.Original, record.Final)
	}
	if compatible && encodedSize == targetSize && wal.pagePatch.encodedSize() < targetSize {
		header.Type = RecordTypePatch
		return appendEncodedPagePatchRecord(transaction, header, &wal.pagePatch), nil
	}
	// A different entry count or field length moves later bytes to new offsets.
	// Use the complete image when the direct scan cannot report fixed-offset changes,
	// or when the patch is not smaller than the complete image.
	return appendEncodedNodeRecord(transaction, header, record.Final), nil
}

// appendLeafNodePatch scans one leaf once and appends changed value bytes.
// A patch is possible only when flags, keys, and value lengths keep the same encoded layout.
func appendLeafNodePatch(patch *PagePatch, base, target *btree.Node) (encodedSize int, compatible bool) {
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
			patch.appendChangedRanges(valueOffset, baseValue, targetValue)
		}
		offset = valueOffset + len(targetValue)
	}
	return offset, true
}

// appendBranchNodePatch scans one branch once and appends changed child IDs and separator keys.
// A patch is possible only when the child count and separator-key lengths keep the same encoded layout.
func appendBranchNodePatch(patch *PagePatch, base, target *btree.Node) (encodedSize int, compatible bool) {
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
			patch.appendChangedRanges(offset, baseChild[:], targetChild[:])
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
			patch.appendChangedRanges(offset, baseKey, targetKey)
		}
		offset += len(targetKey)
	}
	return offset, true
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
func (wal *WAL) ReadRecords() ([]WALRecord, error) {
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

	var records []WALRecord
	for {
		record, err := DecodeWALRecord(wal.file, wal.pageSize)
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
		// Production leaves syncFile nil and syncs the WAL data directly.
		err = fileio.SyncData(wal.file)
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
	if wal.syncOnCommit || (needsCheckpoint && wal.syncOnCheckpoint) {
		if err := wal.Sync(); err != nil {
			return false, err
		}
	}

	return needsCheckpoint, nil
}

func (wal *WAL) reachedCheckpointThreshold() bool {
	return wal.bytesSinceCheckpoint >= wal.checkpointThresholdBytes
}

// CommittedPageToNode returns the decoded node for a committed data page.
func CommittedPageToNode(record *CommittedPage) (*btree.Node, error) {
	if record.Node != nil {
		return record.Node, nil
	}
	if record.Payload == nil {
		return nil, fmt.Errorf("record has no payload")
	}

	node, err := btree.DecodeWALNode(record.Payload, record.Header.PageID)

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node: %w", err)
	}

	return node, nil
}
