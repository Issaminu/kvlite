package kvlite

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// replayWAL makes committed WAL pages visible in the selected open mode.
// It omits old allocation segments that a later commit removed from the file tail.
func (db *DB) replayWAL(committed []wal.WALRecord) error {
	if len(committed) == 0 {
		return nil
	}
	var latestMetaTxID wal.TxID
	for _, record := range committed {
		if record.Header.Type == wal.RecordTypeMeta {
			latestMetaTxID = record.Header.TxID
		}
	}
	visible := committed[:0]
	for _, record := range committed {
		if record.Header.Type == wal.RecordTypeAllocation && !db.allocation.isSegmentHead(record.Header.PageID) {
			pagesPerGroup := db.allocation.pagesPerGroup()
			frames := page.AllocationSegmentPages(db.meta.PageSize())
			if record.Header.PageID <= db.meta.LastPage() || record.Header.PageID%pagesPerGroup >= frames || record.Header.TxID >= latestMetaTxID {
				return fmt.Errorf("allocation WAL record targets page %d: %w", record.Header.PageID, page.ErrInvalid)
			}
			continue
		}
		visible = append(visible, record)
	}
	committed = visible
	if db.options.ReadOnly {
		return db.loadCommittedIntoOverlay(committed)
	}
	return db.ingestWalRecords(committed)
}

// readOrCreateWal opens the WAL of path and reads its committed prefix.
// It also reports whether the WAL file existed before this call.
// With discard, it clears the WAL instead, because the empty main file holds no database (see open).
// syncDirectory is nil when the handle must not sync the database directory.
func (db *DB) readOrCreateWal(fsys fileio.FS, path string, pageSize int64, syncDirectory func() error, discard bool) (*wal.WAL, []wal.WALRecord, bool, error) {
	// Open supplies the resolved absolute path so symbolic links share one log.
	walPath := path + "-wal"
	var walFile fileio.File
	var err error
	var found bool

	if db.options.ReadOnly {
		walFile, err = fsys.OpenFile(walPath, os.O_RDONLY, 0)
		found = err == nil
	} else {
		info, statErr := db.file.Stat()
		if statErr != nil {
			return nil, nil, false, statErr
		}
		// Close removes the WAL, so the Open of a cleanly closed database creates it.
		// O_EXCL creates a missing WAL and reports an existing one in the same call, so that Open makes no failed open call.
		walFile, err = fsys.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
		if errors.Is(err, os.ErrExist) {
			// O_CREATE also creates the target of a dangling symbolic link, which O_EXCL refuses.
			walFile, err = fsys.OpenFile(walPath, os.O_RDWR|os.O_CREATE, info.Mode().Perm())
			found = err == nil
		}
	}

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			walFile = nil
		} else {
			return nil, nil, false, err
		}
	}

	if !db.options.ReadOnly && !db.options.DisableWAL {
		walFile = mapWALWrites(walFile)
	}
	log := wal.New(wal.Config{
		Path:                     walPath,
		File:                     walFile,
		FS:                       fsys,
		PageSize:                 pageSize,
		SyncOnCommit:             db.options.Synchronous == SyncFull,
		SyncOnCheckpoint:         db.options.Synchronous != SyncNone,
		CheckpointThresholdBytes: db.options.CheckpointThresholdBytes,
		SyncDirectory:            syncDirectory,
	})

	if discard {
		// The first commit of a handle syncs the WAL header before its records, and that sync also makes this truncate durable.
		// So no record of this WAL can come back after a power failure.
		if err := log.Truncate(); err != nil {
			return nil, nil, false, errors.Join(err, log.Close())
		}
		return log, nil, found, nil
	}
	records, err := log.ReadRecords()
	if err != nil {
		return nil, nil, false, errors.Join(err, log.Close())
	}

	return log, records, found, nil
}

func (db *DB) ingestWalRecords(committed []wal.WALRecord) error {
	committed, err := db.materializeWALPages(committed)
	if err != nil {
		return err
	}
	for _, record := range committed {
		db.wal.LoadCommittedRecord(record)
	}
	targetSize, err := db.logicalFileSize()
	if err != nil {
		return err
	}
	return db.wal.Checkpoint(db.file, targetSize, db.meta.DataPageChecksums())
}

func committedWALRecords(records []wal.WALRecord) []wal.WALRecord {
	committed := make([]wal.WALRecord, 0, len(records))
	pending := make([]wal.WALRecord, 0)

	for _, record := range records {
		if !wal.IsCommitMarker(&record) {
			pending = append(pending, record)
			continue
		}

		matches := len(pending) > 0 && record.Header.TxID != 0 && record.Header.PageID == 0 && len(record.Payload) == 0
		for _, candidate := range pending {
			if candidate.Header.TxID != record.Header.TxID {
				matches = false
				break
			}
		}
		if !matches {
			break
		}

		committed = append(committed, pending...)
		pending = pending[:0]
	}

	return committed
}

// metaFromCommittedRecords returns the newest valid metadata from committed WAL records. It returns nil when the records do not contain metadata.
func metaFromCommittedRecords(committed []wal.WALRecord) (*page.Meta, error) {
	// Scan backward because the newest committed transaction contains the latest metadata generation.
	for _, record := range slices.Backward(committed) {
		if record.Header.Type != wal.RecordTypeMeta || !page.IsMetaID(record.Header.PageID) {
			continue
		}
		meta, err := page.DecodeMeta(record.Payload)
		if err != nil {
			return nil, err
		}
		if err := validateMeta(meta); err != nil {
			return nil, err
		}
		return meta, nil
	}
	return nil, nil
}

// loadCommittedIntoOverlay makes committed WAL pages visible to a read-only database. It does not change the main file.
func (db *DB) loadCommittedIntoOverlay(committed []wal.WALRecord) error {
	committed, err := db.materializeWALPages(committed)
	if err != nil {
		return err
	}
	// Check all data before any record becomes visible.
	for _, record := range committed {
		if record.Header.Type != wal.RecordTypeNode {
			continue
		}
		if err := btree.ValidateWALNode(record.Payload, record.Header.PageID); err != nil {
			return err
		}
	}
	for _, record := range committed {
		db.wal.LoadCommittedRecord(record)
	}
	return nil
}

// isWALPageRecord reports whether recordType stores a complete image or a patch of one tree or allocation page.
func isWALPageRecord(recordType wal.RecordType) bool {
	switch recordType {
	case wal.RecordTypeNode, wal.RecordTypeBytePatch, wal.RecordTypeEntryPatch, wal.RecordTypeAllocation:
		return true
	}
	return false
}

// walPageChains returns the indexes of the page records of each page, in file order.
func walPageChains(records []wal.WALRecord) map[page.ID][]int {
	chains := make(map[page.ID][]int)
	for index, record := range records {
		if isWALPageRecord(record.Header.Type) {
			chains[record.Header.PageID] = append(chains[record.Header.PageID], index)
		}
	}
	return chains
}

// materializeWALPages rebuilds the latest complete image of each tree and allocation page from its WAL records.
// It puts one complete record at the position of the last record of each page.
// A later Delete can free a page that has older WAL records. Recovery skips that page.
func (db *DB) materializeWALPages(records []wal.WALRecord) ([]wal.WALRecord, error) {
	chains := walPageChains(records)
	materialized := make([]wal.WALRecord, 0, len(records))
	for index, record := range records {
		if isWALPageRecord(record.Header.Type) {
			chain := chains[record.Header.PageID]
			if chain[len(chain)-1] != index || db.allocation != nil && !db.allocation.allocated(record.Header.PageID) {
				continue
			}
			var err error
			if record, err = db.materializeWALPage(records, chain); err != nil {
				return nil, err
			}
		}
		materialized = append(materialized, record)
	}
	return db.visibleWALNodeSpans(materialized)
}

// visibleWALNodeSpans removes an old node whose head became a newer node's raw
// trailing page. Allocation bits alone cannot distinguish that reuse. Other
// overlaps are invalid. No node can overlap allocation metadata or free pages.
func (db *DB) visibleWALNodeSpans(records []wal.WALRecord) ([]wal.WALRecord, error) {
	var nodes []wal.WALRecord
	for _, record := range records {
		if record.Header.Type == wal.RecordTypeNode {
			nodes = append(nodes, record)
		}
	}
	slices.SortFunc(nodes, func(left, right wal.WALRecord) int {
		if left.Header.PageID < right.Header.PageID {
			return -1
		}
		if left.Header.PageID > right.Header.PageID {
			return 1
		}
		return 0
	})
	retired := make(map[page.ID]bool)
	var end page.ID
	var txid wal.TxID
	for _, record := range nodes {
		count, err := wal.ValidateNodeImage(record.Payload, record.Header.PageID, db.meta.PageSize())
		if err != nil {
			return nil, err
		}
		if record.Header.PageID < end {
			if record.Header.TxID < txid {
				retired[record.Header.PageID] = true
				continue
			}
			return nil, fmt.Errorf("overlapping recovered node at page %d: %w", record.Header.PageID, page.ErrInvalid)
		}
		end = record.Header.PageID + page.ID(count)
		txid = record.Header.TxID
	}
	visible := records[:0]
	for _, record := range records {
		if record.Header.Type == wal.RecordTypeNode {
			if retired[record.Header.PageID] {
				continue
			}
			if db.allocation != nil {
				count, _ := btree.NodeSpan(record.Payload, record.Header.PageID, db.meta.PageSize())
				last := record.Header.PageID + page.ID(count) - 1
				if last > db.meta.LastPage() {
					return nil, page.ErrInvalid
				}
				for id := record.Header.PageID; id <= last; id++ {
					if !db.allocation.allocated(id) || db.allocation.isSegmentPage(id) {
						return nil, fmt.Errorf("recovered node uses unavailable page %d: %w", id, page.ErrInvalid)
					}
				}
			}
		}
		visible = append(visible, record)
	}
	return visible, nil
}

// materializeWALPage replays the records of one page, at the chain indexes, from the last complete image of the page.
// It returns one complete record of the type of that image, with the header of the last record.
// A later complete image replaces the older records of a page that was reused.
// The first change to a page after a checkpoint is a complete image (see [wal.WAL.Commit]).
// So each patch changes the image that the earlier WAL records of the page give, and byte patches and entry patches can follow each other in any order.
// Recovery never reads a main-file page as the base of a patch: a partial checkpoint can damage that page.
// A patch with no complete image of its page before it is invalid.
func (db *DB) materializeWALPage(records []wal.WALRecord, chain []int) (wal.WALRecord, error) {
	last := records[chain[len(chain)-1]]
	pageID := last.Header.PageID
	first := len(chain) - 1
	for first >= 0 && records[chain[first]].Header.Type != wal.RecordTypeNode && records[chain[first]].Header.Type != wal.RecordTypeAllocation {
		first--
	}
	if first < 0 {
		return wal.WALRecord{}, fmt.Errorf("WAL patch for page %d has no complete image before it: %w", pageID, page.ErrInvalid)
	}
	base := records[chain[first]]
	image := base.Payload
	imageLimit := int64(wal.MaxNodeImageSize)
	if base.Header.Type == wal.RecordTypeAllocation {
		imageLimit = db.meta.PageSize()
		if int64(len(image)) != imageLimit {
			return wal.WALRecord{}, page.ErrInvalid
		}
	} else if _, err := wal.ValidateNodeImage(image, pageID, db.meta.PageSize()); err != nil {
		return wal.WALRecord{}, err
	}
	// After an entry patch, node holds the latest state instead of image. So a series of entry patches decodes the page once.
	var node *btree.MutableNode
	for _, index := range chain[first+1:] {
		record := records[index]
		switch record.Header.Type {
		case wal.RecordTypeBytePatch:
			if node != nil {
				image, node = btree.EncodeWALNode(node), nil
			}
			var err error
			if image, err = wal.ApplyBytePatch(image, record.Payload, imageLimit); err != nil {
				return wal.WALRecord{}, fmt.Errorf("apply WAL byte patch for page %d: %w", pageID, err)
			}
		case wal.RecordTypeEntryPatch:
			if base.Header.Type != wal.RecordTypeNode {
				return wal.WALRecord{}, fmt.Errorf("WAL entry patch for allocation page %d: %w", pageID, page.ErrInvalid)
			}
			if node == nil {
				var err error
				if node, err = btree.DecodeWALNode(image, pageID, db.meta.PageSize(), nil); err != nil {
					return wal.WALRecord{}, fmt.Errorf("read base of WAL entry patch for page %d: %w", pageID, err)
				}
			}
			if err := wal.ApplyEntryPatch(node, record.Payload); err != nil {
				return wal.WALRecord{}, fmt.Errorf("apply WAL entry patch for page %d: %w", pageID, err)
			}
			if int64(btree.WALNodeEncodedSize(node)) > imageLimit || btree.NodePageCount(node, db.meta.PageSize()) != node.StoragePageCount() {
				return wal.WALRecord{}, fmt.Errorf("WAL entry patch changes the span of node %d: %w", pageID, page.ErrInvalid)
			}
		}
	}
	if node != nil {
		image = btree.EncodeWALNode(node)
	}
	if base.Header.Type == wal.RecordTypeNode {
		if _, err := wal.ValidateNodeImage(image, pageID, db.meta.PageSize()); err != nil {
			return wal.WALRecord{}, err
		}
	} else if int64(len(image)) != imageLimit {
		return wal.WALRecord{}, page.ErrInvalid
	}
	last.Header.Type, last.Payload = base.Header.Type, image
	return last, nil
}
