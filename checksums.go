package kvlite

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// SetDataPageChecksums changes checksum calculation and checks for this file's data pages.
// The file stores the new mode. Reopen and Backup preserve it.
// Selecting the current mode does no conversion.
// A read-only database returns [ErrDatabaseReadOnly]. A memory database returns [errors.ErrUnsupported].
//
// Disabling commits a metadata change without scanning data pages.
// Untouched pages retain their old checksum fields. Later changed pages receive zero.
// Disabled reads ignore these fields. Metadata, allocation pages, and WAL checksums stay enabled.
//
// Enabling waits for active backups and blocks other database operations during conversion.
// It checks retained nonzero checksums and validates node structure.
// It calculates missing checksums before it commits the enabled mode.
// This cannot detect past damage to bytes that had no checksum.
// A failed conversion leaves the mode disabled. Converted pages can retain checksums.
// A later call can retry the conversion. After [ErrWALWriteFailed], reopen the database before the retry.
// Storage synchronization follows [Options.Synchronous].
//
// Do not call SetDataPageChecksums inside a transaction callback.
func (db *DB) SetDataPageChecksums(enabled bool) error {
	if err := db.beginOperation(); err != nil {
		return err
	}
	defer db.endOperation()
	if db.options.ReadOnly {
		return ErrDatabaseReadOnly
	}
	if db.memoryNodes != nil {
		return errors.ErrUnsupported
	}

	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	for enabled && !db.meta.DataPageChecksums() && db.activeBackups > 0 {
		done := db.backupsDone
		db.operationMu.Unlock()
		<-done
		db.operationMu.Lock()
	}
	// A conversion checkpoints before it commits, so it must stop before that work too.
	if err := db.writeFailure(); err != nil {
		return err
	}
	if db.meta.DataPageChecksums() == enabled {
		return nil
	}

	var root *btree.Node
	if enabled {
		if err := db.enableDataPageChecksums(); err != nil {
			return err
		}
		data, err := db.readMainNodePage(db.meta.Root())
		if err != nil {
			return err
		}
		root, err = btree.DecodeNode(slices.Clone(data), db.meta.Root(), db.meta.PageSize(), db.readMainNodePage, true)
		if err != nil {
			return err
		}
	}
	if err := db.updateDirect(func(tx *Tx) error {
		tx.writableMeta().SetDataPageChecksums(enabled)
		tx.metaDirty = true
		return nil
	}); err != nil {
		return err
	}
	if enabled {
		clear(db.writeNodes)
		db.writeNodeSlots = nil
		db.nextWriteNodeSlot = 0
		db.rootNode = root
	}
	return nil
}

// enableDataPageChecksums protects complete nodes in batches.
// The mode stays disabled until the caller commits the final metadata change.
// SyncFull and SyncNormal write redo images first. SyncNone writes directly without recovery protection.
// The caller must hold operationMu and have no active backups.
func (db *DB) enableDataPageChecksums() error {
	// Resolve old changes before checking retained sums. A failed conversion can
	// leave redo images that must replace an interrupted main-file write first.
	if err := db.checkpointWAL(); err != nil {
		return err
	}
	const batchBytes = 1 << 20
	var nodes []wal.NodeRecord
	size := int64(0)
	flush := func() error {
		if len(nodes) == 0 {
			return nil
		}
		if db.options.Synchronous == SyncNone {
			if err := db.writeDirectPages(db.meta, nodes, nil, nil, true); err != nil {
				return err
			}
		} else {
			if _, err := db.wal.Commit(nodes, nil, nil, nil); err != nil {
				return err
			}
			if err := db.checkpointWALWithChecksums(true); err != nil {
				return err
			}
		}
		clear(nodes)
		nodes = nodes[:0]
		size = 0
		return nil
	}
	for id := firstTreePageID; id <= db.meta.LastPage(); id++ {
		if !db.allocation.allocated(id) || db.allocation.isSegmentPage(id) {
			continue
		}
		data, err := db.readMainNodePage(id)
		if err != nil {
			return err
		}
		node, err := btree.DecodeNode(slices.Clone(data), id, db.meta.PageSize(), db.readMainNodePage, true)
		if err != nil {
			return fmt.Errorf("enable data-page checksums at page %d: %w", id, err)
		}
		missing := btree.CheckNodeChecksumMode(data, true) != nil
		id += page.ID(node.StoragePageCount() - 1)
		if !missing {
			continue
		}
		// One continued node can exceed the batch limit. Its existing codec needs
		// the complete logical body, and its pages form one redo transaction.
		nodeSize := int64(btree.NodePageCount(node, db.meta.PageSize())) * db.meta.PageSize()
		if size+nodeSize > batchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		nodes = append(nodes, wal.NodeRecord{Final: node})
		size += nodeSize
		if size >= batchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	// Check every complete node, including its raw tail bytes. The mode
	// must not become enabled if any node has a missing or wrong checksum.
	for id := firstTreePageID; id <= db.meta.LastPage(); id++ {
		if !db.allocation.allocated(id) || db.allocation.isSegmentPage(id) {
			continue
		}
		data, err := db.readMainNodePage(id)
		if err != nil {
			return err
		}
		if err := btree.CheckNodeChecksum(data, id); err != nil {
			return fmt.Errorf("enable data-page checksums at page %d: %w", id, err)
		}
		count, err := btree.NodeSpan(data, id, db.meta.PageSize())
		if err != nil {
			return err
		}
		id += page.ID(count - 1)
	}
	// Valid retained sums can come from an earlier SyncNone handle. Sync the
	// whole main file before committing ON, even if this pass changed no pages.
	if db.options.Synchronous != SyncNone {
		return fileio.SyncData(db.file)
	}
	return nil
}
