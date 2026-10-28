package kvlite

import (
	"errors"
	"fmt"
	"math"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// Checkpoint moves committed WAL data into the main database file and clears the WAL without closing the database.
// In [SyncFull] and [SyncNormal], it synchronizes the WAL and main file before clearing the WAL, regardless of the automatic checkpoint threshold.
// In [SyncNone], it moves the data without storage synchronization. With [Options.DisableWAL] enabled, or in a memory database, it has no WAL changes to move and returns nil. Use [DB.Sync] to force storage synchronization in SyncNone.
//
// Checkpoint waits for active transaction callbacks. A WAL checkpoint also waits for active backups and blocks database operations while it runs.
// Do not call it from a View or Update callback. Writes that have not committed when checkpointing begins are not covered.
// A read-only handle returns [ErrDatabaseReadOnly], and a closed handle returns [ErrDatabaseNotOpen].
// A checkpoint error is returned and recorded in [DB.Stats]. Main-file write and sync failures can be retried; a WAL sync failure stops later data operations with [ErrWALWriteFailed].
func (db *DB) Checkpoint() error {
	if err := db.beginWriteOperation(); err != nil {
		return err
	}
	defer db.endOperation()
	db.operationMu.Lock()
	defer db.operationMu.Unlock()
	if err := db.writeFailure(); err != nil {
		return err
	}
	if db.memoryNodes != nil || db.options.DisableWAL {
		return nil
	}
	for db.activeBackups > 0 {
		done := db.backupsDone
		db.operationMu.Unlock()
		<-done
		db.operationMu.Lock()
	}
	if err := db.writeFailure(); err != nil {
		return err
	}
	return db.checkpointWAL()
}

// checkpointWAL copies committed WAL pages into the main file.
// SyncFull and SyncNormal make the WAL and main file durable before WAL cleanup.
// SyncNone does not issue storage sync calls.
// The WAL removes its committed state only after the required steps succeed.
// A failed write or sync can be retried.
// Checkpoint failures are recorded for [DB.Stats], including WAL cleanup and mapping failures.
//
// The checkpoint refreshes the mapping before another operation can read it.
//
// The caller must hold the exclusive database operation lock.
func (db *DB) checkpointWAL() (err error) {
	return db.checkpointWALWithChecksums(db.meta.DataPageChecksums())
}

// checkpointWALWithChecksums selects data-page protection for this checkpoint.
// Checksum conversion uses true while the committed file mode remains disabled.
// The caller must hold the exclusive database operation lock.
func (db *DB) checkpointWALWithChecksums(enabled bool) (err error) {
	if db.activeBackups > 0 {
		return nil
	}
	defer func() {
		db.stats.LastCheckpointError = err
		if err != nil {
			db.stats.CheckpointFailureCount++
		}
	}()
	if db.wal.Stats().CommittedRecordCount == 0 {
		// A failed WAL truncate can leave the main file current and the overlay empty.
		// Retry that cleanup; Truncate does no file work when the WAL is already empty.
		return db.wal.Truncate()
	}
	targetSize, err := db.logicalFileSize()
	if err != nil {
		return err
	}
	// A mapping past the new end could become invalid after truncate.
	if int64(len(db.mappedFile)) > targetSize {
		if err := db.unmapMainFile(); err != nil {
			return err
		}
	}
	checkpointErr := db.wal.Checkpoint(db.file, targetSize, enabled)
	mapErr := db.refreshMainFileMapping()
	return errors.Join(checkpointErr, mapErr)
}

// logicalFileSize is the byte length through the last allocated page.
func (db *DB) logicalFileSize() (int64, error) {
	return fileSizeForMeta(db.meta)
}

// fileSizeForMeta returns the logical file size of private or committed metadata.
func fileSizeForMeta(meta *page.Meta) (int64, error) {
	pageSize := meta.PageSize()
	lastPage := uint64(meta.LastPage())
	if pageSize <= 0 || lastPage >= uint64(math.MaxInt64)/uint64(pageSize) {
		return 0, fmt.Errorf("calculate database file size: %w", page.ErrInvalid)
	}
	return int64(lastPage+1) * pageSize, nil
}

// writeDirectPages writes private transaction pages before their state becomes visible.
// The caller holds operationMu and has waited for active backups.
// A failure stops data operations because the main file can be partly changed.
func (db *DB) writeDirectPages(meta *page.Meta, nodes []wal.NodeRecord, allocationPages []wal.AllocationRecord, encodedMeta []byte, checksums bool) (err error) {
	defer func() {
		if err != nil {
			db.mainWriteFailure = fmt.Errorf("%w: %w", ErrDatabaseWriteFailed, err)
			err = db.mainWriteFailure
		}
	}()
	if db.activeBackups != 0 {
		return page.ErrInvalid
	}
	targetSize, err := fileSizeForMeta(meta)
	if err != nil {
		return err
	}
	// Hide bytes past the new end before the file is shortened.
	if int64(len(db.mappedFile)) > targetSize {
		if !db.resizeDirectMappedView(targetSize) {
			if err := db.unmapMainFile(); err != nil {
				return err
			}
		}
	}
	if err := db.wal.WriteDirect(db.directWriteFile(), targetSize, checksums, nodes, allocationPages, encodedMeta); err != nil {
		return err
	}
	if db.resizeDirectMappedView(targetSize) {
		return nil
	}
	if int64(len(db.mappedFile)) == targetSize && db.mainFileMappingFollowsWrites() {
		return nil
	}
	return db.refreshMainFileMapping()
}

// WriteNode encodes a prepared node into its mapped main-file span.
// The caller holds the operation write lock. Transaction nodes keep their key and
// value bytes outside the file mapping, so encoding cannot overwrite its input.
// A span outside the visible mapping uses the ordinary writer without changes here.
func (file directMappedFile) WriteNode(node *btree.Node, pageSize int64, checksums bool) (bool, error) {
	if node == nil || pageSize < btree.NodeHeaderSize {
		return false, ErrInvalid
	}
	count := btree.NodePageCount(node, pageSize)
	if count <= 0 || uint64(node.PageID()) > uint64(math.MaxInt64)/uint64(pageSize) {
		return false, ErrInvalid
	}
	offset := int64(node.PageID()) * pageSize
	size := int64(count) * pageSize
	if offset > int64(len(file.db.mappedFile)) || size > int64(len(file.db.mappedFile))-offset {
		return false, nil
	}
	return true, btree.EncodeNodeInto(file.db.mappedFile[offset:offset+size:offset+size], node, pageSize, checksums)
}
