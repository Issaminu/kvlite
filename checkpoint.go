package kvlite

import (
	"errors"
	"fmt"
	"math"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

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
	if db.wal.Stats().CommittedRecordCount == 0 {
		return nil
	}
	defer func() {
		db.stats.LastCheckpointError = err
		if err != nil {
			db.stats.CheckpointFailureCount++
		}
	}()
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
