package kvlite

import (
	"errors"
	"fmt"
	"math"

	"github.com/Issaminu/kvlite/internal/page"
)

// checkpointWAL copies committed WAL pages into the main file.
// SyncFull and SyncNormal make the WAL and main file durable before WAL cleanup.
// SyncNone does not issue storage sync calls.
// The WAL removes its committed state only after the required steps succeed.
// A failed write or sync can be retried.
//
// The checkpoint refreshes the mapping before another operation can read it.
//
// The caller must hold the exclusive database operation lock.
func (db *DB) checkpointWAL() error {
	if db.wal.Stats().CommittedRecordCount == 0 {
		return nil
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
	checkpointErr := db.wal.Checkpoint(db.file, targetSize)
	mapErr := db.refreshMainFileMapping()
	return errors.Join(checkpointErr, mapErr)
}

// logicalFileSize is the byte length through the last allocated page.
func (db *DB) logicalFileSize() (int64, error) {
	pageSize := db.meta.PageSize()
	lastPage := uint64(db.meta.LastPage())
	if pageSize <= 0 || lastPage >= uint64(math.MaxInt64)/uint64(pageSize) {
		return 0, fmt.Errorf("calculate database file size: %w", page.ErrInvalid)
	}
	return int64(lastPage+1) * pageSize, nil
}
