package kvlite

import (
	"io"

	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/wal"
)

func (db *DB) applyWALRecord(record *wal.Record) error {
	pageSize := db.meta.PageSize()
	offset := int64(record.Header.PageID) * pageSize
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	// WAL records omit page padding. The main file stores every page at its full size.
	data := record.PageContent
	if int64(len(data)) < pageSize {
		padded := make([]byte, pageSize)
		copy(padded, data)
		data = padded
	}
	return fileio.WriteFull(db.file, data)
}

// checkpointWAL makes the WAL durable, copies its committed pages into the main
// file, makes the main file durable, and then clears the WAL state.
func (db *DB) checkpointWAL() error {
	return db.wal.Checkpoint(db.applyWALRecord, db.file.Sync)
}
