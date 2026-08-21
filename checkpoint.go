package kvlite

import (
	"io"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/wal"
)

// applyWALRecord writes one WAL page image to the main file without changing the file's shared offset.
// Startup recovery calls this for committed records and synchronizes the main file after all records are applied.
func (db *DB) applyWALRecord(record *wal.Record) error {
	pageBuffer := make([]byte, db.meta.PageSize())
	return db.applyWALRecordWithBuffer(record, pageBuffer)
}

func (db *DB) applyWALRecordWithBuffer(record *wal.Record, pageBuffer []byte) error {
	pageSize := db.meta.PageSize()
	// WAL records omit page padding. The main file stores every page at its full size.
	clear(pageBuffer)
	copy(pageBuffer, record.PageContent)
	if record.Header.Type == wal.RecordTypeData {
		if err := btree.VerifyNodeIDAndSetChecksum(pageBuffer, record.Header.PageID); err != nil {
			return err
		}
	}
	offset := int64(record.Header.PageID) * pageSize
	return fileio.WriteFull(io.NewOffsetWriter(db.file, offset), pageBuffer)
}

// checkpointWAL makes the WAL durable, copies its committed pages into the main file, and then makes the main file durable.
// The WAL removes its committed state only after those steps succeed, so a failed write or main-file sync can be retried.
func (db *DB) checkpointWAL() error {
	return db.wal.Checkpoint(db.file)
}
