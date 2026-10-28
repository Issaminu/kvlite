package kvlite

import (
	"errors"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

var (
	ErrPathRequired         = errors.New("path required") // ErrPathRequired means Open or Backup received an empty path.
	ErrDatabaseNotOpen      = errors.New("database not open")
	ErrDatabaseLocked       = errors.New("database is locked") // ErrDatabaseLocked means Open did not acquire a conflicting database-file lock before the configured positive timeout expired.
	ErrInvalid              = page.ErrInvalid
	ErrVersionMismatch      = page.ErrVersionMismatch
	ErrChecksum             = page.ErrChecksum
	ErrTxNotWritable        = errors.New("tx not writable")
	ErrTxClosed             = errors.New("tx closed")
	ErrDatabaseReadOnly     = errors.New("database is in read-only mode")
	ErrBucketNotFound       = errors.New("bucket not found") // ErrBucketNotFound means a bucket is absent or its handle was removed in the current transaction.
	ErrBucketExists         = errors.New("bucket already exists")
	ErrBucketNameRequired   = errors.New("bucket name required")
	ErrKeyNotFound          = btree.ErrKeyNotFound
	ErrKeyRequired          = btree.ErrKeyRequired
	ErrKeyTooLarge          = btree.ErrKeyTooLarge
	ErrValueTooLarge        = btree.ErrValueTooLarge
	ErrIncompatibleValue    = btree.ErrIncompatibleValue
	ErrCursorInvalidated    = errors.New("cursor invalidated by bucket change") // ErrCursorInvalidated is returned when a bucket changes while one of its cursors is in use. Create a new cursor before traversal continues.
	ErrScanCallbackRequired = errors.New("scan callback required")              // ErrScanCallbackRequired is returned when [Bucket.ScanPrefix] or [Bucket.ScanRange] receives a nil callback.
	ErrDatabaseWriteFailed  = errors.New("database write failed")               // ErrDatabaseWriteFailed means a main-file write, mapping refresh, or explicit main-file/directory synchronization failed. Later transactions, reads, writes, backups, and Sync return this error. Stats, Path, and Close remain available. Close and reopen before using the data again. A failed direct write in SyncNone with Options.DisableWAL enabled can corrupt the file and has no WAL to repair it.
	ErrWALWriteFailed       = wal.ErrWriteFailed                                // ErrWALWriteFailed means a write or sync of the write-ahead log failed during a commit or a checkpoint. That commit, or the commits that the last sync did not make durable, can be saved or lost. The DB then returns this error from every later transaction, read, write, and backup. [DB.Stats], [DB.Path], and [DB.Close] still work. Close the DB, open it again, and check the data.
)
