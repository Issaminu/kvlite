package kvlite

import (
	"errors"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

var (
	ErrPathRequired         = errors.New("path required")                       // ErrPathRequired means Open or Backup received an empty path.
	ErrDatabaseNotOpen      = errors.New("database not open")                   // ErrDatabaseNotOpen means the DB is closed. Data operations and Checkpoint return it after Close.
	ErrDatabaseLocked       = errors.New("database is locked")                  // ErrDatabaseLocked means Open did not acquire a conflicting database-file lock before the configured positive timeout expired.
	ErrInvalid              = page.ErrInvalid                                   // ErrInvalid means that stored data is damaged or not valid, or that an argument such as an Options field is not valid.
	ErrVersionMismatch      = page.ErrVersionMismatch                           // ErrVersionMismatch means the database file or its write-ahead log uses a format version that this KVLite version cannot read.
	ErrChecksum             = page.ErrChecksum                                  // ErrChecksum means stored bytes do not match their checksum. The data is damaged.
	ErrTxNotWritable        = errors.New("tx not writable")                     // ErrTxNotWritable means a write was called in a read-only transaction, such as a DB.View callback.
	ErrTxClosed             = errors.New("tx closed")                           // ErrTxClosed means a Tx, Bucket, or Cursor was used after its transaction callback returned.
	ErrTxFailed             = errors.New("tx failed")                           // ErrTxFailed means a write in the transaction failed after it changed the transaction. Every later operation in that transaction returns it, and [DB.Update] discards the transaction even when its callback returns nil. The error also wraps the first failure.
	ErrDatabaseReadOnly     = errors.New("database is in read-only mode")       // ErrDatabaseReadOnly means a write or a change of settings was called on a DB that was opened with Options.ReadOnly.
	ErrBucketNotFound       = errors.New("bucket not found")                    // ErrBucketNotFound means a bucket is absent or its handle was removed in the current transaction.
	ErrBucketExists         = errors.New("bucket already exists")               // ErrBucketExists means CreateBucket received the name of a bucket that already exists.
	ErrBucketNameRequired   = errors.New("bucket name required")                // ErrBucketNameRequired means a bucket operation received an empty bucket name.
	ErrKeyNotFound          = btree.ErrKeyNotFound                              // ErrKeyNotFound means Get found no value for the key.
	ErrKeyRequired          = btree.ErrKeyRequired                              // ErrKeyRequired means an operation received an empty key.
	ErrKeyTooLarge          = btree.ErrKeyTooLarge                              // ErrKeyTooLarge means a key is longer than MaxKeySize.
	ErrValueTooLarge        = btree.ErrValueTooLarge                            // ErrValueTooLarge means a value is longer than MaxValueSize.
	ErrIncompatibleValue    = btree.ErrIncompatibleValue                        // ErrIncompatibleValue means a name belongs to the other kind of entry: a value operation found a nested bucket, or a bucket operation found a plain value.
	ErrCursorInvalidated    = errors.New("cursor invalidated by bucket change") // ErrCursorInvalidated is returned when a bucket changes while one of its cursors is in use. Create a new cursor before traversal continues.
	ErrScanCallbackRequired = errors.New("scan callback required")              // ErrScanCallbackRequired is returned when [Bucket.ScanPrefix] or [Bucket.ScanRange] receives a nil callback.
	ErrDatabaseWriteFailed  = errors.New("database write failed")               // ErrDatabaseWriteFailed means a main-file write, mapping refresh, or explicit main-file/directory synchronization failed. Later transactions, reads, writes, backups, and Sync return this error. Stats, Path, and Close remain available. Close and reopen before using the data again. A failed direct write in SyncNone with Options.DisableWAL enabled can corrupt the file and has no WAL to repair it.
	ErrWALWriteFailed       = wal.ErrWriteFailed                                // ErrWALWriteFailed means a write or sync of the write-ahead log failed during a commit or a checkpoint. That commit, or the commits that the last sync did not make durable, can be saved or lost. The DB then returns this error from every later transaction, read, write, and backup. [DB.Stats], [DB.Path], and [DB.Close] still work. Close the DB, open it again, and check the data.
)
