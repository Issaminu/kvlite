package kvlite

import (
	"errors"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

var (
	ErrDatabaseNotOpen      = errors.New("database not open")
	ErrDatabaseLocked       = errors.New("database is locked") // ErrDatabaseLocked means Open did not acquire a conflicting database-file lock before the configured positive timeout expired.
	ErrInvalid              = page.ErrInvalid
	ErrVersionMismatch      = page.ErrVersionMismatch
	ErrChecksum             = page.ErrChecksum
	ErrTxNotWritable        = errors.New("tx not writable")
	ErrTxClosed             = errors.New("tx closed")
	ErrDatabaseReadOnly     = errors.New("database is in read-only mode")
	ErrBucketNotFound       = errors.New("bucket not found")
	ErrBucketExists         = errors.New("bucket already exists")
	ErrBucketNameRequired   = errors.New("bucket name required")
	ErrKeyNotFound          = btree.ErrKeyNotFound
	ErrKeyRequired          = btree.ErrKeyRequired
	ErrKeyTooLarge          = btree.ErrKeyTooLarge
	ErrValueTooLarge        = btree.ErrValueTooLarge
	ErrIncompatibleValue    = btree.ErrIncompatibleValue
	ErrEntryTooLargeForPage = btree.ErrEntryTooLarge
)
