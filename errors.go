package kvlite

import "errors"

// errNotImplemented is a scaffold placeholder returned by stubs that are not yet
// built. Remove each usage as its component is implemented.
var errNotImplemented = errors.New("kvlite: not implemented")

// Sentinel errors, mirroring bbolt's names so bbolt's tests port cleanly.
var (
	ErrDatabaseNotOpen    = errors.New("database not open")
	ErrInvalid            = errors.New("invalid database")
	ErrVersionMismatch    = errors.New("version mismatch")
	ErrChecksum           = errors.New("checksum error")
	ErrTxNotWritable      = errors.New("tx not writable")
	ErrTxClosed           = errors.New("tx closed")
	ErrDatabaseReadOnly   = errors.New("database is in read-only mode")
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrBucketExists       = errors.New("bucket already exists")
	ErrBucketNameRequired = errors.New("bucket name required")
	ErrKeyRequired        = errors.New("key required")
	ErrKeyTooLarge        = errors.New("key too large")
	ErrValueTooLarge      = errors.New("value too large")
	ErrIncompatibleValue  = errors.New("incompatible value")
)
