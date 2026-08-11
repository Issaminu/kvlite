package kvlite

import "errors"

// errNotImplemented is a scaffold placeholder returned by stubs that are not yet
// built. Remove each usage as its component is implemented.
var errNotImplemented = errors.New("kvlite: not implemented")

var (
	ErrDatabaseNotOpen     = errors.New("database not open")
	ErrInvalid             = errors.New("invalid database")
	ErrVersionNotSupported = errors.New("database version is not supported by current kvlite version")
	ErrChecksum            = errors.New("checksum error")
	ErrTxNotWritable       = errors.New("tx not writable")
	ErrTxClosed            = errors.New("tx closed")
	ErrDatabaseReadOnly    = errors.New("database is in read-only mode")
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketExists        = errors.New("bucket already exists")
	ErrBucketNameRequired  = errors.New("bucket name required")
	ErrKeyNotFound         = errors.New("key not found")
	ErrKeyRequired         = errors.New("key required")
	ErrKeyTooLarge         = errors.New("key too large")
	ErrKeyEmpty            = errors.New("key is empty")
	ErrNotBranchNode       = errors.New("node is a leaf node, which is not allowed here")
	ErrNotLeafNode         = errors.New("node is a branch node, which is not allowed here")
	ErrValueTooLarge       = errors.New("value too large")
	ErrValueEmpty          = errors.New("value is empty")
	ErrIncompatibleValue   = errors.New("incompatible value")
	ErrNodeNotSaturated    = errors.New("node entries size hasn't reached a large enough size to split")
)
