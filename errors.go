package kvlite

import (
	"errors"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// errNotImplemented is a scaffold placeholder returned by stubs that are not yet
// built. Remove each usage as its component is implemented.
var errNotImplemented = errors.New("kvlite: not implemented")

var (
	ErrDatabaseNotOpen      = errors.New("database not open")
	ErrInvalid              = page.ErrInvalid
	ErrVersionNotSupported  = page.ErrVersionNotSupported
	ErrChecksum             = page.ErrChecksum
	ErrTxNotWritable        = errors.New("tx not writable")
	ErrTxClosed             = errors.New("tx closed")
	ErrDatabaseReadOnly     = errors.New("database is in read-only mode")
	ErrBucketNotFound       = errors.New("bucket not found")
	ErrBucketExists         = errors.New("bucket already exists")
	ErrBucketNameRequired   = errors.New("bucket name required")
	ErrKeyNotFound          = errors.New("key not found")
	ErrKeyRequired          = errors.New("key required")
	ErrKeyTooLarge          = errors.New("key too large")
	ErrKeyEmpty             = errors.New("key is empty")
	ErrNotBranchNode        = btree.ErrNotBranchNode
	ErrNotLeafNode          = btree.ErrNotLeafNode
	ErrValueTooLarge        = errors.New("value too large")
	ErrValueEmpty           = errors.New("value is empty")
	ErrIncompatibleValue    = btree.ErrIncompatibleValue
	ErrNodeNotSaturated     = btree.ErrNodeNotSaturated
	ErrNodeTooLarge         = btree.ErrNodeTooLarge
	ErrEntryTooLargeForPage = errors.New("key/value entry is too large to fit in a single page on its own")
)
