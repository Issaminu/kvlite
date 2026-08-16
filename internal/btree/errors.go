package btree

import (
	"errors"

	"github.com/Issaminu/kvlite/internal/page"
)

var (
	ErrInvalid           = page.ErrInvalid
	ErrNotBranchNode     = errors.New("node is a leaf node, which is not allowed here")
	ErrNotLeafNode       = errors.New("node is a branch node, which is not allowed here")
	ErrIncompatibleValue = errors.New("incompatible value")
	ErrNodeNotSaturated  = errors.New("node entries size hasn't reached a large enough size to split")
	ErrNodeTooLarge      = errors.New("node is too large to fit page size")
)

const BucketLeafFlag uint32 = 0x01
