package kvlite

import (
	"os"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// OpenMemory creates an empty database that stores all data in memory.
// It does no file operations and creates no write-ahead log.
// Each call creates an independent, writable database.
//
// Use the same buckets, transactions, and ordered reads as a database opened by [Open].
// A write callback runs alone. A callback error or panic discards its changes.
// Keys and values use [MaxKeySize] and [MaxValueSize].
// Large entries use the same logical page counts as file storage.
//
// The caller must call [DB.Close] when the database is no longer needed.
// Close discards all data. Data is also lost when the process ends.
// [DB.Path] returns an empty string. File sync options do not apply.
// [DB.Backup] cannot save a memory database.
func OpenMemory() *DB {
	meta := page.NewMeta(int64(os.Getpagesize()))
	root := btree.NewLeafNode(meta.Root())
	options := defaultOptions(meta.PageSize())
	return &DB{
		storage: storage{
			meta:        meta,
			rootNode:    root,
			memoryNodes: map[page.ID]*btree.Node{root.PageID(): root},
			options:     &options,
			allocation:  newAllocationBitmap(meta.PageSize()),
		},
	}
}
