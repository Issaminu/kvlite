package kvlite

import (
	"sync"
	"sync/atomic"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

type writeNodeCacheEntry struct {
	node *btree.Node // Nil means the decoded image was invalidated.
	// referenced is the CLOCK bit. Read-only lookups set it while they hold only the read lock of writeNodesMu, so it is atomic.
	referenced atomic.Bool
}

// storage owns committed node images and their physical storage.
// Database operations pin this state with operationMu: writers, checkpoints,
// and remapping wait until all read operations and View callbacks finish.
type storage struct {
	activeBackups int           // Protected by operationMu. A backup keeps the main file unchanged.
	backupsDone   chan struct{} // Closed under operationMu when the last active backup finishes.

	fs                fileio.FS // the file system of the database files; nil for a memory database
	file              fileio.File
	meta              *page.Meta
	rootNode          *btree.Node
	memoryNodes       map[page.ID]*btree.Node          // A non-nil map owns all committed nodes of a memory database. It never evicts a live node.
	writeNodes        map[page.ID]*writeNodeCacheEntry // Each committed node refers only to stable heap bytes that later write clones can share.
	writeNodesMu      sync.RWMutex                     // Protects cache access by concurrent readers. Writers already hold operationMu exclusively.
	writeNodeSlots    []page.ID
	nextWriteNodeSlot int
	mappedFile        []byte
	mappedBacking     []byte // full writable mapping; mappedFile exposes only the real file length
	mappedWritable    bool
	directWriter      directMappedFile
	mainWriteFailure  error // A failed main-file write or explicit sync stops later data operations.
	options           *Options
	wal               *wal.WAL

	stats      Stats
	allocation *allocationBitmap
}
