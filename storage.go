package kvlite

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

type writeNodeCacheEntry struct {
	node *btree.MutableNode // Nil means the decoded image was invalidated.
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
	memoryNodes       map[page.ID]*btree.MutableNode   // A non-nil map owns all committed nodes of a memory database. It never evicts a live node.
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

// readMainPage returns one complete main-file page.
// Mapped bytes stay valid while the caller holds the database operation lock.
func (db *storage) readMainPage(pageID page.ID) ([]byte, error) {
	pageSize := db.meta.PageSize()
	if pageSize <= 0 || uint64(pageID) > uint64(math.MaxInt64)/uint64(pageSize) {
		return nil, fmt.Errorf("read page %d offset: %w", pageID, page.ErrInvalid)
	}
	offset := int64(pageID) * pageSize
	if data, ok := db.mappedBytes(offset, pageSize); ok {
		return data, nil
	}
	data := make([]byte, int(pageSize))
	if err := db.readFileAt(data, offset); err != nil {
		return nil, err
	}
	return data, nil
}

// mappedBytes returns size bytes of the main-file mapping at offset.
// It returns false when the mapping does not hold all of those bytes.
func (db *storage) mappedBytes(offset, size int64) ([]byte, bool) {
	if offset > int64(len(db.mappedFile)) || size > int64(len(db.mappedFile))-offset {
		return nil, false
	}
	return db.mappedFile[offset : offset+size : offset+size], true
}

// readFileAt fills data from the main file at offset.
// A read that ends before data is full returns [io.ErrUnexpectedEOF].
func (db *storage) readFileAt(data []byte, offset int64) error {
	n, err := db.file.ReadAt(data, offset)
	if err != nil {
		if errors.Is(err, io.EOF) && n > 0 {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if n != len(data) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// readMainNodePage returns the complete consecutive span of one node.
// Mapped spans borrow the main-file mapping. A large unmapped node needs a head read, then one full-span read.
func (db *storage) readMainNodePage(pageID page.ID) ([]byte, error) {
	data, err := db.readMainPage(pageID)
	if err != nil {
		return nil, err
	}
	count, err := btree.NodeSpan(data, pageID, db.meta.PageSize())
	if err != nil {
		return nil, fmt.Errorf("read node %d: %w", pageID, err)
	}
	if pageID > db.meta.LastPage() || uint64(count) > uint64(db.meta.LastPage())+1-uint64(pageID) {
		return nil, ErrInvalid
	}
	if !db.allocation.ownsNodeSpan(pageID, count) {
		return nil, fmt.Errorf("node %d has free or reserved storage: %w", pageID, ErrInvalid)
	}
	if count > 1 {
		size := int64(count) * db.meta.PageSize()
		offset := int64(pageID) * db.meta.PageSize()
		if mapped, ok := db.mappedBytes(offset, size); ok {
			data = mapped
		} else {
			data = make([]byte, int(size))
			if err := db.readFileAt(data, offset); err != nil {
				return nil, err
			}
			// Check the returned header too. This read can return different bytes from the head read.
			actualCount, err := btree.NodeSpan(data, pageID, db.meta.PageSize())
			if err != nil {
				return nil, fmt.Errorf("read node %d: %w", pageID, err)
			}
			if actualCount != count {
				return nil, fmt.Errorf("node %d span changed during read: %w", pageID, ErrInvalid)
			}
		}
	}
	if err := btree.CheckNodeChecksumMode(data, db.meta.DataPageChecksums()); err != nil {
		return nil, fmt.Errorf("read data node %d: %w", pageID, err)
	}
	return data, nil
}

// nodeReader selects committed storage once for an operation's pinned state.
// The caller must keep the operation lock for the reader's entire lifetime.
func (db *storage) nodeReader() btree.NodeReader {
	if db.memoryNodes != nil {
		return memoryNodeReader{nodes: db.memoryNodes}
	}
	if db.wal.Stats().CommittedRecordCount != 0 {
		return walNodeReader{store: db}
	}
	return fileNodeReader{store: db}
}

// checkNode reads a committed node without the decoded cache and checks every entry and child reference.
// Open uses it to reject a damaged catalog root before the first transaction.
// A main-file page or a recovered WAL image also gets its stored bytes and checksum checked.
// A memory node or a WAL node from this process has no stored bytes, so only the decoded node is checked.
func (db *storage) checkNode(id page.ID) (btree.Node, error) {
	node, err := db.nodeReader().ReadNode(id)
	if err != nil {
		return btree.Node{}, err
	}
	if err := node.VisitReferences(db.meta.PageSize(), func(page.ID) error { return nil }, nil); err != nil {
		return btree.Node{}, err
	}
	return node, nil
}

// cachedWriteNode returns an immutable committed node. It marks the node as recently used so eviction skips it once.
// The caller must hold the database operation write lock, or its read lock with writeNodesMu locked for writing. Open can call this before other goroutines can use the handle.
func (db *storage) cachedWriteNode(pageID page.ID) (*btree.MutableNode, bool) {
	entry, ok := db.writeNodes[pageID]
	if !ok || entry.node == nil {
		return nil, false
	}
	entry.referenced.Store(true)
	return entry.node, true
}

// invalidateWriteNode discards the decoded image and keeps its existing clock slot.
// The caller must hold the database operation write lock.
func (db *storage) invalidateWriteNode(id page.ID) {
	if cached := db.writeNodes[id]; cached != nil {
		cached.node = nil
		cached.referenced.Store(false)
	}
}

// cacheWriteNode keeps an immutable committed node for later write transactions. It keeps at most [maxCachedWriteNodePages] nodes.
// The caller must hold the database operation write lock, or its read lock with writeNodesMu locked for writing. Open can call this before other goroutines can use the handle.
func (db *storage) cacheWriteNode(node *btree.MutableNode) {
	pageID := node.PageID()
	if entry, ok := db.writeNodes[pageID]; ok {
		entry.node = node
		entry.referenced.Store(true)
		return
	}

	if len(db.writeNodeSlots) < maxCachedWriteNodePages {
		db.writeNodeSlots = append(db.writeNodeSlots, pageID)
	} else {
		// Give recently used nodes one more pass. Remove the first node that no read used after the last pass.
		for {
			victimPageID := db.writeNodeSlots[db.nextWriteNodeSlot]
			victim := db.writeNodes[victimPageID]
			if !victim.referenced.Load() {
				delete(db.writeNodes, victimPageID)
				db.writeNodeSlots[db.nextWriteNodeSlot] = pageID
				db.nextWriteNodeSlot = (db.nextWriteNodeSlot + 1) % maxCachedWriteNodePages
				break
			}
			victim.referenced.Store(false)
			db.nextWriteNodeSlot = (db.nextWriteNodeSlot + 1) % maxCachedWriteNodePages
		}
	}
	entry := &writeNodeCacheEntry{node: node}
	entry.referenced.Store(true)
	db.writeNodes[pageID] = entry
}

// findCommittedEntry searches one committed tree for Get without cursor state.
// It omits encoded keys because Get needs only the value and flags.
// The value borrows the node bytes from the reader. They stay valid while the caller holds the operation lock.
func (db *storage) findCommittedEntry(id page.ID, key []byte) (btree.Entry, bool, error) {
	return btree.FindEntry(db.nodeReader(), id, key, false)
}

// nodeIsMapped reports whether checked node bytes alias the main-file mapping.
func (db *storage) nodeIsMapped(data []byte, id page.ID) bool {
	mapped, ok := db.mappedBytes(int64(id)*db.meta.PageSize(), int64(len(data)))
	return ok && &data[0] == &mapped[0]
}

// writeFailure returns the WAL or main-file error that stopped this handle, or nil.
// After that failure, data operations must stop because the saved state is unknown.
// The caller must hold operationMu.
func (db *storage) writeFailure() error {
	if db.mainWriteFailure != nil {
		return db.mainWriteFailure
	}
	if db.wal == nil {
		return nil
	}
	return db.wal.AppendFailure()
}

// These readers keep the exported tree method off DB's public interface.
// Each implements one actual storage source; only the WAL reader has a fallback.
type memoryNodeReader struct {
	nodes map[page.ID]*btree.MutableNode
}

func (reader memoryNodeReader) ReadNode(id page.ID) (btree.Node, error) {
	node := reader.nodes[id]
	if node == nil {
		return btree.Node{}, fmt.Errorf("read memory page %d: %w", id, page.ErrInvalid)
	}
	return node.ReadOnly(), nil
}

type fileNodeReader struct{ store *storage }

func (reader fileNodeReader) ReadNode(id page.ID) (btree.Node, error) {
	data, err := reader.store.readMainNodePage(id)
	if err != nil {
		return btree.Node{}, err
	}
	return btree.ReadCheckedNodeSpan(data, reader.store.meta.DataPageChecksums())
}

type walNodeReader struct{ store *storage }

func (reader walNodeReader) ReadNode(id page.ID) (btree.Node, error) {
	if record, ok := reader.store.wal.Lookup(id); ok {
		return record.ReadNode(reader.store.meta.PageSize())
	}
	return fileNodeReader{store: reader.store}.ReadNode(id)
}

// cloneNode returns a private copy of one committed node for editing.
// It also returns the original that a WAL patch compares against.
// original is nil for a file page without a WAL image, because persistence then writes a complete image.
// A private copy of an encoded page borrows its unchanged bytes until commit makes the transaction own them.
func (db *storage) cloneNode(id page.ID) (original, private *btree.MutableNode, err error) {
	original, logged := db.decodedOriginal(id)
	keepOriginal := db.memoryNodes != nil || logged
	if original == nil {
		view, err := db.nodeReader().ReadNode(id)
		if err != nil {
			return nil, nil, err
		}
		decoded, err := view.Clone(db.meta.PageSize())
		if err != nil {
			return nil, nil, err
		}
		if !keepOriginal {
			// No WAL patch needs the old node, so the decoded copy is the private copy.
			return nil, decoded, nil
		}
		original = decoded
	}
	private = original.Clone()
	if !keepOriginal {
		original = nil
	}
	return original, private, nil
}

// decodedOriginal returns a decoded committed node that storage already owns, or nil.
// It checks the memory nodes, the write-node cache, and the nodes of WAL commits from this process.
// logged reports whether the WAL holds an image of the page.
// The caller must not change the returned node.
func (db *storage) decodedOriginal(id page.ID) (node *btree.MutableNode, logged bool) {
	if db.memoryNodes != nil {
		return db.memoryNodes[id], false
	}
	record, logged := db.wal.Lookup(id)
	db.writeNodesMu.RLock()
	if cached := db.writeNodes[id]; cached != nil && cached.node != nil {
		node = cached.node
		// A load first keeps hits from writing a shared cache line again and again.
		if !cached.referenced.Load() {
			cached.referenced.Store(true)
		}
	}
	db.writeNodesMu.RUnlock()
	if node == nil {
		node = record.Node
	}
	return node, logged
}

// persist writes a prepared transaction through the configured storage backend.
// allocation is nil when the transaction did not change page ownership.
// The caller publishes its private metadata and nodes only after this succeeds.
func (db *storage) persist(meta *page.Meta, nodes []wal.NodeRecord, allocationPages []wal.AllocationRecord, encodedMeta []byte, allocation *allocationChanges) (bool, error) {
	if db.options.DisableWAL {
		return false, db.writeDirectPages(meta, nodes, allocationPages, encodedMeta, meta.DataPageChecksums())
	}
	var retired map[page.ID]struct{}
	if allocation != nil {
		retired = allocation.walRetiredPages()
	}
	return db.wal.Commit(nodes, allocationPages, encodedMeta, retired)
}
