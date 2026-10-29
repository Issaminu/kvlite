package kvlite

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// dirtyNode keeps the original and final images of one changed page.
// original is nil for a new page or a replacement that needs a complete WAL image.
type dirtyNode struct {
	original *btree.Node
	final    *btree.Node
	// parent holds the branch or bucket-entry leaf that points to this node.
	// Set before relocation (due to large-page not fitting the next N nodes). Only the database root has no parent then.
	parent *btree.Node
}

// txNode keeps the current node and the parent found during this callback's traversal.
// A parent can be known before its child node is loaded.
type txNode struct {
	node   *btree.Node
	parent *btree.Node
}

// txTreeStore reads committed nodes for both transaction modes and owns private nodes only for a write transaction.
// Discarding a write store discards uncommitted node and metadata changes without restoring shared database state.
type txTreeStore struct {
	tx *Tx
	// baseDirty contains changes from earlier callbacks in the same write batch. They remain private until the full batch commits to the WAL.
	baseDirty map[page.ID]dirtyNode
	// nodes caches this callback's node images and direct parents.
	// A parent-only entry does not load its child. Read transactions keep this map nil.
	nodes map[page.ID]txNode
	// dirty contains one original and final image for each changed page.
	// A nil original requires a complete WAL image instead of an entry patch.
	dirty map[page.ID]dirtyNode
}

func (store *txTreeStore) PageSize() int64 {
	return store.tx.meta.PageSize()
}

// LookupPage uses committed storage for a read transaction.
// A write transaction keeps each node that it reads. Its decoded entries always include the key.
func (store *txTreeStore) LookupPage(pageID page.ID, key []byte, includeKey bool) (btree.Entry, bool, page.ID, error) {
	if store.tx.readOnly {
		return store.tx.db.lookupCommittedPage(pageID, key, includeKey)
	}
	node, err := store.ReadNode(pageID)
	if err != nil {
		return btree.Entry{}, false, 0, err
	}
	if node == nil {
		return btree.Entry{}, false, 0, btree.ErrKeyNotFound
	}
	entry, found, child, err := btree.LookupNode(node, key)
	if err == nil && found && entry.Flags()&btree.BucketLeafFlag != 0 {
		root, decodeErr := page.DecodeID(entry.Value())
		if decodeErr != nil {
			return btree.Entry{}, false, 0, decodeErr
		}
		store.SetParent(root, node)
	}
	return entry, found, child, err
}

func (store *txTreeStore) ReadNode(pageID page.ID) (*btree.Node, error) {
	// Returning the cached pointer makes later reads observe writes made earlier in this transaction.
	view, cached := store.nodes[pageID]
	if view.node != nil {
		return view.node, nil
	}
	// WritableNode clones this shared batch image before it changes the node. This keeps an unsuccessful callback from changing an earlier result.
	if dirty, ok := store.baseDirty[pageID]; ok {
		view.node = dirty.final
		if !cached {
			view.parent = dirty.parent
		}
		store.nodes[pageID] = view
		return dirty.final, nil
	}

	var node *btree.Node
	var err error
	if store.tx.readOnly {
		node, err = store.tx.db.readOnlyNode(pageID)
	} else {
		node, err = store.tx.db.readNode(pageID)
	}
	if err != nil {
		return nil, err
	}
	if store.nodes != nil {
		view.node = node
		store.nodes[pageID] = view
	}
	return node, nil
}

// ReplaceLeafEntry replaces an uncached main-file leaf without copying its old value.
// A page without a WAL image needs a complete first image. Commit preparation
// therefore needs its old allocation count, but not a decoded original node.
func (store *txTreeStore) ReplaceLeafEntry(pageID page.ID, entry btree.Entry, skipIdentical bool) (bool, error) {
	if entry.Flags() != 0 {
		return false, nil
	}
	db := store.tx.db
	data, err := store.uncachedMainNode(pageID)
	if err != nil || data == nil {
		return false, err
	}
	node, replaced, err := btree.ReplaceMappedLeaf(data, pageID, store.PageSize(), db.meta.DataPageChecksums(), entry, skipIdentical)
	if err != nil || !replaced {
		return false, err
	}
	if node != nil {
		store.StageNode(node)
	}
	return true, nil
}

// DeleteLeafEntry stages an empty uncached main-file leaf without copying its old value.
// Its full empty image and old allocation count are sufficient for commit or retirement.
// Cached, WAL-backed and earlier callback images use the ordinary private-copy path.
func (store *txTreeStore) DeleteLeafEntry(pageID page.ID, key []byte) (*btree.Node, error) {
	data, err := store.uncachedMainNode(pageID)
	if err != nil || data == nil {
		return nil, err
	}
	node, err := btree.DeleteMappedLeaf(data, pageID, store.PageSize(), store.tx.meta.DataPageChecksums(), key)
	if err == nil && node != nil {
		store.StageNode(node)
	}
	return node, err
}

// InsertLeafNeighbor stages a new leaf without decoding the unchanged old value.
// Only an uncached plain leaf in the mapped main file can use this path.
func (store *txTreeStore) InsertLeafNeighbor(id page.ID, entry btree.Entry) (*btree.Node, []byte, bool, error) {
	db := store.tx.db
	if store.tx.readOnly || db.memoryNodes != nil || len(db.mappedFile) == 0 || entry.Flags() != 0 {
		return nil, nil, false, nil
	}
	data, err := store.uncachedMainNode(id)
	if err != nil || data == nil {
		return nil, nil, false, err
	}
	separator, before, eligible, err := btree.MappedLeafNeighbor(data, id, store.PageSize(), db.meta.DataPageChecksums(), entry.Key())
	if err != nil || !eligible {
		return nil, nil, false, err
	}
	newID, err := store.AllocatePage()
	if err != nil {
		return nil, nil, false, err
	}
	neighbor := btree.NewLeafNode(newID)
	if err := neighbor.InsertEntry(entry); err != nil {
		return nil, nil, false, err
	}
	if !before {
		separator = neighbor.EntryAt(0).Key()
	}
	store.StageNode(neighbor)
	return neighbor, separator, before, nil
}

// uncachedMainNode selects a committed main image without a transaction or cache owner.
// Nil means that an existing owner must supply the node through the ordinary path.
func (store *txTreeStore) uncachedMainNode(id page.ID) ([]byte, error) {
	db := store.tx.db
	if store.tx.readOnly || db.memoryNodes != nil || store.nodes[id].node != nil {
		return nil, nil
	}
	if _, ok := store.baseDirty[id]; ok {
		return nil, nil
	}
	if _, ok := db.wal.Lookup(id); ok {
		return nil, nil
	}
	if _, ok := db.cachedWriteNode(id); ok {
		return nil, nil
	}
	return db.readMainNodePage(id)
}

// VisitNodeReferences visits one node without decoding a committed main page.
func (store *txTreeStore) VisitNodeReferences(pageID page.ID, visit, visitContinuation func(page.ID) error) error {
	if view := store.nodes[pageID]; view.node != nil {
		return btree.VisitNodeReferences(view.node, visit, visitContinuation)
	}
	if dirty, ok := store.baseDirty[pageID]; ok {
		return btree.VisitNodeReferences(dirty.final, visit, visitContinuation)
	}
	return store.tx.db.visitCommittedNodeReferences(pageID, visit, visitContinuation)
}

func (store *txTreeStore) AllocatePage() (page.ID, error) {
	pageID, err := store.tx.writableAllocation().allocate(store.tx.meta.LastPage())
	if err != nil {
		return 0, err
	}
	if pageID > store.tx.meta.LastPage() {
		store.tx.writableMeta().SetLastPage(pageID)
		store.tx.metaDirty = true
	}
	return pageID, nil
}

// FreePage marks every page of a removed node for release at commit.
// It drops any private node image so the WAL does not write a page that the tree no longer owns.
// The page stays allocated until commit preparation, so this transaction cannot reuse it early.
func (store *txTreeStore) FreePage(pageID page.ID) error {
	node, err := store.ReadNode(pageID)
	if err != nil {
		return err
	}
	allocation := store.tx.writableAllocation()
	count := 1
	if node != nil {
		count = node.StoragePageCount()
	}
	if err := allocation.retireSpan(pageID, count); err != nil {
		return err
	}
	delete(store.nodes, pageID)
	delete(store.dirty, pageID)
	return nil
}

// WritableNode returns the transaction-owned version of node. The first write
// copies a committed node. Later writes reuse the same dirty node.
func (store *txTreeStore) WritableNode(node *btree.Node) *btree.Node {
	if dirty, ok := store.dirty[node.PageID()]; ok {
		// A parent-link change can retain an earlier callback's image without owning it.
		if base, shared := store.baseDirty[node.PageID()]; !shared || dirty.final != base.final {
			return dirty.final
		}
	}
	return store.stageNodeClone(node, node.Clone())
}

// DeleteLeafEntries copies only retained entries on the first write to a leaf.
// Later deletes compact the transaction's private entry list in place.
func (store *txTreeStore) DeleteLeafEntries(node *btree.Node, firstIndex int, rest []int, minimumSize int64) *btree.Node {
	if dirty, ok := store.dirty[node.PageID()]; ok {
		if base, shared := store.baseDirty[node.PageID()]; !shared || dirty.final != base.final {
			dirty.final.DeleteLeafEntries(firstIndex, rest)
			return dirty.final
		}
	}
	return store.stageNodeClone(node, node.CloneWithoutLeafEntries(firstIndex, rest, minimumSize))
}

// stageNodeClone records the committed base and the new callback-owned image.
func (store *txTreeStore) stageNodeClone(node, private *btree.Node) *btree.Node {
	change, ok := store.baseDirty[node.PageID()]
	if !ok {
		change.original = node
	}
	change.final = private
	if view, cached := store.nodes[node.PageID()]; cached {
		change.parent = view.parent
	}
	store.nodes[node.PageID()] = txNode{node: private, parent: change.parent}
	store.dirty[node.PageID()] = change
	return private
}

func (store *txTreeStore) StageNode(node *btree.Node) {
	dirty := store.dirty[node.PageID()]
	dirty.final = node
	if baseDirty, ok := store.baseDirty[node.PageID()]; ok {
		dirty.original = baseDirty.original
		dirty.parent = baseDirty.parent
	}
	if view, cached := store.nodes[node.PageID()]; cached {
		dirty.parent = view.parent
	}
	// Keep the mutable node here. The commit path encodes its final state once after the callback succeeds.
	store.nodes[node.PageID()] = txNode{node: node, parent: dirty.parent}
	store.dirty[node.PageID()] = dirty
}

// SetParent records a known branch or bucket-entry owner without searching other nodes.
// A nil parent selects the metadata root. Earlier batch images stay shared until WritableNode copies them.
func (store *txTreeStore) SetParent(child page.ID, parent *btree.Node) {
	if store.tx.readOnly {
		return
	}
	view, cached := store.nodes[child]
	if cached && view.parent == parent {
		return
	}
	view.parent = parent
	if change, ok := store.dirty[child]; ok {
		change.parent = parent
		store.dirty[child] = change
	} else if change, ok := store.baseDirty[child]; ok {
		view.node = change.final
		// A new object at the same page ID is still the same parent.
		// Commit preparation resolves its latest private image.
		if change.parent != parent && (change.parent == nil || parent == nil || change.parent.PageID() != parent.PageID()) {
			change.parent = parent
			store.dirty[child] = change
		}
	}
	store.nodes[child] = view
}

// prepareCommit assigns one consecutive storage span to each changed node.
// Moved nodes and their reference changes stay private until commit.
func (tx *Tx) prepareCommit() error {
	if tx.allocation != nil {
		tx.allocation.forEachRetired(func(id page.ID) { delete(tx.store.dirty, id) })
	}
	if len(tx.store.dirty) == 0 {
		if tx.allocation != nil && len(tx.allocation.retired) != 0 {
			allocation := tx.writableAllocation()
			if allocation.finalize(tx.meta) {
				tx.metaDirty = true
			}
		}
		return nil
	}
	// Equal span lengths keep every head in place, so no parent repair is needed.
	unchangedSpans := true
	for _, change := range tx.store.dirty {
		node := change.final
		oldCount := node.StoragePageCount()
		if oldCount <= 0 || page.ID(oldCount)-1 > ^page.ID(0)-node.PageID() {
			return fmt.Errorf("node %d has an invalid old span: %w", node.PageID(), btree.ErrInvalid)
		}
		node.CacheEncodedSize()
		count := btree.NodePageCount(node, tx.meta.PageSize())
		if count <= 0 {
			return fmt.Errorf("node %d has an invalid span: %w", node.PageID(), btree.ErrInvalid)
		}
		if count != oldCount {
			unchangedSpans = false
			break
		}
	}
	if unchangedSpans {
		if tx.allocation != nil && len(tx.allocation.retired) != 0 {
			allocation := tx.writableAllocation()
			if allocation.finalize(tx.meta) {
				tx.metaDirty = true
			}
		}
		return nil
	}
	tx.store.tx = tx
	if tx.store.nodes == nil {
		tx.store.nodes = make(map[page.ID]txNode)
	}
	prepared := make(map[*btree.Node]bool, len(tx.store.dirty))
	for {
		var pending []dirtyNode
		for _, change := range tx.store.dirty {
			if !prepared[change.final] {
				pending = append(pending, change)
			}
		}
		if len(pending) == 0 {
			break
		}
		slices.SortFunc(pending, func(left, right dirtyNode) int { return cmp.Compare(left.final.PageID(), right.final.PageID()) })
		moved := make(map[page.ID]page.ID)
		for _, change := range pending {
			node := change.final
			head := node.PageID()
			oldCount := node.StoragePageCount()
			if oldCount <= 0 || page.ID(oldCount)-1 > ^page.ID(0)-head {
				return fmt.Errorf("node %d has an invalid old span: %w", head, btree.ErrInvalid)
			}
			node.CacheEncodedSize()
			count := btree.NodePageCount(node, tx.meta.PageSize())
			if count <= 0 {
				return fmt.Errorf("node %d has an invalid span: %w", head, btree.ErrInvalid)
			}
			if count > oldCount {
				allocation := tx.writableAllocation()
				if !allocation.extendSpan(head, oldCount, count, tx.meta.LastPage()) {
					newHead, err := allocation.allocateSpan(tx.meta.LastPage(), count)
					if err != nil {
						return err
					}
					if err := allocation.retireSpan(head, oldCount); err != nil {
						return err
					}
					delete(tx.store.dirty, head)
					delete(tx.store.nodes, head)
					node.SetPageID(newHead)
					change.original = nil
					tx.store.nodes[newHead] = txNode{node: node, parent: change.parent}
					tx.store.dirty[newHead] = change
					moved[head] = newHead
					head = newHead
				}
				last := head + page.ID(count) - 1
				if last > tx.meta.LastPage() {
					tx.writableMeta().SetLastPage(last)
					tx.metaDirty = true
				}
			} else if count < oldCount {
				if err := tx.writableAllocation().retireSpan(head+page.ID(count), oldCount-count); err != nil {
					return err
				}
			}
			node.SetStoragePageCount(count)
			prepared[node] = true
		}
		if len(moved) != 0 {
			if err := tx.repairMovedNodes(moved); err != nil {
				return err
			}
		}
	}
	if tx.allocation != nil && len(tx.allocation.retired) != 0 {
		allocation := tx.writableAllocation()
		if allocation.finalize(tx.meta) {
			tx.metaDirty = true
		}
	}
	return nil
}

// repairMovedNodes changes only the saved direct parent of each moved node.
// A later callback or this preparation pass can replace that parent. Use its latest private version before changing the reference.
func (tx *Tx) repairMovedNodes(moved map[page.ID]page.ID) error {
	parents := make([]*btree.Node, 0, len(moved))
	for id, target := range moved {
		parent := tx.store.dirty[target].parent
		if parent == nil {
			if id != tx.meta.Root() {
				return fmt.Errorf("moved node %d has no parent: %w", id, btree.ErrInvalid)
			}
			continue
		}
		parentID := parent.PageID()
		if replacement, ok := moved[parentID]; ok {
			parentID = replacement
		}
		if change, ok := tx.store.dirty[parentID]; ok {
			parent = change.final
		}
		parents = append(parents, parent)
	}
	// Several moved children can share one parent. Repair that parent once.
	slices.SortFunc(parents, func(left, right *btree.Node) int { return cmp.Compare(left.PageID(), right.PageID()) })
	for first := 0; first < len(parents); {
		parent := parents[first]
		last := first + 1
		for last < len(parents) && parents[last].PageID() == parent.PageID() {
			last++
		}
		parent = tx.store.WritableNode(parent)
		repaired := 0
		if !parent.IsLeaf() {
			for index, child := range parent.Children {
				if target, ok := moved[child]; ok {
					parent.Children[index] = target
					repaired++
				}
			}
		} else {
			for index := range parent.EntryCount() {
				entry := parent.EntryAt(index)
				if entry.Flags()&btree.BucketLeafFlag == 0 {
					continue
				}
				root, err := page.DecodeID(entry.Value())
				if err != nil {
					return err
				}
				if target, ok := moved[root]; ok {
					if err := parent.InsertOrReplaceLeafEntry(btree.NewEntry(entry.Flags(), entry.Key(), page.EncodeID(target))); err != nil {
						return err
					}
					repaired++
				}
			}
		}
		if repaired != last-first {
			return fmt.Errorf("parent %d repaired %d moved children, want %d: %w", parent.PageID(), repaired, last-first, btree.ErrInvalid)
		}
		first = last
	}
	rootID := tx.meta.Root()
	if target, ok := moved[rootID]; ok {
		rootID = target
		tx.writableMeta().SetRoot(target)
		tx.metaDirty = true
	}
	if change, ok := tx.store.dirty[rootID]; ok {
		tx.rootNode = change.final
	}
	if tx.rootNode == nil {
		return fmt.Errorf("relocated root %d is absent: %w", rootID, btree.ErrInvalid)
	}
	var refresh func(*Bucket)
	refresh = func(bucket *Bucket) {
		if bucket == nil || bucket.deleted {
			return
		}
		if bucket.rootNode != nil {
			id := bucket.rootNode.PageID()
			if target, ok := moved[id]; ok {
				id = target
			}
			if change, ok := tx.store.dirty[id]; ok {
				bucket.rootNode = change.final
			}
		}
		refresh(bucket.child)
		for _, child := range bucket.children {
			refresh(child)
		}
	}
	refresh(tx.bucket)
	for _, bucket := range tx.buckets {
		refresh(bucket)
	}
	return nil
}
