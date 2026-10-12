package kvlite

import (
	"fmt"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// dirtyNode keeps the original committed image and the final private image of one changed page. original is nil for a new page.
type dirtyNode struct {
	original *btree.Node
	final    *btree.Node
}

// txTreeStore reads committed nodes for both transaction modes and owns private nodes only for a write transaction.
// Discarding a write store discards uncommitted node and metadata changes without restoring shared database state.
type txTreeStore struct {
	tx *Tx
	// baseDirty contains changes from earlier callbacks in the same write batch. They remain private until the full batch commits to the WAL.
	baseDirty map[page.ID]dirtyNode
	// nodes is a write transaction's complete page view. It contains committed
	// nodes read by the transaction and private nodes staged by the transaction.
	// It stays nil for a read transaction.
	nodes map[page.ID]*btree.Node
	// dirty contains one original and final image for each changed page. A nil original means that the transaction created the page.
	dirty map[page.ID]dirtyNode
}

func (store *txTreeStore) PageSize() int64 {
	return store.tx.meta.PageSize()
}

// LookupPage uses encoded pages for a read transaction.
// A write transaction decodes and keeps each page that it reads.
func (store *txTreeStore) LookupPage(pageID page.ID, key []byte) (btree.Entry, bool, page.ID, error) {
	if store.tx.readOnly {
		return store.tx.db.lookupCommittedPage(pageID, key)
	}
	node, err := store.ReadNode(pageID)
	if err != nil {
		return btree.Entry{}, false, 0, err
	}
	if node == nil {
		return btree.Entry{}, false, 0, btree.ErrKeyNotFound
	}
	return btree.LookupNode(node, key)
}

func (store *txTreeStore) ReadNode(pageID page.ID) (*btree.Node, error) {
	// Returning the cached pointer makes later reads observe writes made earlier in this transaction.
	if node, ok := store.nodes[pageID]; ok {
		return node, nil
	}
	// WritableNode clones this shared batch image before it changes the node. This keeps an unsuccessful callback from changing an earlier result.
	if dirty, ok := store.baseDirty[pageID]; ok {
		store.nodes[pageID] = dirty.final
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
		store.nodes[pageID] = node
	}
	return node, nil
}

// VisitNodeReferences visits one node without decoding a committed main page.
func (store *txTreeStore) VisitNodeReferences(pageID page.ID, visit, visitContinuation func(page.ID) error) error {
	if node, ok := store.nodes[pageID]; ok {
		return btree.VisitNodeReferences(node, visit, visitContinuation)
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
		store.tx.meta.SetLastPage(pageID)
		store.tx.metaDirty = true
	}
	return pageID, nil
}

// FreePage marks a removed tree node and all of its continuation pages for release at commit.
// It drops any private node image so the WAL does not write a page that the tree no longer owns.
// The page stays allocated until commit preparation, so this transaction cannot reuse it early.
func (store *txTreeStore) FreePage(pageID page.ID) error {
	node, err := store.ReadNode(pageID)
	if err != nil {
		return err
	}
	allocation := store.tx.writableAllocation()
	if node != nil {
		for _, id := range node.OverflowPages() {
			if err := allocation.retire(id); err != nil {
				return err
			}
		}
	}
	if err := allocation.retire(pageID); err != nil {
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
		return dirty.final
	}
	original := node
	if batchDirty, ok := store.baseDirty[node.PageID()]; ok {
		original = batchDirty.original
	}
	private := node.Clone()
	store.nodes[node.PageID()] = private
	store.dirty[node.PageID()] = dirtyNode{original: original, final: private}
	return private
}

func (store *txTreeStore) StageNode(node *btree.Node) {
	dirty := store.dirty[node.PageID()]
	dirty.final = node
	if baseDirty, ok := store.baseDirty[node.PageID()]; ok {
		dirty.original = baseDirty.original
	}
	// Keep the mutable node here. The commit path encodes its final state once after the callback succeeds.
	store.nodes[node.PageID()] = node
	store.dirty[node.PageID()] = dirty
}

// prepareCommit assigns continuation pages after all tree changes finish.
// It serves direct writes and write batches. The first leaf or branch page
// keeps its ID. Each changed node keeps as many of its committed continuation
// IDs as its final body needs. Growth allocates additional pages through the
// normal allocator. Shrink retires only the surplus IDs. A body that fits one
// page loses all continuation IDs. IDs need not be consecutive.
//
// Page IDs can stay the same because the WAL writes new page contents into
// separate buffers. It publishes them only after the commit succeeds. This
// function changes only private node and allocation state. The caller must
// discard that state if preparation or the WAL commit fails.
func (tx *Tx) prepareCommit() error {
	if tx.allocation != nil {
		// A later batch callback can remove a node changed by an earlier callback.
		for id := range tx.allocation.retired {
			delete(tx.store.dirty, id)
		}
	}
	for _, change := range tx.store.dirty {
		count := btree.NodePageCount(change.final, tx.meta.PageSize())
		if count == 0 {
			return fmt.Errorf("node %d cannot hold a continuation ID: %w", change.final.PageID(), btree.ErrInvalid)
		}
		var previous []page.ID
		if change.original != nil {
			previous = change.original.OverflowPages()
		}
		required := count - 1
		var ids []page.ID
		if required > 0 {
			ids = make([]page.ID, required)
			copy(ids, previous)
		}
		if required != len(previous) {
			allocation := tx.writableAllocation()
			// Retired IDs stay allocated until every node's new IDs are assigned.
			for _, id := range previous[min(required, len(previous)):] {
				if err := allocation.retire(id); err != nil {
					return err
				}
			}
			for i := len(previous); i < required; i++ {
				id, err := allocation.allocate(tx.meta.LastPage())
				if err != nil {
					return err
				}
				ids[i] = id
				if id > tx.meta.LastPage() {
					tx.meta.SetLastPage(id)
					tx.metaDirty = true
				}
			}
		}
		change.final.SetOverflowPages(ids)
	}
	if tx.allocation != nil && len(tx.allocation.retired) != 0 {
		if tx.writableAllocation().finalize(tx.meta) {
			tx.metaDirty = true
		}
	}
	return nil
}
