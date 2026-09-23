package kvlite

import (
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

	node, err := store.tx.db.readNode(pageID)
	if err != nil {
		return nil, err
	}
	if store.nodes != nil {
		store.nodes[pageID] = node
	}
	return node, nil
}

func (store *txTreeStore) AllocatePage() page.ID {
	// The allocation changes the metadata copy even if the new page later becomes the tree root.
	store.tx.metaDirty = true
	return store.tx.meta.Allocate()
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
