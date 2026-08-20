package kvlite

import (
	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// txTreeStore reads committed nodes for both transaction modes and owns private nodes only for a write transaction.
// Discarding a write store discards uncommitted node and metadata changes without restoring shared database state.
type txTreeStore struct {
	tx *Tx
	// baseNodes contains page images from earlier callbacks in the same write batch. They remain private until the full batch commits to the WAL.
	baseNodes map[page.ID]*btree.Node
	// nodes is a write transaction's complete page view. It contains committed
	// nodes read by the transaction and private nodes staged by the transaction.
	// It stays nil for a read transaction.
	nodes map[page.ID]*btree.Node
	// dirty is the changed subset of nodes whose final images walRecords must encode.
	// StageNode stores the same private node in both maps.
	dirty map[page.ID]*btree.Node
}

func (store *txTreeStore) PageSize() int64 {
	return store.tx.meta.PageSize()
}

func (store *txTreeStore) ReadNode(pageID page.ID) (*btree.Node, error) {
	// Returning the cached pointer makes later reads observe writes made earlier in this transaction.
	if node, ok := store.nodes[pageID]; ok {
		return node, nil
	}
	// WritableNode clones this shared batch image before it changes the node. This keeps an unsuccessful callback from changing an earlier result.
	if node, ok := store.baseNodes[pageID]; ok {
		store.nodes[pageID] = node
		return node, nil
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
		return dirty
	}
	private := node.Clone()
	store.StageNode(private)
	return private
}

func (store *txTreeStore) StageNode(node *btree.Node) {
	// Keep the mutable node here. The commit path encodes its final state once after the callback succeeds.
	store.nodes[node.PageID()] = node
	store.dirty[node.PageID()] = node
}
