package kvlite

import (
	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// txTreeStore owns the decoded and changed pages for one transaction.
// Discarding the store discards uncommitted node and metadata changes without restoring shared database state.
type txTreeStore struct {
	tx *Tx
	// nodes caches each node read, created or modified in this transaction.
	// ReadNode checks this map first, so later reads see earlier transaction changes.
	nodes map[page.ID]*btree.Node
	// dirty contains the changed subset of nodes that walRecords must encode at commit.
	// StageNode stores the same pointer in both maps, so both maps show the final node state.
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

	node, err := store.tx.db.readNode(pageID)
	if err != nil {
		return nil, err
	}
	store.nodes[pageID] = node
	return node, nil
}

func (store *txTreeStore) AllocatePage() page.ID {
	// The allocation changes the metadata copy even if the new page later becomes the tree root.
	store.tx.metaDirty = true
	return store.tx.meta.Allocate()
}

func (store *txTreeStore) StageNode(node *btree.Node) {
	// Keep the mutable node here. The commit path encodes its final state once after the callback succeeds.
	store.nodes[node.PageID()] = node
	store.dirty[node.PageID()] = node
}
