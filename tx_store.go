package kvlite

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// dirtyNode keeps the original and final images of one changed node.
// A nil original requests a complete WAL image.
type dirtyNode struct {
	original *btree.MutableNode
	final    *btree.MutableNode
}

// txTreeStore owns transaction visibility, private edits, and allocation changes.
// Parent paths and reference repair belong to Tree.
type txTreeStore struct {
	tx        *Tx
	committed btree.NodeReader
	baseDirty map[page.ID]dirtyNode
	dirty     map[page.ID]dirtyNode
}

func (store *txTreeStore) PageSize() int64 { return store.tx.meta.PageSize() }

func (store *txTreeStore) Writable() bool { return store.tx.Writable() }

func (store *txTreeStore) privateNode(id page.ID) *btree.MutableNode {
	if len(store.dirty) != 0 {
		if change, ok := store.dirty[id]; ok {
			return change.final
		}
	}
	if len(store.baseDirty) != 0 {
		return store.baseDirty[id].final
	}
	return nil
}

// ReadNode resolves this callback, earlier successful batch callbacks, then committed storage. It does not decode or retain nodes merely to read them.
func (store *txTreeStore) ReadNode(id page.ID) (btree.Node, error) {
	if node := store.privateNode(id); node != nil {
		return node.ReadOnly(), nil
	}
	return store.committed.ReadNode(id)
}

// EditNode returns this callback's writable node, copying shared lists on first edit.
func (store *txTreeStore) EditNode(id page.ID) (*btree.MutableNode, error) {
	if err := store.tx.writableError(); err != nil {
		return nil, err
	}
	// The caller changes the returned node, so each call is a change.
	store.tx.changes++
	if change, ok := store.dirty[id]; ok {
		return change.final, nil
	}
	original := store.privateNode(id)
	var private *btree.MutableNode
	if original != nil {
		private = original.Clone()
	} else {
		var err error
		original, private, err = store.tx.db.cloneNode(id)
		if err != nil {
			return nil, err
		}
	}
	change := store.baseDirty[id]
	if change.final == nil {
		change.original = original
	}
	change.final = private
	store.dirty[id] = change
	return change.final, nil
}

func (store *txTreeStore) VisitNodeReferences(id page.ID, visit, continuation func(page.ID) error) error {
	node, err := store.ReadNode(id)
	if err != nil {
		return err
	}
	return node.VisitReferences(store.PageSize(), visit, continuation)
}

func (store *txTreeStore) AllocatePage() (page.ID, error) {
	if err := store.tx.writableError(); err != nil {
		return 0, err
	}
	store.tx.changes++
	id, err := store.tx.writableAllocation().allocate(store.tx.meta.LastPage())
	if err != nil {
		return 0, err
	}
	if id > store.tx.meta.LastPage() {
		store.tx.writableMeta().SetLastPage(id)
		store.tx.metaDirty = true
	}
	return id, nil
}

// FreePage defers reuse of the entire node span until commit preparation.
func (store *txTreeStore) FreePage(id page.ID) error {
	if err := store.tx.writableError(); err != nil {
		return err
	}
	store.tx.changes++
	node, err := store.ReadNode(id)
	if err != nil {
		return err
	}
	if err := store.tx.writableAllocation().retireSpan(id, node.StoragePageCount(store.PageSize())); err != nil {
		return err
	}
	delete(store.dirty, id)
	return nil
}

func (store *txTreeStore) StageNode(node *btree.MutableNode) {
	store.tx.changes++
	change := store.baseDirty[node.PageID()]
	change.final = node
	store.dirty[node.PageID()] = change
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
	prepared := make(map[*btree.MutableNode]bool, len(tx.store.dirty))
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
					node.SetPageID(newHead)
					change.original = nil
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

// repairMovedNodes delegates relationship changes to Tree and updates bucket handles.
func (tx *Tx) repairMovedNodes(moved map[page.ID]page.ID) error {
	root, err := tx.tree.RepairMoved(tx.meta.Root(), moved)
	if err != nil {
		return err
	}
	if root != tx.meta.Root() {
		tx.writableMeta().SetRoot(root)
		tx.metaDirty = true
	}
	var refresh func(*Bucket)
	refresh = func(bucket *Bucket) {
		if bucket == nil || bucket.deleted {
			return
		}
		if target, ok := moved[bucket.rootPageID]; ok {
			bucket.rootPageID = target
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
