package btree

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/page"
)

// Store provides the page view, mutable-node ownership, and page allocation used by a [Tree].
type Store interface {
	// LookupPage searches one page without returning a Node.
	// The store selects the page representation.
	// A leaf returns an entry and a zero child page ID.
	// A branch returns the selected child page ID.
	// With includeKey false, an encoded leaf entry can omit its key and does not copy a continued key.
	LookupPage(pageID page.ID, key []byte, includeKey bool) (Entry, bool, page.ID, error)
	// PageSize returns the fixed encoded size of one tree page.
	PageSize() int64
	// ReadNode returns the node visible to the current operation.
	// A returned node is read-only. WritableNode and DeleteLeafEntries return its private mutable version.
	ReadNode(page.ID) (*Node, error)
	// ReplaceLeafEntry stages a replacement without decoding the old leaf.
	// skipIdentical permits an identical entry to remain unchanged.
	// A false result keeps the ordinary read-and-update path.
	ReplaceLeafEntry(pageID page.ID, entry Entry, skipIdentical bool) (bool, error)
	// DeleteLeafEntry stages an empty replacement for a leaf containing only key.
	// The empty leaf retains its old allocation count for repair. Nil keeps the ordinary deletion path.
	DeleteLeafEntry(pageID page.ID, key []byte) (*Node, error)
	// InsertLeafNeighbor stages a separate leaf beside an unchanged large leaf.
	// before places the neighbor before the old leaf in key order.
	// A nil neighbor keeps the ordinary insertion path.
	InsertLeafNeighbor(pageID page.ID, entry Entry) (neighbor *Node, separator []byte, before bool, err error)
	// SetParent records the immediate owner of child. A nil parent selects the metadata root.
	// The store must keep these links private to the current write operation.
	SetParent(child page.ID, parent *Node)
	// WritableNode returns a private mutable version of node with the same page ID.
	// Repeated calls for that page must return the same private node.
	WritableNode(*Node) *Node
	// DeleteLeafEntries returns the private leaf after removing selected entries.
	// It must preserve the committed node and earlier write-batch callback images.
	// firstIndex and rest contain distinct, ascending indexes from the visible leaf.
	// A separate first index avoids allocating a list for single-key deletes.
	// Repeated writes to that page must reuse the same private node.
	// minimumSize permits spare entry slots below the delete-repair threshold.
	// Zero selects a leaf root, which needs no merge reservation.
	DeleteLeafEntries(*Node, int, []int, int64) *Node
	// AllocatePage returns a page ID that is not in use by another node.
	// It returns an error when the store cannot allocate a page.
	AllocatePage() (page.ID, error)
	// FreePage retires a removed tree node and all of its continuation pages.
	FreePage(page.ID) error
	// StageNode makes a private node visible to later reads and records its final image for commit.
	StageNode(*Node)
}

// Tree performs B+tree lookup, insertion, deletion, and repair through a [Store].
// It keeps no page state outside that store.
type Tree struct {
	store Store

	// SkipIdenticalPuts keeps an identical entry clean when the caller requires no durable rewrite.
	SkipIdenticalPuts bool
}

// treePathStep records one move from a branch node to one child node.
// parent is the branch node at the current level.
// childIndex selects the next node in parent.Children.
type treePathStep struct {
	parent     *Node
	childIndex int
}

// NewTree returns a tree that uses store for every node read, write, and page allocation.
func NewTree(store Store) *Tree {
	return &Tree{store: store}
}

func (tree *Tree) readChild(parent *Node, index int) (*Node, error) {
	id := parent.Children[index]
	child, err := tree.store.ReadNode(id)
	if err == nil && child != nil {
		tree.store.SetParent(id, parent)
	}
	return child, err
}

// reparentChildren records links after a split, merge, or redistribution moves entries.
// It visits only this changed node's references. It does not load the referenced nodes.
func (tree *Tree) reparentChildren(parent *Node) error {
	return VisitNodeReferences(parent, func(child page.ID) error {
		tree.store.SetParent(child, parent)
		return nil
	}, nil)
}

func (tree *Tree) recordBucketParent(parent *Node, entry Entry) error {
	if entry.Flags()&BucketLeafFlag == 0 {
		return nil
	}
	id, err := page.DecodeID(entry.Value())
	if err != nil {
		return err
	}
	tree.store.SetParent(id, parent)
	return nil
}

// FindEntryRef looks key up without copying the stored key or value.
// It returns a zero entry with found false when the key is absent.
// A found entry refers to read-only storage owned by root or the store. It must not outlive its owner.
// Set includeKey to false when the caller does not use the entry key.
// The store can then omit the key and does not copy a continued key.
// An empty or oversized key returns [ErrKeyRequired] or [ErrKeyTooLarge].
func (tree *Tree) FindEntryRef(root *Node, key []byte, includeKey bool) (Entry, bool, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, err
	}

	if root.IsLeaf() {
		entry, found, err := root.FindEntryRef(key)
		if err == nil && found {
			err = tree.recordBucketParent(root, entry)
		}
		return entry, found, err
	}
	_, _, childPageID, err := LookupNode(root, key)
	if err != nil {
		return Entry{}, false, err
	}
	return tree.findEntryRefFromPage(childPageID, key, includeKey)
}

// FindEntryRefFromPage starts at rootPageID without requiring a root Node.
// A found entry refers to storage owned by the store.
// includeKey has the same meaning as in [Tree.FindEntryRef].
func (tree *Tree) FindEntryRefFromPage(rootPageID page.ID, key []byte, includeKey bool) (Entry, bool, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, err
	}
	return tree.findEntryRefFromPage(rootPageID, key, includeKey)
}

func (tree *Tree) findEntryRefFromPage(pageID page.ID, key []byte, includeKey bool) (Entry, bool, error) {
	for {
		entry, found, childPageID, err := tree.store.LookupPage(pageID, key, includeKey)
		if err != nil {
			return Entry{}, false, err
		}
		if childPageID == 0 {
			return entry, found, nil
		}
		pageID = childPageID
	}
}

// validateEntry checks the key and value limits before insertion changes a node.
// Entries larger than one page use continuation pages at commit.
func (tree *Tree) validateEntry(entry Entry) error {
	if err := validateLookupKey(entry.Key()); err != nil {
		return err
	}
	if len(entry.Value()) > MaxValueSize {
		return ErrValueTooLarge
	}
	return nil
}

// PutEntry inserts or replaces entry and returns the current root.
// It validates the entry and checks leaf conflicts before it asks the store for a mutable node.
// If page allocation fails after a node changes, the caller must discard its private transaction state.
// The caller must use the returned root because a write can replace the root object or create a new root page.
func (tree *Tree) PutEntry(root *Node, entry Entry) (*Node, error) {
	if err := tree.validateEntry(entry); err != nil {
		return nil, err
	}

	node := root
	var path []treePathStep
	// Find the target leaf and save each branch-to-child move.
	// The split loop can use this local path without changing committed nodes.
	for !node.IsLeaf() {
		childIndex, err := node.FindChildIndex(entry.Key())
		if err != nil {
			return nil, err
		}
		path = append(path, treePathStep{parent: node, childIndex: childIndex})

		child := node.Children[childIndex]
		replaced, err := tree.store.ReplaceLeafEntry(child, entry, tree.SkipIdenticalPuts)
		if err != nil {
			return nil, err
		}
		if replaced {
			// Commit preparation can move the replacement if it grows.
			// Keep its immediate parent without decoding the old child.
			tree.store.SetParent(child, node)
			return root, nil
		}

		neighbor, separator, before, err := tree.store.InsertLeafNeighbor(child, entry)
		if err != nil {
			return nil, err
		}
		if neighbor != nil {
			parent := tree.store.WritableNode(node)
			if before {
				parent.Children[childIndex] = neighbor.PageID()
				parent.InsertSplitChild(childIndex, NewLeafNode(child), separator)
			} else {
				parent.InsertSplitChild(childIndex, neighbor, separator)
			}
			tree.store.SetParent(child, parent)
			tree.store.SetParent(neighbor.PageID(), parent)
			if parent.PageID() == root.PageID() {
				root = parent
			}
			return tree.finishPut(root, parent, path[:len(path)-1], false)
		}

		node, err = tree.readChild(node, childIndex)
		if err != nil {
			return nil, err
		}
		if node == nil {
			return nil, ErrKeyNotFound
		}
	}

	// Check leaf-specific conflicts before WritableNode makes transaction-visible state mutable.
	prepared, err := node.prepareInsert(entry)
	if err != nil {
		return nil, err
	}

	if tree.SkipIdenticalPuts && prepared.found {
		existing := node.entries[prepared.index]
		if existing.flags == entry.flags && bytes.Equal(existing.value, entry.value) {
			return root, nil
		}
	}

	// WritableNode clones and stages the leaf on its first write in this transaction.
	// If the leaf is already dirty, WritableNode returns the existing private node.
	node = tree.store.WritableNode(node)
	// The dirty map already holds this pointer, so this change needs no second StageNode call.
	node.applyInsert(prepared)
	if err := tree.recordBucketParent(node, entry); err != nil {
		return nil, err
	}
	if node.PageID() == root.PageID() {
		root = node
	}

	return tree.finishPut(root, node, path, !prepared.found && prepared.index == node.EntryCount()-1)
}

// finishPut splits the changed node and its ancestors as needed.
func (tree *Tree) finishPut(root, node *Node, path []treePathStep, tailInsert bool) (*Node, error) {
	pageSize := tree.store.PageSize()
	if tailInsert && node.NeedsSplit(pageSize) {
		for _, step := range path {
			if step.childIndex != len(step.parent.Children)-1 {
				tailInsert = false
				break
			}
		}
	}
	for node.NeedsSplit(pageSize) {
		if len(path) == 0 {
			// No saved parent remains, so node is the current root.
			// Keep the left result in node and create a new root above both children.
			rightNode, separator, err := tree.splitNode(node, pageSize, tailInsert)
			if err != nil {
				return nil, err
			}
			rootPageID, err := tree.store.AllocatePage()
			if err != nil {
				return nil, err
			}
			root = NewRootNode(rootPageID, node, rightNode, separator)
			// node is already staged. Only the new right node and new root need staging.
			tree.store.StageNode(rightNode)
			tree.store.StageNode(root)
			tree.store.SetParent(node.PageID(), root)
			tree.store.SetParent(rightNode.PageID(), root)
			// rightNode starts at root.Children[1].
			// Split it again if one split did not make it small enough.
			if err := tree.splitChildIntoSiblings(root, rightNode, 1, pageSize, false); err != nil {
				return nil, err
			}
			node = root
			continue
		}

		// The last path item contains the direct parent of node.
		// Remove it because the next pass can continue with that parent.
		lastStep := len(path) - 1
		parentStep := path[lastStep]
		path = path[:lastStep]

		// A child split adds a separator and a child page ID to its parent.
		// WritableNode returns a staged private parent that this operation can change.
		parent := tree.store.WritableNode(parentStep.parent)
		if parent.PageID() == root.PageID() {
			// The private root has the same page ID but a different object.
			root = parent
		}

		// node is at parent.Children[parentStep.childIndex].
		// Keep its left result in that slot and insert each new right sibling after it.
		if err := tree.splitChildIntoSiblings(parent, node, parentStep.childIndex, pageSize, tailInsert); err != nil {
			return nil, err
		}
		// New separators can make the parent too large, so check the parent next.
		node = parent
	}
	return root, nil
}

// splitNode splits a private node that is known to be larger than one page.
// It returns a page allocation error. Entry validation and the NeedsSplit check guarantee that the node can split, so a Split error is an internal tree invariant failure.
func (tree *Tree) splitNode(node *Node, pageSize int64, tailInsert bool) (*Node, []byte, error) {
	pageID, err := tree.store.AllocatePage()
	if err != nil {
		return nil, nil, err
	}
	var rightNode *Node
	var separator []byte
	last := node.EntryCount() - 1
	if tailInsert && node.IsLeaf() && (last == 1 || node.EncodedSize()-node.entries[last].EncodedSize(true) <= int(pageSize)) {
		// A global append leaves the earlier keys in their already-fitting page.
		separator = leafSeparator(node.entries[last-1].key, node.entries[last].key)
		rightNode = NewLeafNode(pageID)
		node.splitLeaf(rightNode, last)
		node.header.Checksum = 0
	} else {
		rightNode, separator, err = node.Split(pageID, pageSize)
	}
	if err != nil {
		panic(fmt.Sprintf("btree invariant: split node %d: %v", node.PageID(), err))
	}
	if err := tree.reparentChildren(rightNode); err != nil {
		return nil, nil, err
	}
	return rightNode, separator, nil
}

// splitChildIntoSiblings splits one child until the right result fits or cannot split.
// A leaf with one large entry or a branch with fewer than three keys uses continuation pages.
// child must be the private node at parent.Children[childIndex].
// Each split keeps the left result at the current index and inserts the right result at the next index.
// parent must also be private because each split adds one separator and one child page ID to it.
func (tree *Tree) splitChildIntoSiblings(parent, child *Node, childIndex int, pageSize int64, tailInsert bool) error {
	for child.NeedsSplit(pageSize) {
		rightNode, separator, err := tree.splitNode(child, pageSize, tailInsert)
		if err != nil {
			return err
		}
		tree.store.StageNode(rightNode)
		parent.InsertSplitChild(childIndex, rightNode, separator)
		tree.store.SetParent(rightNode.PageID(), parent)
		// If the right half is still too large, split it at its new index.
		child = rightNode
		childIndex++
	}
	return nil
}

// DeleteEntry removes one non-bucket entry from the tree rooted at root.
// It returns the replacement root and true when it removes the key.
// root must be non-nil and must belong to this tree's store.
// A missing key returns root, false, and nil without changing the tree.
// An empty or oversized key returns [ErrKeyRequired] or [ErrKeyTooLarge].
// A key that identifies a nested bucket returns [ErrIncompatibleValue].
// A successful delete can replace the root and retire unused pages through
// [Store.FreePage]. If a store operation fails after mutation starts, the
// caller must discard the private store changes.
func (tree *Tree) DeleteEntry(root *Node, key []byte) (*Node, bool, error) {
	return tree.deleteEntry(root, key, false)
}

// DeleteEntries removes plain keys from one tree. It changes each leaf once
// for keys in that leaf and repairs the leaf before it moves to the next one.
// A missing key has no effect. Duplicate keys have the same effect as one key.
// If a key names a bucket, earlier leaves can already have changed.
// The caller must discard the transaction when it returns an error.
func (tree *Tree) DeleteEntries(root *Node, keys [][]byte) (*Node, bool, error) {
	for _, key := range keys {
		if err := validateLookupKey(key); err != nil {
			return root, false, err
		}
	}
	ordered := keys
	if !slices.IsSortedFunc(keys, bytes.Compare) {
		ordered = slices.Clone(keys)
		slices.SortFunc(ordered, bytes.Compare)
	}
	changed := false
	for first := 0; first < len(ordered); {
		node, path, deleted, err := tree.findDeleteLeaf(root, ordered[first], false)
		if err != nil {
			return nil, changed, err
		}
		if node == nil {
			return nil, changed, ErrKeyNotFound
		}
		var upper []byte
		for index := len(path) - 1; index >= 0; index-- {
			step := path[index]
			if step.childIndex < step.parent.EntryCount() {
				upper = step.parent.EntryAt(step.childIndex).Key()
				break
			}
		}
		last := first
		for last < len(ordered) && (upper == nil || bytes.Compare(ordered[last], upper) < 0) {
			last++
		}
		firstIndex, previousIndex := -1, -1
		var rest []int
		if deleted {
			firstIndex = 0
		}
		for _, key := range ordered[first:last] {
			if deleted {
				break
			}
			index, found, err := node.prepareDelete(key, false)
			if err != nil {
				return root, changed, err
			}
			if !found || index == previousIndex {
				continue
			}
			if firstIndex < 0 {
				firstIndex = index
			} else {
				if rest == nil {
					rest = make([]int, 0, last-first-1)
				}
				rest = append(rest, index)
			}
			previousIndex = index
		}
		if firstIndex >= 0 {
			var minimumSize int64
			if len(path) > 0 {
				minimumSize = tree.store.PageSize() / 4
			}
			leaf := node
			if !deleted {
				leaf = tree.store.DeleteLeafEntries(node, firstIndex, rest, minimumSize)
			}
			if leaf.PageID() == root.PageID() {
				root = leaf
			}
			var err error
			root, err = tree.repairAfterDelete(root, leaf, path)
			if err != nil {
				return nil, changed, err
			}
			changed = true
		}
		first = last
	}
	return root, changed, nil
}

// DeleteBucketEntry removes one bucket entry and repairs its parent tree.
// The caller must release the pages owned by that bucket.
// If a store operation fails after mutation starts, the caller must discard its private store changes.
func (tree *Tree) DeleteBucketEntry(root *Node, key []byte) (*Node, bool, error) {
	return tree.deleteEntry(root, key, true)
}

func (tree *Tree) deleteEntry(root *Node, key []byte, bucket bool) (*Node, bool, error) {
	if len(key) == 0 {
		return nil, false, ErrKeyRequired
	}
	if len(key) > MaxKeySize {
		return nil, false, ErrKeyTooLarge
	}

	node, path, deleted, err := tree.findDeleteLeaf(root, key, bucket)
	if err != nil || node == nil {
		return root, false, err
	}

	deleteIndex := 0
	if !deleted {
		var found bool
		deleteIndex, found, err = node.prepareDelete(key, bucket)
		if err != nil || !found {
			return root, found, err
		}
	}

	var minimumSize int64
	if len(path) > 0 {
		minimumSize = tree.store.PageSize() / 4
	}
	leaf := node
	if !deleted {
		leaf = tree.store.DeleteLeafEntries(node, deleteIndex, nil, minimumSize)
	}
	if leaf.PageID() == root.PageID() {
		root = leaf
	}

	// Retain valid lower boundaries and repair nodes that became under-full.
	root, err = tree.repairAfterDelete(root, leaf, path)
	if err != nil {
		return nil, false, err
	}
	return root, true, nil
}

// findDeleteLeaf shares traversal and parent tracking for scalar and batch deletes.
// deleted identifies an already-staged empty leaf that only needs ordinary repair.
func (tree *Tree) findDeleteLeaf(root *Node, key []byte, bucket bool) (*Node, []treePathStep, bool, error) {
	node := root
	var path []treePathStep
	for !node.IsLeaf() {
		index, err := node.FindChildIndex(key)
		if err != nil {
			return nil, path, false, err
		}
		path = append(path, treePathStep{parent: node, childIndex: index})
		if !bucket {
			id := node.Children[index]
			tree.store.SetParent(id, node)
			leaf, err := tree.store.DeleteLeafEntry(id, key)
			if err != nil || leaf != nil {
				return leaf, path, leaf != nil, err
			}
		}
		node, err = tree.readChild(node, index)
		if err != nil || node == nil {
			return node, path, false, err
		}
	}
	return node, path, false, nil
}

// repairAfterDelete merges or redistributes under-full nodes after a deletion.
// Separators are lower bounds: deleting a minimum key leaves its old boundary valid.
// Redistribution writes the boundary that moves. Merges carry a parent boundary
// into the combined branch, so they also preserve the same routing invariant.
func (tree *Tree) repairAfterDelete(root, node *Node, path []treePathStep) (*Node, error) {
	pageSize := tree.store.PageSize()
	// Repair below one quarter of a page rather than moving entries after every delete.
	minimumSize := pageSize / 4
	for len(path) > 0 {
		last := len(path) - 1
		step := path[last]
		path = path[:last]
		if node.EntryCount() != 0 && int64(node.EncodedSize()) >= minimumSize {
			return root, nil
		}
		parent := tree.store.WritableNode(step.parent)
		if parent.PageID() == root.PageID() {
			root = parent
		}
		// Remove an empty leaf without decoding or changing its neighbor.
		if node.IsLeaf() && node.EntryCount() == 0 && len(parent.Children) > 1 {
			separatorIndex := max(step.childIndex-1, 0)
			parent.entries = slices.Delete(parent.entries, separatorIndex, separatorIndex+1)
			parent.Children = slices.Delete(parent.Children, step.childIndex, step.childIndex+1)
			parent.encodedSize = 0
			parent.header.Checksum = 0
			if err := tree.store.FreePage(node.PageID()); err != nil {
				return nil, err
			}
			node = parent
			continue
		}
		var left, right *Node
		var err error
		if step.childIndex > 0 {
			left, err = tree.readChild(parent, step.childIndex-1)
			if err != nil {
				return nil, err
			}
		}
		if step.childIndex+1 < len(parent.Children) {
			right, err = tree.readChild(parent, step.childIndex+1)
			if err != nil {
				return nil, err
			}
		}
		// Prefer a merge with the left neighbor when both fit. An empty branch must
		// merge even if the combined image needs more than one storage page.
		if left != nil && (node.EntryCount() == 0 || mergedChildrenSize(parent, step.childIndex-1, left, node) <= pageSize) {
			left = tree.store.WritableNode(left)
			mergeChildren(parent, step.childIndex-1, left, node)
			if err := tree.reparentChildren(left); err != nil {
				return nil, err
			}
			if err := tree.store.FreePage(node.PageID()); err != nil {
				return nil, err
			}
			node = parent
		} else if right != nil && (node.EntryCount() == 0 || mergedChildrenSize(parent, step.childIndex, node, right) <= pageSize) {
			// A parent reached after a merge can still be a committed node.
			node = tree.store.WritableNode(node)
			mergeChildren(parent, step.childIndex, node, right)
			if err := tree.reparentChildren(node); err != nil {
				return nil, err
			}
			if err := tree.store.FreePage(right.PageID()); err != nil {
				return nil, err
			}
			node = parent
		} else {
			if left == nil && right == nil {
				node = parent
				continue
			}
			useLeft, canRedistribute := chooseSiblingRedistribution(parent, step.childIndex, left, node, right, pageSize, minimumSize)
			if !canRedistribute {
				return root, nil
			}
			// Trials use copies. Apply the same rule only to private nodes.
			node = tree.store.WritableNode(node)
			if !useLeft {
				right = tree.store.WritableNode(right)
				if !redistributeChildren(parent, step.childIndex, node, right, pageSize, minimumSize) {
					return nil, fmt.Errorf("selected right redistribution: %w", ErrRedistributionInvalid)
				}
				if err := tree.reparentChildren(node); err != nil {
					return nil, err
				}
				if err := tree.reparentChildren(right); err != nil {
					return nil, err
				}
			} else {
				left = tree.store.WritableNode(left)
				if !redistributeChildren(parent, step.childIndex-1, left, node, pageSize, minimumSize) {
					return nil, fmt.Errorf("selected left redistribution: %w", ErrRedistributionInvalid)
				}
				if err := tree.reparentChildren(left); err != nil {
					return nil, err
				}
				if err := tree.reparentChildren(node); err != nil {
					return nil, err
				}
			}
			return root, nil
		}
	}
	// A branch with one child no longer needs to be a root.
	if !root.IsLeaf() && len(root.Children) == 1 {
		child, err := tree.readChild(root, 0)
		if err != nil {
			return nil, err
		}
		if err := tree.store.FreePage(root.PageID()); err != nil {
			return nil, err
		}
		root = child
		tree.store.SetParent(root.PageID(), nil)
	}
	return root, nil
}
