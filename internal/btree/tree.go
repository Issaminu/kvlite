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
		return root.FindEntryRef(key)
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

		node, err = tree.store.ReadNode(node.Children[childIndex])
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

	// WritableNode clones and stages the leaf on its first write in this transaction.
	// If the leaf is already dirty, WritableNode returns the existing private node.
	node = tree.store.WritableNode(node)
	// The dirty map already holds this pointer, so this change needs no second StageNode call.
	node.applyInsert(prepared)
	if node.PageID() == root.PageID() {
		root = node
	}

	pageSize := tree.store.PageSize()
	for node.NeedsSplit(pageSize) {
		if len(path) == 0 {
			// No saved parent remains, so node is the current root.
			// Keep the left half in node and create a new root above both halves.
			rightNode, separator, err := tree.splitNode(node, pageSize)
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
			// rightNode starts at root.Children[1].
			// Split it again if one split did not make it small enough.
			if err := tree.splitChildIntoSiblings(root, rightNode, 1, pageSize); err != nil {
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
		// Keep its left half in that slot and insert each new right sibling after it.
		if err := tree.splitChildIntoSiblings(parent, node, parentStep.childIndex, pageSize); err != nil {
			return nil, err
		}
		// New separators can make the parent too large, so check the parent next.
		node = parent
	}
	return root, nil
}

// splitNode splits a private node that is known to be larger than one page.
// It returns a page allocation error. Entry validation and the NeedsSplit check guarantee that the node can split, so a Split error is an internal tree invariant failure.
func (tree *Tree) splitNode(node *Node, pageSize int64) (*Node, []byte, error) {
	pageID, err := tree.store.AllocatePage()
	if err != nil {
		return nil, nil, err
	}
	rightNode, separator, err := node.Split(pageID, pageSize)
	if err != nil {
		panic(fmt.Sprintf("btree invariant: split node %d: %v", node.PageID(), err))
	}
	return rightNode, separator, nil
}

// splitChildIntoSiblings splits one child until the right result fits or cannot split.
// A leaf with one large entry or a branch with fewer than three keys uses continuation pages.
// child must be the private node at parent.Children[childIndex].
// Each split keeps the left half at the current index and inserts the right half at the next index.
// parent must also be private because each split adds one separator and one child page ID to it.
func (tree *Tree) splitChildIntoSiblings(parent, child *Node, childIndex int, pageSize int64) error {
	for child.NeedsSplit(pageSize) {
		rightNode, separator, err := tree.splitNode(child, pageSize)
		if err != nil {
			return err
		}
		tree.store.StageNode(rightNode)
		parent.InsertSplitChild(childIndex, rightNode, separator)
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
		node := root
		var path []treePathStep
		for !node.IsLeaf() {
			index, err := node.FindChildIndex(ordered[first])
			if err != nil {
				return nil, changed, err
			}
			path = append(path, treePathStep{parent: node, childIndex: index})
			node, err = tree.store.ReadNode(node.Children[index])
			if err != nil {
				return nil, changed, err
			}
			if node == nil {
				return nil, changed, ErrKeyNotFound
			}
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
		for _, key := range ordered[first:last] {
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
			leaf := tree.store.DeleteLeafEntries(node, firstIndex, rest, minimumSize)
			if leaf.PageID() == root.PageID() {
				root = leaf
			}
			var err error
			root, err = tree.repairAfterDelete(root, leaf, path, firstIndex == 0)
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

	node := root
	var path []treePathStep
	for !node.IsLeaf() {
		childIndex, err := node.FindChildIndex(key)
		if err != nil {
			return nil, false, err
		}
		path = append(path, treePathStep{parent: node, childIndex: childIndex})
		node, err = tree.store.ReadNode(node.Children[childIndex])
		if err != nil {
			return nil, false, err
		}
		if node == nil {
			return root, false, nil
		}
	}

	deleteIndex, found, err := node.prepareDelete(key, bucket)
	if err != nil || !found {
		return root, found, err
	}

	var minimumSize int64
	if len(path) > 0 {
		minimumSize = tree.store.PageSize() / 4
	}
	leaf := tree.store.DeleteLeafEntries(node, deleteIndex, nil, minimumSize)
	if leaf.PageID() == root.PageID() {
		root = leaf
	}

	// A branch separator stores the smallest key of each non-leftmost child.
	// Deleting entry zero can therefore make an ancestor separator stale.
	root, err = tree.repairAfterDelete(root, leaf, path, deleteIndex == 0)
	if err != nil {
		return nil, false, err
	}
	return root, true, nil
}

// repairAfterDelete fixes the tree after DeleteEntry removes a key from a leaf.
//
// A leaf holds keys and values. A branch holds the page IDs of its children.
// For each child after the first, the branch also holds that child's first key.
// This key helps a search choose the right child. For example, leaves [a, b]
// and [m, n] need "m" in their parent. If we delete "m", the parent must use
// "n" instead. The first child has no key in its parent. When its first key
// changes, we must check the next parent up the tree.
//
// A delete can also leave a page with little data. This function first tries
// to put its data into an adjacent page. That lets the store reuse the empty
// page. If the data will not fit, it tries to move some entries between the two
// pages. It starts at the changed leaf and moves up when a merge makes a parent
// smaller. path holds those parents in root-to-leaf order. minimumChanged says
// that the first key in the current subtree may have changed.
func (tree *Tree) repairAfterDelete(root, node *Node, path []treePathStep, minimumChanged bool) (*Node, error) {
	pageSize := tree.store.PageSize()
	// Try to save space when a node uses less than a quarter of a page.
	// For a 256-byte page, this starts below 64 bytes (25%). We do not move
	// entries after every small delete. If no move works, we keep the node.
	minimumSize := pageSize / 4
	for len(path) > 0 {
		// Start with the leaf's parent. A merge removes a child from that
		// parent, so the next loop can repair the parent in turn.
		last := len(path) - 1
		step := path[last]
		path = path[:last]

		// When the node has enough data, only its first key can require a
		// change higher in the tree.
		if node.EntryCount() != 0 && int64(node.EncodedSize()) >= minimumSize {
			if !minimumChanged {
				return root, nil
			}
			if step.childIndex == 0 {
				// This is the parent's first child. There is no key for it in
				// this parent. Pass the changed first key to the next parent.
				node = step.parent
				continue
			}

			// This child has a key in its parent. Fix that key here. The
			// parent's own first key did not change, so we can stop.
			parent := tree.store.WritableNode(step.parent)
			if parent.PageID() == root.PageID() {
				root = parent
			}
			if err := tree.refreshMinimumSeparator(parent, step.childIndex, node); err != nil {
				return nil, err
			}
			return root, nil
		}

		// Changes to a parent must use the store's private copy.
		parent := tree.store.WritableNode(step.parent)
		if parent.PageID() == root.PageID() {
			root = parent
		}
		if minimumChanged && step.childIndex > 0 {
			// Fix the child's key before a merge. For branch nodes, the merge
			// can move that key down into a child.
			if err := tree.refreshMinimumSeparator(parent, step.childIndex, node); err != nil {
				return nil, err
			}
		}

		// Only adjacent children have neighboring key ranges.
		var left, right *Node
		var err error
		if step.childIndex > 0 {
			left, err = tree.store.ReadNode(parent.Children[step.childIndex-1])
			if err != nil {
				return nil, err
			}
		}
		if step.childIndex+1 < len(parent.Children) {
			right, err = tree.store.ReadNode(parent.Children[step.childIndex+1])
			if err != nil {
				return nil, err
			}
		}

		// Try the left neighbor first. Either merge returns one page, so
		// choosing left when both fit gives a predictable result. Count
		// bytes because entries can have different sizes. An empty node
		// must merge even with a large neighbor. The result can use
		// continuation pages; the empty page can then be released.
		if left != nil && (node.EntryCount() == 0 || mergedChildrenSize(parent, step.childIndex-1, left, node) <= pageSize) {
			left = tree.store.WritableNode(left)
			mergeChildren(parent, step.childIndex-1, left, node)
			if err := tree.store.FreePage(node.PageID()); err != nil {
				return nil, err
			}
			node = parent
			// The left page stays in the tree. It still holds the parent's
			// first key, so no earlier parent needs a new key.
			minimumChanged = false
		} else if right != nil && (node.EntryCount() == 0 || mergedChildrenSize(parent, step.childIndex, node, right) <= pageSize) {
			mergeChildren(parent, step.childIndex, node, right)
			if minimumChanged && step.childIndex > 0 {
				// The delete may have emptied node. Before the merge, it had
				// no first key for its parent. It now holds right's keys, so
				// the parent can use its new first key.
				if err := tree.refreshMinimumSeparator(parent, step.childIndex, node); err != nil {
					return nil, err
				}
			}
			if err := tree.store.FreePage(right.PageID()); err != nil {
				return nil, err
			}
			node = parent
			// Only a change to the first child can change the first key of
			// the whole parent. Carry that change upward if it occurred.
			minimumChanged = minimumChanged && step.childIndex == 0
		} else {
			// Neither merge fits. Try to share the entries across two pages.
			if left == nil && right == nil {
				// The parent has only this child. There is no page to share
				// with at this level. Try to repair the parent instead.
				node = parent
				minimumChanged = minimumChanged && step.childIndex == 0
				continue
			}

			// Test each neighbor before changing it. Choose the one that
			// gives the fuller pair of pages.
			useLeft, canRedistribute := chooseSiblingRedistribution(parent, step.childIndex, left, node, right, pageSize, minimumSize)
			if !canRedistribute {
				// No way of dividing these entries fits both pages. Keep
				// the smaller page and stop.
				return root, nil
			}
			if !useLeft {
				right = tree.store.WritableNode(right)
				if !redistributeChildren(parent, step.childIndex, node, right, pageSize, minimumSize) {
					// The trial used the same rule on copies. It must also
					// work on the pages we now change.
					return nil, fmt.Errorf("selected right redistribution: %w", ErrRedistributionInvalid)
				}
				// The right page now starts at a different key. Put that key
				// in the parent so future searches reach the right page.
				if err := tree.refreshMinimumSeparator(parent, step.childIndex+1, right); err != nil {
					return nil, err
				}
				if minimumChanged && step.childIndex > 0 {
					// The changed page also has its own key in this parent.
					if err := tree.refreshMinimumSeparator(parent, step.childIndex, node); err != nil {
						return nil, err
					}
				}
				if minimumChanged && step.childIndex == 0 {
					// The first child has no key here. Pass its changed first
					// key to the next parent.
					node = parent
					continue
				}
				// Both pages still exist. The parent did not get smaller.
				return root, nil
			}

			left = tree.store.WritableNode(left)
			if !redistributeChildren(parent, step.childIndex-1, left, node, pageSize, minimumSize) {
				// The trial used the same rule on copies. It must also
				// work on the pages we now change.
				return nil, fmt.Errorf("selected left redistribution: %w", ErrRedistributionInvalid)
			}
			// The changed page got entries from the left page. Use its new
			// first key in the parent.
			if err := tree.refreshMinimumSeparator(parent, step.childIndex, node); err != nil {
				return nil, err
			}
			// The left page still holds the parent's first key.
			return root, nil
		}
	}

	// A branch with one child has no choice to make during a search. Use
	// that child as the root and return the old root page to the store.
	if !root.IsLeaf() && len(root.Children) == 1 {
		child, err := tree.store.ReadNode(root.Children[0])
		if err != nil {
			return nil, err
		}
		if err := tree.store.FreePage(root.PageID()); err != nil {
			return nil, err
		}
		root = child
	}
	return root, nil
}

// refreshMinimumSeparator gives a child its current key in the parent.
// For example, if a child once started with "m" and now starts with "n", its
// parent must store "n" to send later searches to the right child. The first
// child has no such key, so childIndex must be greater than zero. An empty
// child gives no new key. setSeparator copies a key from a read page.
func (tree *Tree) refreshMinimumSeparator(parent *Node, childIndex int, node *Node) error {
	minimum, err := tree.subtreeMinimum(node)
	if err != nil {
		return err
	}
	if minimum != nil {
		parent.setSeparator(childIndex-1, minimum)
	}
	return nil
}

// subtreeMinimum finds the first key below node. The search follows the first
// child until it reaches a leaf. The returned key belongs to that leaf and
// remains valid while the leaf remains valid. An empty leaf returns nil.
func (tree *Tree) subtreeMinimum(node *Node) ([]byte, error) {
	for !node.IsLeaf() {
		var err error
		node, err = tree.store.ReadNode(node.Children[0])
		if err != nil {
			return nil, err
		}
	}
	return node.firstKey(), nil
}
