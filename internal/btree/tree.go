package btree

import (
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/page"
)

// Store provides the page view, mutable-node ownership, and page allocation used by a [Tree].
type Store interface {
	// PageSize returns the fixed encoded size of one tree page.
	PageSize() int64
	// ReadNode returns the node visible to the current operation.
	// A returned node is read-only until [Store.WritableNode] returns a private version.
	ReadNode(page.ID) (*Node, error)
	// WritableNode returns a private mutable version of node with the same page ID.
	// Repeated calls for that page must return the same private node.
	WritableNode(*Node) *Node
	// AllocatePage returns a page ID that is not in use by another node.
	AllocatePage() page.ID
	// StageNode makes a private node visible to later reads and records its final image for commit.
	StageNode(*Node)
}

// Tree performs B+tree lookup, insertion, and splitting through a [Store].
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

// findLeafNode follows branch separators without requesting mutable nodes.
// It returns [ErrKeyNotFound] if a selected child page has no node.
func (tree *Tree) findLeafNode(root *Node, key []byte) (*Node, error) {
	node := root
	for !node.IsLeaf {
		childIndex, err := node.FindChildIndex(key)
		if err != nil {
			return nil, err
		}
		childNode, err := tree.store.ReadNode(node.Children[childIndex])
		if err != nil {
			return nil, err
		}
		if childNode == nil {
			return nil, ErrKeyNotFound
		}
		node = childNode
	}
	return node, nil
}

// validateEntry rejects an empty or oversized key, an oversized value, or an entry that cannot fit the tree's fixed-page representation.
// A leaf stores the full key and value.
// A later split can promote the key into a branch separator, where the smallest valid branch stores the separator and two child page IDs.
// Both forms must fit before insertion starts.
func (tree *Tree) validateEntry(entry Entry) error {
	if len(entry.Key()) == 0 {
		return ErrKeyRequired
	}
	if len(entry.Key()) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(entry.Value()) > MaxValueSize {
		return ErrValueTooLarge
	}
	pageSize := int(tree.store.PageSize())
	leafSize := nodeHeaderSize + entry.EncodedSize(true)
	// The smallest branch has one separator with a left and right child, so it needs two page IDs.
	branchSize := nodeHeaderSize + entry.EncodedSize(false) + 2*page.IDSize
	if leafSize > pageSize || branchSize > pageSize {
		return fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLarge, entry.Key(), tree.store.PageSize())
	}
	return nil
}

// PutEntry inserts or replaces entry and returns the current root.
// It validates the entry and checks leaf conflicts before it asks the store for a mutable node.
// If PutEntry returns an error, it has not changed the tree.
// The caller must use the returned root because a write can replace the root object or create a new root page.
func (tree *Tree) PutEntry(root *Node, entry Entry) (*Node, error) {
	if err := tree.validateEntry(entry); err != nil {
		return nil, err
	}

	node := root
	var path []treePathStep
	// Find the target leaf and save each branch-to-child move.
	// The split loop can use this local path without changing committed nodes.
	for !node.IsLeaf {
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
			rightNode, separator := tree.splitNode(node, pageSize)
			root = NewRootNode(tree.store.AllocatePage(), node, rightNode, separator)
			// node is already staged. Only the new right node and new root need staging.
			tree.store.StageNode(rightNode)
			tree.store.StageNode(root)
			// rightNode starts at root.Children[1].
			// Split it again if one split did not make it small enough.
			tree.splitChildIntoSiblings(root, rightNode, 1, pageSize)
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
		tree.splitChildIntoSiblings(parent, node, parentStep.childIndex, pageSize)
		// New separators can make the parent too large, so check the parent next.
		node = parent
	}
	return root, nil
}

// splitNode splits a private node that is known to be larger than one page.
// Entry validation and the NeedsSplit check guarantee that the node can split, so an error here is an internal tree invariant failure.
func (tree *Tree) splitNode(node *Node, pageSize int64) (*Node, []byte) {
	rightNode, separator, err := node.Split(tree.store.AllocatePage(), pageSize)
	if err != nil {
		panic(fmt.Sprintf("btree invariant: split node %d: %v", node.PageID(), err))
	}
	return rightNode, separator
}

// splitChildIntoSiblings splits one child until each result fits in one page.
// child must be the private node at parent.Children[childIndex].
// Each split keeps the left half at the current index and inserts the right half at the next index.
// parent must also be private because each split adds one separator and one child page ID to it.
func (tree *Tree) splitChildIntoSiblings(parent, child *Node, childIndex int, pageSize int64) {
	for child.NeedsSplit(pageSize) {
		rightNode, separator := tree.splitNode(child, pageSize)
		tree.store.StageNode(rightNode)
		parent.InsertSplitChild(childIndex, rightNode, separator)
		// If the right half is still too large, split it at its new index.
		child = rightNode
		childIndex++
	}
}

// FindEntry looks key up and returns an entry whose key and value do not share storage with the leaf node.
// It returns a zero entry with found false when the key is absent.
// An empty or oversized key returns [ErrKeyRequired] or [ErrKeyTooLarge].
func (tree *Tree) FindEntry(root *Node, key []byte) (Entry, bool, error) {
	if len(key) == 0 {
		return Entry{}, false, ErrKeyRequired
	}
	if len(key) > MaxKeySize {
		return Entry{}, false, ErrKeyTooLarge
	}

	node, err := tree.findLeafNode(root, key)
	if err != nil {
		return Entry{}, false, err
	}
	return node.FindEntry(key)
}

// FindEntryRef looks key up without copying the stored key or value.
// It returns a zero entry with found false when the key is absent.
// A found entry refers to read-only storage owned by the leaf node and must not outlive that node's owner.
// An empty or oversized key returns [ErrKeyRequired] or [ErrKeyTooLarge].
func (tree *Tree) FindEntryRef(root *Node, key []byte) (Entry, bool, error) {
	if len(key) == 0 {
		return Entry{}, false, ErrKeyRequired
	}
	if len(key) > MaxKeySize {
		return Entry{}, false, ErrKeyTooLarge
	}

	node, err := tree.findLeafNode(root, key)
	if err != nil {
		return Entry{}, false, err
	}
	return node.FindEntryRef(key)
}

// NeedsSplit reports whether the encoded node is larger than one page.
func (n *Node) NeedsSplit(pageSize int64) bool {
	return int64(n.EncodedSize()) > pageSize
}

// EncodedSize reports the exact number of bytes that [EncodeNode] writes for n.
func (n *Node) EncodedSize() int {
	size := nodeHeaderSize
	for _, entry := range n.entries {
		size += entry.EncodedSize(n.IsLeaf)
	}
	if !n.IsLeaf {
		size += len(n.Children) * page.IDSize
	}
	return size
}

// chooseSplitIndex selects a separator near half a page based on encoded entry bytes while preserving valid left and right node shapes.
// A leaf keeps its separator as the first entry of the right node, while a branch promotes and removes its separator.
func (n *Node) chooseSplitIndex(pageSize int64) (int, error) {
	limit := int(pageSize / 2)
	currSize := 0
	separatorIndex := -1

	for i, entry := range n.entries {
		currSize += entry.EncodedSize(n.IsLeaf)
		if currSize >= limit {
			separatorIndex = i
			break
		}
	}

	if separatorIndex == -1 {
		return -1, ErrNodeNotSaturated
	}

	// The leaf separator remains in the right node, so each side must keep at least one entry.
	if n.IsLeaf {
		if len(n.entries) < 2 {
			return -1, ErrNodeNotSaturated
		}
		return min(max(separatorIndex, 1), len(n.entries)-1), nil
	}

	if len(n.entries) < 2 {
		return -1, ErrNodeNotSaturated
	}
	// Removing a separator from a two-entry branch leaves one side with no separator and one child page.
	if len(n.entries) == 2 {
		return min(max(separatorIndex, 0), 1), nil
	}
	return min(max(separatorIndex, 1), len(n.entries)-2), nil
}

// Split changes n into the left node and returns a new right node and its read-only separator key.
// A leaf keeps the separator as the first entry of the right node.
// A branch promotes the separator and removes it from both child nodes.
// If n has too few entries to split, Split returns [ErrNodeNotSaturated] without changing n.
func (n *Node) Split(newPageID page.ID, pageSize int64) (*Node, []byte, error) {
	separatorIndex, err := n.chooseSplitIndex(pageSize)
	if err != nil {
		return nil, nil, err
	}

	rightNode := &Node{
		IsLeaf:   n.IsLeaf,
		Children: []page.ID{},
		pgid:     newPageID,
	}

	separator := n.entries[separatorIndex].key
	if n.IsLeaf {
		rightNode.entries = slices.Clone(n.entries[separatorIndex:])
		n.entries = n.entries[:separatorIndex]
	} else {
		rightNode.entries = slices.Clone(n.entries[separatorIndex+1:])
		n.entries = n.entries[:separatorIndex]
	}

	if !n.IsLeaf {
		rightNode.Children = slices.Clone(n.Children[separatorIndex+1:])
		n.Children = n.Children[:separatorIndex+1]
	}

	return rightNode, separator, nil
}

// InsertSplitChild inserts separator and rightNode immediately after the existing child at leftIndex.
// parent must be a branch node, leftIndex must identify an existing child, and separator must remain read-only after the call.
func (parent *Node) InsertSplitChild(leftIndex int, rightNode *Node, separator []byte) {
	parent.entries = append(parent.entries, Entry{})
	copy(parent.entries[leftIndex+1:], parent.entries[leftIndex:])
	parent.entries[leftIndex] = Entry{key: separator}

	parent.Children = append(parent.Children, 0)
	copy(parent.Children[leftIndex+2:], parent.Children[leftIndex+1:])
	parent.Children[leftIndex+1] = rightNode.pgid
}
