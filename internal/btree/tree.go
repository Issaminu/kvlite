package btree

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/page"
)

// Store provides the storage operations that Tree needs. Tree owns the B+tree
// algorithm, but it does not own database files, page allocation, or the WAL.
// The database package implements this interface so Tree can use those services
// without importing the database package and creating an import cycle.
type Store interface {
	PageSize() int64
	ReadNode(page.ID) (*Node, error)
	AllocatePage() page.ID
	StageNode(*Node)
}

// Tree performs B+tree lookup, insertion, and splitting. It uses its Store when
// an operation needs database-owned state, such as a child page or a new page ID.
type Tree struct {
	store Store
}

func NewTree(store Store) *Tree {
	return &Tree{store: store}
}

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

		childNode.SetParent(node)
		childNode.Index = childIndex
		node = childNode
	}
	return node, nil
}

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
	if nodeHeaderSize+entry.EncodedSize(true) > int(tree.store.PageSize()) {
		return fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLarge, entry.Key(), tree.store.PageSize())
	}
	return nil
}

// PutEntry inserts entry into the tree that starts at root. It returns the
// current root because a split can create a replacement root. The caller owns
// that root and must adopt it after this function succeeds.
func (tree *Tree) PutEntry(root *Node, entry Entry) (*Node, error) {
	if err := tree.validateEntry(entry); err != nil {
		return nil, err
	}

	node, err := tree.findLeafNode(root, entry.Key())
	if err != nil {
		return nil, err
	}
	if err := node.InsertEntry(entry); err != nil {
		return nil, err
	}

	pageSize := tree.store.PageSize()
	if !node.NeedsSplit(pageSize) {
		tree.store.StageNode(node)
		return root, nil
	}

	for node.NeedsSplit(pageSize) {
		rightNode, _, separator, err := node.Split(tree.store.AllocatePage(), pageSize)
		if err != nil {
			if errors.Is(err, ErrNodeNotSaturated) {
				// Split only fails this way when one entry, or too few branch
				// entries, already exceeds the page size. Two non-empty nodes
				// cannot be produced in that case.
				return nil, fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLarge, entry.Key(), pageSize)
			}
			return nil, err
		}

		if node == root {
			root = NewRootNode(tree.store.AllocatePage(), node, rightNode, separator)
			tree.store.StageNode(root)
		} else {
			parent := node.Parent()
			parent.InsertSplitChild(node, rightNode, separator)
			tree.store.StageNode(parent)
		}

		tree.store.StageNode(node)
		tree.store.StageNode(rightNode)

		// Split an oversized right sibling before moving to its parent. This
		// ensures that every staged node fits inside one page.
		if rightNode.NeedsSplit(pageSize) {
			node = rightNode
			continue
		}
		node = node.Parent()
	}

	return root, nil
}

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

// FindEntryRef looks a key up without copying the stored key or value.
// The returned entry refers to storage owned by the leaf node.
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

	// Keep both sides non-empty. The half-page cut lands on index 0 when the first
	// entry alone fills half a page; without this clamp the left node would be empty
	// and the right node would keep the whole over-page content — an invalid split.
	// A leaf keeps [:sep] on the left and [sep:] on the right, so sep must be in
	// [1, len-1]. A branch also pushes entries[sep] up, so its right side is
	// [sep+1:]; sep must be in [1, len-2], which needs at least 3 entries.
	if n.IsLeaf {
		if len(n.entries) < 2 {
			return -1, ErrNodeNotSaturated
		}
		return min(max(separatorIndex, 1), len(n.entries)-1), nil
	}

	if len(n.entries) < 3 {
		return -1, ErrNodeNotSaturated
	}
	return min(max(separatorIndex, 1), len(n.entries)-2), nil
}

func (n *Node) Split(newPageID page.ID, pageSize int64) (*Node, int, []byte, error) {
	separatorIndex, err := n.chooseSplitIndex(pageSize)
	if err != nil {
		return nil, separatorIndex, nil, err
	}

	rightNode := &Node{
		IsLeaf:   n.IsLeaf,
		Children: []page.ID{},
		Index:    n.Index + 1,
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

	return rightNode, separatorIndex, separator, nil
}

func (parent *Node) InsertSplitChild(leftNode, rightNode *Node, separator []byte) {
	rightNode.parent = parent

	parent.entries = append(parent.entries, Entry{})
	copy(parent.entries[leftNode.Index+1:], parent.entries[leftNode.Index:])
	parent.entries[leftNode.Index] = Entry{key: separator}

	parent.Children = append(parent.Children, 0)
	copy(parent.Children[rightNode.Index+1:], parent.Children[rightNode.Index:])
	parent.Children[rightNode.Index] = rightNode.pgid
}
