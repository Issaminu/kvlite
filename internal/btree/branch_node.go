package btree

import (
	"bytes"
	"slices"
	"sort"

	"github.com/Issaminu/kvlite/internal/page"
)

func NewRootNode(pgid page.ID, leftNode, rightNode *Node, separator []byte) *Node {
	return &Node{
		header:   newNodeHeader(NodeTypeBranch, pgid),
		entries:  []Entry{{key: slices.Clone(separator)}},
		Children: []page.ID{leftNode.PageID(), rightNode.PageID()},
	}
}

// Find the correct child node for this key.
// It only returns the correct child for this node, if you actually want to reach the leaf node that has the key, then this function should be called in a loop.
func (n *Node) FindChildIndex(key []byte) (int, error) {
	if n.IsLeaf() {
		return -1, ErrNotBranchNode
	}
	index := sort.Search(len(n.entries), func(index int) bool {
		return bytes.Compare(key, n.entries[index].key) < 0
	})
	return index, nil
}

func (n *Node) setSeparator(index int, key []byte) {
	n.encodedSize = 0
	n.entries[index].key = slices.Clone(key)
	n.header.Checksum = 0
}

// InsertSplitChild inserts separator and rightNode immediately after the existing child at leftIndex.
// parent must be a branch node, leftIndex must identify an existing child, and separator must remain read-only after the call.
func (parent *Node) InsertSplitChild(leftIndex int, rightNode *Node, separator []byte) {
	parent.encodedSize = 0
	parent.entries = append(parent.entries, Entry{})
	copy(parent.entries[leftIndex+1:], parent.entries[leftIndex:])
	parent.entries[leftIndex] = Entry{key: separator}

	parent.Children = append(parent.Children, 0)
	copy(parent.Children[leftIndex+2:], parent.Children[leftIndex+1:])
	parent.Children[leftIndex+1] = rightNode.PageID()
	parent.header.Checksum = 0
}

// chooseBranchRedistribution finds which key should move up to the parent.
// The keys before it stay in the left branch. The keys after it stay in the
// right branch. Each branch must keep a key, at least two children, and fit in
// one page. It returns -1 if no key gives that result.
func chooseBranchRedistribution(entries []Entry, pageSize, minimumSize int64) int {
	// We need a key on each side of the key that moves up.
	if len(entries) < 3 {
		return -1
	}

	// prefix[i] counts the bytes before entry i. An entry's size includes
	// one child page ID. The size calculations add the last child ID.
	prefix := make([]int64, len(entries)+1)
	for index := range entries {
		prefix[index+1] = prefix[index] + int64(entries[index].EncodedSize(false))
	}
	best := -1
	bestUnderfull, bestImbalance := int64(^uint64(0)>>1), int64(^uint64(0)>>1)
	for pivot := 1; pivot < len(entries)-1; pivot++ {
		// Do not count the key that moves up. Each branch also needs its
		// last child ID after its last key.
		leftSize := int64(NodeHeaderSize+page.IDSize) + prefix[pivot]
		rightEntrySize := prefix[len(entries)] - prefix[pivot+1]
		rightSize := int64(NodeHeaderSize+page.IDSize) + rightEntrySize
		if leftSize > pageSize || rightSize > pageSize {
			continue
		}
		underfull, imbalance := redistributionPenalty(leftSize, rightSize, minimumSize)
		if underfull < bestUnderfull || underfull == bestUnderfull && imbalance < bestImbalance {
			best, bestUnderfull, bestImbalance = pivot, underfull, imbalance
		}
	}
	return best
}

// redistributeBranches changes the entry lists and the parent separator if both pages fit.
func redistributeBranches(parent *Node, separatorIndex int, left, right *Node, pageSize, minimumSize int64) bool {
	// Branch entries route searches to child pages. Put the old parent
	// key between these two groups before we select a new key for it.
	entries := make([]Entry, 0, len(left.entries)+len(right.entries)+1)
	entries = append(entries, left.entries...)
	entries = append(entries, parent.entries[separatorIndex])
	entries = append(entries, right.entries...)
	children := make([]page.ID, 0, len(left.Children)+len(right.Children))
	children = append(children, left.Children...)
	children = append(children, right.Children...)
	pivot := chooseBranchRedistribution(entries, pageSize, minimumSize)
	if pivot < 0 {
		return false
	}
	// The chosen key leaves the child pages and becomes the parent key.
	// The children on either side of it must move with their keys.
	parent.entries[separatorIndex] = Entry{key: slices.Clone(entries[pivot].key)}
	left.entries = entries[:pivot:pivot]
	right.entries = entries[pivot+1 : len(entries) : len(entries)]
	left.Children = children[: pivot+1 : pivot+1]
	right.Children = children[pivot+1 : len(children) : len(children)]
	left.encodedSize, right.encodedSize, parent.encodedSize = 0, 0, 0

	return true
}

// splitBranch removes the promoted separator and moves its right children together.
func (n *Node) splitBranch(right *Node, index int) {
	n.encodedSize, right.encodedSize = 0, 0
	right.entries = slices.Clone(n.entries[index+1:])
	n.entries = n.entries[:index]
	right.Children = slices.Clone(n.Children[index+1:])
	n.Children = n.Children[:index+1]
}

// visitBranchReferences visits each child page of a decoded branch.
func visitBranchReferences(node *Node, visit func(page.ID) error) error {
	for _, child := range node.Children {
		if err := visit(child); err != nil {
			return err
		}
	}
	return nil
}
