package btree

import (
	"errors"
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
	LookupPage(page.ID, []byte) (Entry, bool, page.ID, error)
	// PageSize returns the fixed encoded size of one tree page.
	PageSize() int64
	// ReadNode returns the node visible to the current operation.
	// A returned node is read-only until [Store.WritableNode] returns a private version.
	ReadNode(page.ID) (*Node, error)
	// WritableNode returns a private mutable version of node with the same page ID.
	// Repeated calls for that page must return the same private node.
	WritableNode(*Node) *Node
	// AllocatePage returns a page ID that is not in use by another node.
	// It returns an error when the store cannot allocate a page.
	AllocatePage() (page.ID, error)
	// FreePage retires a removed tree page.
	FreePage(page.ID) error
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

// LookupNode searches one decoded node.
// A leaf returns an entry and a zero child page ID.
// A branch returns the selected child page ID.
// The caller must validate key.
func LookupNode(node *Node, key []byte) (Entry, bool, page.ID, error) {
	if node.IsLeaf() {
		entry, found, err := node.FindEntryRef(key)
		return entry, found, 0, err
	}
	childIndex, err := node.FindChildIndex(key)
	if err != nil {
		return Entry{}, false, 0, err
	}
	childPageID := node.Children[childIndex]
	if childPageID == page.Meta0ID {
		return Entry{}, false, 0, fmt.Errorf("read node child page ID %d: %w", childPageID, ErrInvalid)
	}
	return Entry{}, false, childPageID, nil
}

// LookupDecodedNode validates key and searches one decoded node.
func LookupDecodedNode(node *Node, key []byte) (Entry, bool, page.ID, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, 0, err
	}
	return LookupNode(node, key)
}

// validateEntry rejects an empty or oversized key, an oversized value, or an entry that cannot fit the tree's fixed-page representation.
// A leaf stores the full key and value.
// A later split can promote the key into a branch separator, where the smallest valid branch stores the separator and two child page IDs.
// Both forms must fit before insertion starts.
func (tree *Tree) validateEntry(entry Entry) error {
	if err := validateLookupKey(entry.Key()); err != nil {
		return err
	}
	if len(entry.Value()) > MaxValueSize {
		return ErrValueTooLarge
	}
	pageSize := int(tree.store.PageSize())

	leafSize := NodeHeaderSize + entry.EncodedSize(true)
	// A one-entry branch stores two child page IDs.
	branchSize := NodeHeaderSize + page.IDSize + entry.EncodedSize(false)
	if leafSize > pageSize || branchSize > pageSize {
		return fmt.Errorf("%w: key %q, page size %d bytes", ErrEntryTooLarge, entry.Key(), tree.store.PageSize())
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

// splitChildIntoSiblings splits one child until each result fits in one page.
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

// FindEntryRef looks key up without copying the stored key or value.
// It returns a zero entry with found false when the key is absent.
// A found entry refers to read-only storage owned by root or the store. It must not outlive its owner.
// An empty or oversized key returns [ErrKeyRequired] or [ErrKeyTooLarge].
func (tree *Tree) FindEntryRef(root *Node, key []byte) (Entry, bool, error) {
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
	return tree.findEntryRefFromPage(childPageID, key)
}

// FindEntryRefFromPage starts at rootPageID without requiring a root Node.
// A found entry refers to storage owned by the store.
func (tree *Tree) FindEntryRefFromPage(rootPageID page.ID, key []byte) (Entry, bool, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, err
	}
	return tree.findEntryRefFromPage(rootPageID, key)
}

func (tree *Tree) findEntryRefFromPage(pageID page.ID, key []byte) (Entry, bool, error) {
	for {
		entry, found, childPageID, err := tree.store.LookupPage(pageID, key)
		if err != nil {
			return Entry{}, false, err
		}
		if childPageID == 0 {
			return entry, found, nil
		}
		pageID = childPageID
	}
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

	deleteIndex, found, err := node.prepareDelete(key)
	if err != nil || !found {
		return root, found, err
	}

	leaf := tree.store.WritableNode(node)
	leaf.applyDelete(deleteIndex)
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
		if int64(node.EncodedSize()) >= minimumSize {
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
		// bytes because entries can have different sizes.
		if left != nil && mergedChildrenSize(parent, step.childIndex-1, left, node) <= pageSize {
			left = tree.store.WritableNode(left)
			mergeChildren(parent, step.childIndex-1, left, node)
			if err := tree.store.FreePage(node.PageID()); err != nil {
				return nil, err
			}
			node = parent
			// The left page stays in the tree. It still holds the parent's
			// first key, so no earlier parent needs a new key.
			minimumChanged = false
		} else if right != nil && mergedChildrenSize(parent, step.childIndex, node, right) <= pageSize {
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
					return nil, errors.New("selected right redistribution is not valid")
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
				return nil, errors.New("selected left redistribution is not valid")
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

// redistributionScore describes the two pages we would get by moving entries
// between neighbors. The score is valid only if both pages fit. underfull says
// how many more bytes the pages need to reach the target size. imbalance says
// how far apart their sizes are. Smaller values are better.
type redistributionScore struct {
	underfull int64 // for nuking the Node and putting it's content in the left/right nodes. underfull is "how much of the left/right node's size remains to reach the 25% target (a quarter of a page's size)"
	imbalance int64 // for NOT nuking the Node, and taking some of the left/right node's content into the Node. imbalance is "if we were to distribute some of the left/right node's content into the Node, what's the difference would be between the left/right size and the Node's size?"
	valid     bool
}

// chooseSiblingRedistribution decides which neighbor should share entries with
// node. It tries left with node and node with right on copies. The pages stay
// unchanged until the caller makes the chosen move.
//
// First prefer the pair that leaves fewer bytes below the target size. If the
// pairs tie, prefer the pair with sizes closer together. An exact tie selects
// left. ok is false if neither pair can make two pages that fit.
func chooseSiblingRedistribution(parent *Node, childIndex int, left, node, right *Node, pageSize, minimumSize int64) (useLeft, ok bool) {
	leftScore := redistributionScore{}
	if left != nil {
		leftScore = scoreRedistribution(parent, childIndex-1, left, node, pageSize, minimumSize)
	}
	rightScore := redistributionScore{}
	if right != nil {
		rightScore = scoreRedistribution(parent, childIndex, node, right, pageSize, minimumSize)
	}

	if !leftScore.valid {
		return false, rightScore.valid
	}
	if !rightScore.valid {
		return true, true
	}

	if rightScore.underfull < leftScore.underfull ||
		rightScore.underfull == leftScore.underfull && rightScore.imbalance < leftScore.imbalance {
		return false, true
	}
	return true, true
}

// scoreRedistribution tries one move on copies and measures the result.
// The real pages must stay unchanged while we compare the two neighbors.
// separatorIndex selects the parent key between left and right.
func scoreRedistribution(parent *Node, separatorIndex int, left, right *Node, pageSize, minimumSize int64) redistributionScore {
	parent = parent.Clone()
	left = left.Clone()
	right = right.Clone()
	if !redistributeChildren(parent, separatorIndex, left, right, pageSize, minimumSize) {
		return redistributionScore{}
	}
	underfull, imbalance := redistributionPenalty(int64(left.EncodedSize()), int64(right.EncodedSize()), minimumSize)
	return redistributionScore{underfull: underfull, imbalance: imbalance, valid: true}
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

// mergedChildrenSize checks if left can hold all of right before a merge.
// It counts the bytes that the merged node would write. It changes no node.
// For leaves, the parent key already occurs in right. For branches, the parent
// key moves into the merged node, along with the child page IDs.
func mergedChildrenSize(parent *Node, separatorIndex int, left, right *Node) int64 {
	size := NodeHeaderSize
	for _, entry := range left.entries {
		size += entry.EncodedSize(left.IsLeaf())
	}
	for _, entry := range right.entries {
		size += entry.EncodedSize(right.IsLeaf())
	}
	if !left.IsLeaf() {
		// Each branch entry counts one child ID. A branch has one more child
		// than entries, so count that last ID as well.
		size += parent.entries[separatorIndex].EncodedSize(false)
		size += page.IDSize
	}
	return int64(size)
}

// redistributeChildren moves entries between two adjacent pages that cannot
// merge. It chooses a place to divide their entries, then changes both pages
// and the parent key between them. It returns false without changes if no
// division makes two pages that fit.
//
// For leaves, the parent must store the first key in the new right page.
// For branches, the old parent key goes between the two entry lists. The chosen
// dividing key then moves from the list into the parent.
func redistributeChildren(parent *Node, separatorIndex int, left, right *Node, pageSize, minimumSize int64) bool {
	if left.IsLeaf() {
		entries := make([]Entry, 0, len(left.entries)+len(right.entries))
		entries = append(entries, left.entries...)
		entries = append(entries, right.entries...)
		split := chooseLeafRedistribution(entries, pageSize, minimumSize)
		if split < 0 {
			return false
		}
		// Both pages share this new array. Keep an append to one page from
		// changing entries in the other page.
		left.entries = entries[:split:split]
		right.entries = entries[split:len(entries):len(entries)]
		parent.entries[separatorIndex] = Entry{key: slices.Clone(right.entries[0].key)}
	} else {
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
	}
	// These three pages now have different bytes. Their old checksums no
	// longer match them.
	left.header.Checksum = 0
	right.header.Checksum = 0
	parent.header.Checksum = 0
	return true
}

// chooseLeafRedistribution finds where to divide the entries between two leaves.
// It tries each place between keys. Each result must keep a key and fit in one
// page. It first prefers the fewest bytes below the target across both pages.
// It then prefers pages whose sizes are close. It returns -1 if no place works.
func chooseLeafRedistribution(entries []Entry, pageSize, minimumSize int64) int {
	if len(entries) < 2 {
		return -1
	}

	// Keys and values can have different sizes. prefix[i] counts the bytes
	// before entry i, so each possible division needs only two counts.
	prefix := make([]int64, len(entries)+1)
	for index := range entries {
		prefix[index+1] = prefix[index] + int64(entries[index].EncodedSize(true))
	}
	best := -1
	bestUnderfull, bestImbalance := int64(^uint64(0)>>1), int64(^uint64(0)>>1)
	for split := 1; split < len(entries); split++ {
		leftSize := int64(NodeHeaderSize) + prefix[split]
		rightSize := int64(NodeHeaderSize) + prefix[len(entries)] - prefix[split]
		if leftSize > pageSize || rightSize > pageSize {
			continue
		}
		underfull, imbalance := redistributionPenalty(leftSize, rightSize, minimumSize)
		if underfull < bestUnderfull || underfull == bestUnderfull && imbalance < bestImbalance {
			best, bestUnderfull, bestImbalance = split, underfull, imbalance
		}
	}
	return best
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

// redistributionPenalty measures how well two pages use their space.
// For a target of 64 bytes, pages of 40 and 80 bytes miss the target by 24
// bytes in total. Their sizes differ by 40 bytes. Callers compare the first
// result before the second, so they first try to make both pages less empty.
func redistributionPenalty(leftSize, rightSize, minimumSize int64) (underfull, imbalance int64) {
	underfull = max(minimumSize-leftSize, 0) + max(minimumSize-rightSize, 0)
	return underfull, max(leftSize-rightSize, rightSize-leftSize)
}

// mergeChildren puts right into left and removes right from their parent.
// The caller first checks that all data will fit in left. After this call,
// the caller returns right's page to the store.
//
// For leaves, the parent's key is already the first key in right. For
// branches, that key exists only in the parent. When branches join, the key
// moves down between their two groups of keys.
func mergeChildren(parent *Node, separatorIndex int, left, right *Node) {
	if left.IsLeaf() {
		left.entries = append(left.entries, right.entries...)
	} else {
		left.entries = append(left.entries, parent.entries[separatorIndex])
		left.entries = append(left.entries, right.entries...)
		left.Children = append(left.Children, right.Children...)
	}
	// The parent no longer needs a key or a child ID for the old right page.
	parent.entries = slices.Delete(parent.entries, separatorIndex, separatorIndex+1)
	removedChild := separatorIndex + 1
	parent.Children = slices.Delete(parent.Children, removedChild, removedChild+1)
	// Only the pages that stay in the tree need their checksums cleared.
	left.header.Checksum = 0
	parent.header.Checksum = 0
}

// NeedsSplit reports whether the encoded node is larger than one page.
func (n *Node) NeedsSplit(pageSize int64) bool {
	return int64(n.EncodedSize()) > pageSize
}

// EncodedSize reports the exact number of bytes that [EncodeNode] writes for n.
func (n *Node) EncodedSize() int {
	return NodeHeaderSize + nodePayloadSize(n)
}

// chooseSplitIndex selects a separator near half a page based on encoded entry bytes while preserving valid left and right node shapes.
// A leaf keeps its separator as the first entry of the right node, while a branch promotes and removes its separator.
func (n *Node) chooseSplitIndex(pageSize int64) (int, error) {
	limit := int(pageSize / 2)
	currSize := 0
	separatorIndex := -1

	for i, entry := range n.entries {
		currSize += entry.EncodedSize(n.IsLeaf())
		if currSize >= limit {
			separatorIndex = i
			break
		}
	}

	if separatorIndex == -1 {
		return -1, ErrNodeNotSaturated
	}

	// The leaf separator remains in the right node, so each side must keep at least one entry.
	if n.IsLeaf() {
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
		header:   newNodeHeader(n.header.Type, newPageID),
		Children: []page.ID{},
	}

	separator := n.entries[separatorIndex].key
	if n.IsLeaf() {
		rightNode.entries = slices.Clone(n.entries[separatorIndex:])
		n.entries = n.entries[:separatorIndex]
	} else {
		rightNode.entries = slices.Clone(n.entries[separatorIndex+1:])
		n.entries = n.entries[:separatorIndex]
	}

	if !n.IsLeaf() {
		rightNode.Children = slices.Clone(n.Children[separatorIndex+1:])
		n.Children = n.Children[:separatorIndex+1]
	}
	n.header.Checksum = 0

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
	parent.Children[leftIndex+1] = rightNode.PageID()
	parent.header.Checksum = 0
}
