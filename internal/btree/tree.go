package btree

import (
	"bytes"
	"fmt"
	"maps"
	"slices"

	"github.com/Issaminu/kvlite/internal/page"
)

// Store supplies transaction-visible nodes and owns permission to edit them.
type Store interface {
	NodeReader
	// PageSize returns the physical storage page size in bytes.
	PageSize() int64
	// Writable reports whether this transaction permits node and allocation changes.
	Writable() bool
	// EditNode returns a private writable node. Repeated edits reuse it.
	// A read-only transaction returns an error.
	EditNode(page.ID) (*MutableNode, error)
	// AllocatePage returns a page ID that no other node uses.
	// It returns an error when the store cannot allocate a page.
	AllocatePage() (page.ID, error)
	// FreePage retires a removed tree node and all of its continuation pages.
	FreePage(page.ID) error
	// StageNode publishes a newly created private node inside the transaction.
	StageNode(*MutableNode)
}

// Tree performs B+tree lookup, insertion, deletion, and repair through a [Store].
// It owns traversal paths and parent links; Store owns node versions and write permission.
type Tree struct {
	store         Store
	parents       map[page.ID]page.ID
	parentsShared bool
	readOnly      bool

	// SkipIdenticalPuts keeps an identical entry clean when the caller requires no durable rewrite.
	SkipIdenticalPuts bool
}

// treePathStep records one move from a branch node to one child node.
// parent is the branch node at the current level.
// childIndex selects the child of parent that the path follows.
type treePathStep struct {
	parent     Node
	childIndex int
}

// NewTree returns a tree that uses store for every node read, write, and page allocation.
func NewTree(store Store) *Tree {
	return &Tree{store: store, readOnly: !store.Writable()}
}

// Fork starts an independent tree traversal over a new transaction store.
// Parent links remain shared until the first change, so a failed batch callback
// cannot change the links retained by an earlier successful callback.
func (tree *Tree) Fork(store Store) Tree {
	return Tree{store: store, parents: tree.parents, parentsShared: true, SkipIdenticalPuts: tree.SkipIdenticalPuts, readOnly: !store.Writable()}
}

func (tree *Tree) setParent(child page.ID, parent page.ID) {
	if tree.readOnly {
		return
	}
	if previous, ok := tree.parents[child]; ok && previous == parent {
		return
	}
	if tree.parents == nil {
		tree.parents = make(map[page.ID]page.ID)
	} else if tree.parentsShared {
		copied := make(map[page.ID]page.ID, len(tree.parents)+1)
		maps.Copy(copied, tree.parents)
		tree.parents = copied
	}
	tree.parentsShared = false
	tree.parents[child] = parent
}

// Parent returns the saved owner of id. Zero identifies the catalog root.
func (tree *Tree) Parent(id page.ID) (page.ID, bool) {
	parent, ok := tree.parents[id]
	return parent, ok
}

func (tree *Tree) readChild(parent Node, index int) (Node, error) {
	id, err := parent.Child(index)
	if err != nil {
		return Node{}, err
	}
	child, err := tree.store.ReadNode(id)
	if err == nil {
		tree.setParent(id, parent.PageID())
	}
	return child, err
}

func (tree *Tree) reparentChildren(parent *MutableNode) error {
	return VisitNodeReferences(parent, func(child page.ID) error {
		tree.setParent(child, parent.PageID())
		return nil
	}, nil)
}

func (tree *Tree) recordBucketParent(parent page.ID, entry Entry) error {
	if entry.Flags()&BucketLeafFlag == 0 {
		return nil
	}
	id, err := page.DecodeID(entry.Value())
	if err != nil {
		return err
	}
	tree.setParent(id, parent)
	return nil
}

// FindEntryRef searches the tree at root without cursor state.
// Returned bytes are read-only and borrow the transaction. includeKey allows
// an encoded leaf to omit the matched key. A missing key returns found false.
// An empty or oversized key returns [ErrKeyRequired] or [ErrKeyTooLarge].
func (tree *Tree) FindEntryRef(root page.ID, key []byte, includeKey bool) (Entry, bool, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, err
	}
	for {
		node, err := tree.store.ReadNode(root)
		if err != nil {
			return Entry{}, false, err
		}
		entry, found, child, err := node.Lookup(key, includeKey)
		if err != nil {
			return Entry{}, false, err
		}
		if child == 0 {
			if found {
				err = tree.recordBucketParent(root, entry)
			}
			return entry, found, err
		}
		tree.setParent(child, root)
		root = child
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

// PutEntry inserts or replaces entry and returns the resulting root page.
// It checks the entry limits and leaf conflicts before it asks the store for a writable node.
// The store keeps all edits private until the enclosing transaction commits.
// If page allocation fails after a node changes, the caller must discard its private transaction state.
func (tree *Tree) PutEntry(root page.ID, entry Entry) (page.ID, error) {
	if err := tree.validateEntry(entry); err != nil {
		return 0, err
	}
	node, path, err := tree.findLeaf(root, entry.Key())
	if err != nil {
		return 0, err
	}
	existing, found, _, err := node.Lookup(entry.Key(), true)
	if err != nil {
		return 0, err
	}
	if found {
		if existing.flags&BucketLeafFlag != entry.flags&BucketLeafFlag {
			return 0, ErrIncompatibleValue
		}
		if tree.SkipIdenticalPuts && existing.flags == entry.flags && bytes.Equal(existing.value, entry.value) {
			return root, nil
		}
	}
	// A large one-entry leaf need not change when a new neighboring key arrives.
	// This is a tree split decision independent of how either node is stored.
	if !found && entry.flags == 0 && node.EntryCount() == 1 {
		old, err := node.Entry(0)
		if err != nil {
			return 0, err
		}
		if old.flags == 0 && int64(len(old.value)) > tree.store.PageSize() {
			return tree.insertNeighbor(root, node, old, entry, path)
		}
	}
	private, err := tree.store.EditNode(node.PageID())
	if err != nil {
		return 0, err
	}
	prepared, err := private.prepareInsert(entry)
	if err != nil {
		return 0, err
	}
	private.applyInsert(prepared)
	if err := tree.recordBucketParent(private.PageID(), entry); err != nil {
		return 0, err
	}
	return tree.finishPut(root, private, path, !prepared.found && prepared.index == private.EntryCount()-1)
}

// insertNeighbor puts entry in a new leaf beside node, which holds only the large entry old.
// node does not change, so its large value is not copied or rewritten.
// The new leaf goes before node when entry sorts first. The parent gets a separator for the new boundary.
func (tree *Tree) insertNeighbor(root page.ID, node Node, old, entry Entry, path []treePathStep) (page.ID, error) {
	id, err := tree.store.AllocatePage()
	if err != nil {
		return 0, err
	}
	neighbor := NewLeafNode(id)
	if err := neighbor.InsertEntry(entry); err != nil {
		return 0, err
	}
	tree.store.StageNode(neighbor)
	before := bytes.Compare(entry.key, old.key) < 0
	separator := neighbor.EntryAt(0).Key()
	if before {
		separator = old.key
	}
	if len(path) == 0 {
		rootID, err := tree.store.AllocatePage()
		if err != nil {
			return 0, err
		}
		left, right := node.PageID(), neighbor.PageID()
		if before {
			left, right = right, left
		}
		parent := NewRootNode(rootID, left, right, separator)
		tree.store.StageNode(parent)
		tree.setParent(left, rootID)
		tree.setParent(right, rootID)
		return rootID, nil
	}
	step := path[len(path)-1]
	parent, err := tree.store.EditNode(step.parent.PageID())
	if err != nil {
		return 0, err
	}
	if before {
		parent.Children[step.childIndex] = neighbor.PageID()
		parent.InsertSplitChild(step.childIndex, node.PageID(), separator)
	} else {
		parent.InsertSplitChild(step.childIndex, neighbor.PageID(), separator)
	}
	tree.setParent(node.PageID(), parent.PageID())
	tree.setParent(neighbor.PageID(), parent.PageID())
	return tree.finishPut(root, parent, path[:len(path)-1], false)
}

// finishPut splits the changed node and its ancestors as needed.
func (tree *Tree) finishPut(root page.ID, node *MutableNode, path []treePathStep, tailInsert bool) (page.ID, error) {
	pageSize := tree.store.PageSize()
	if tailInsert && node.NeedsSplit(pageSize) {
		for _, step := range path {
			if step.childIndex != step.parent.EntryCount() {
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
				return 0, err
			}
			rootPageID, err := tree.store.AllocatePage()
			if err != nil {
				return 0, err
			}
			parent := NewRootNode(rootPageID, node.PageID(), rightNode.PageID(), separator)
			root = rootPageID
			// node is already staged. Only the new right node and new root need staging.
			tree.store.StageNode(rightNode)
			tree.store.StageNode(parent)
			tree.setParent(node.PageID(), root)
			tree.setParent(rightNode.PageID(), root)
			// rightNode starts at root.Children[1].
			// Split it again if one split did not make it small enough.
			if err := tree.splitChildIntoSiblings(parent, rightNode, 1, pageSize, false); err != nil {
				return 0, err
			}
			node = parent
			continue
		}

		// The last path item contains the direct parent of node.
		// Remove it because the next pass can continue with that parent.
		lastStep := len(path) - 1
		parentStep := path[lastStep]
		path = path[:lastStep]

		// A child split adds a separator and a child page ID to its parent.
		// EditNode returns a staged private parent that this operation can change.
		parent, err := tree.store.EditNode(parentStep.parent.PageID())
		if err != nil {
			return 0, err
		}
		// node is at parent.Children[parentStep.childIndex].
		// Keep its left result in that slot and insert each new right sibling after it.
		if err := tree.splitChildIntoSiblings(parent, node, parentStep.childIndex, pageSize, tailInsert); err != nil {
			return 0, err
		}
		// New separators can make the parent too large, so check the parent next.
		node = parent
	}
	return root, nil
}

// splitNode splits a private node that is known to be larger than one page.
// It returns a page allocation error. Entry validation and the NeedsSplit check guarantee that the node can split, so a Split error is an internal tree invariant failure.
func (tree *Tree) splitNode(node *MutableNode, pageSize int64, tailInsert bool) (*MutableNode, []byte, error) {
	pageID, err := tree.store.AllocatePage()
	if err != nil {
		return nil, nil, err
	}
	var rightNode *MutableNode
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
func (tree *Tree) splitChildIntoSiblings(parent, child *MutableNode, childIndex int, pageSize int64, tailInsert bool) error {
	for child.NeedsSplit(pageSize) {
		rightNode, separator, err := tree.splitNode(child, pageSize, tailInsert)
		if err != nil {
			return err
		}
		tree.store.StageNode(rightNode)
		parent.InsertSplitChild(childIndex, rightNode.PageID(), separator)
		tree.setParent(rightNode.PageID(), parent.PageID())
		// If the right half is still too large, split it at its new index.
		child = rightNode
		childIndex++
	}
	return nil
}

// DeleteEntry removes one non-bucket entry from the tree rooted at root.
// It returns the replacement root and true when it removes the key.
// root must identify a node in this tree's store.
// A missing key returns root, false, and nil without changing the tree.
// An empty or oversized key returns [ErrKeyRequired] or [ErrKeyTooLarge].
// A key that identifies a nested bucket returns [ErrIncompatibleValue].
// A successful delete can replace the root and retire unused pages through
// [Store.FreePage]. If a store operation fails after mutation starts, the
// caller must discard the private store changes.
func (tree *Tree) DeleteEntry(root page.ID, key []byte) (page.ID, bool, error) {
	return tree.deleteEntry(root, key, false)
}

// DeleteEntries removes plain keys from one tree. It changes each leaf once
// for keys in that leaf and repairs the leaf before it moves to the next one.
// A missing key has no effect. Duplicate keys have the same effect as one key.
// If a key names a bucket, earlier leaves can already have changed.
// The caller must discard the transaction when it returns an error.
func (tree *Tree) DeleteEntries(root page.ID, keys [][]byte) (page.ID, bool, error) {
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
		node, path, err := tree.findLeaf(root, ordered[first])
		if err != nil {
			return 0, changed, err
		}
		var upper []byte
		for _, step := range slices.Backward(path) {
			if step.childIndex < step.parent.EntryCount() {
				upper, err = step.parent.separator(step.childIndex)
				if err != nil {
					return 0, changed, err
				}
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
			leaf, err := tree.store.EditNode(node.PageID())
			if err != nil {
				return 0, changed, err
			}
			leaf.DeleteLeafEntries(firstIndex, rest)
			root, err = tree.repairAfterDelete(root, leaf, path)
			if err != nil {
				return 0, changed, err
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
func (tree *Tree) DeleteBucketEntry(root page.ID, key []byte) (page.ID, bool, error) {
	return tree.deleteEntry(root, key, true)
}

func (tree *Tree) deleteEntry(root page.ID, key []byte, bucket bool) (page.ID, bool, error) {
	if len(key) == 0 {
		return 0, false, ErrKeyRequired
	}
	if len(key) > MaxKeySize {
		return 0, false, ErrKeyTooLarge
	}

	node, path, err := tree.findLeaf(root, key)
	if err != nil {
		return root, false, err
	}

	deleteIndex, found, err := node.prepareDelete(key, bucket)
	if err != nil || !found {
		return root, found, err
	}
	leaf, err := tree.store.EditNode(node.PageID())
	if err != nil {
		return 0, false, err
	}
	leaf.DeleteLeafEntries(deleteIndex, nil)

	// Retain valid lower boundaries and repair nodes that became under-full.
	root, err = tree.repairAfterDelete(root, leaf, path)
	if err != nil {
		return 0, false, err
	}
	return root, true, nil
}

// findLeaf returns the leaf for key and the branch path from root to it.
// It saves the parent of each node on that path, so that RepairMoved can find the parents later.
func (tree *Tree) findLeaf(root page.ID, key []byte) (Node, []treePathStep, error) {
	node, err := tree.store.ReadNode(root)
	if err != nil {
		return Node{}, nil, err
	}
	leaf, path, err := findLeaf(tree.store, node, key, nil)
	if err != nil {
		return Node{}, path, err
	}
	for index, step := range path {
		child := leaf.PageID()
		if index+1 < len(path) {
			child = path[index+1].parent.PageID()
		}
		tree.setParent(child, step.parent.PageID())
	}
	return leaf, path, nil
}

// repairAfterDelete merges or redistributes under-full nodes after a deletion.
// Separators are lower bounds: deleting a minimum key leaves its old boundary valid.
// Redistribution writes the boundary that moves. Merges carry a parent boundary
// into the combined branch, so they also preserve the same routing invariant.
func (tree *Tree) repairAfterDelete(root page.ID, node *MutableNode, path []treePathStep) (page.ID, error) {
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
		parent, err := tree.store.EditNode(step.parent.PageID())
		if err != nil {
			return 0, err
		}
		// Remove an empty leaf without decoding or changing its neighbor.
		if node.IsLeaf() && node.EntryCount() == 0 && len(parent.Children) > 1 {
			separatorIndex := max(step.childIndex-1, 0)
			parent.entries = slices.Delete(parent.entries, separatorIndex, separatorIndex+1)
			parent.Children = slices.Delete(parent.Children, step.childIndex, step.childIndex+1)
			parent.encodedSize = 0
			parent.header.Checksum = 0
			if err := tree.store.FreePage(node.PageID()); err != nil {
				return 0, err
			}
			node = parent
			continue
		}
		var left, right *MutableNode
		if step.childIndex > 0 {
			left, err = tree.readSibling(parent, step.childIndex-1)
			if err != nil {
				return 0, err
			}
		}
		if step.childIndex+1 < len(parent.Children) {
			right, err = tree.readSibling(parent, step.childIndex+1)
			if err != nil {
				return 0, err
			}
		}
		// Prefer a merge with the left neighbor when both fit. An empty branch must
		// merge even if the combined image needs more than one storage page.
		if left != nil && (node.EntryCount() == 0 || mergedChildrenSize(parent, step.childIndex-1, left, node) <= pageSize) {
			left, err = tree.store.EditNode(left.PageID())
			if err != nil {
				return 0, err
			}
			mergeChildren(parent, step.childIndex-1, left, node)
			if err := tree.reparentChildren(left); err != nil {
				return 0, err
			}
			if err := tree.store.FreePage(node.PageID()); err != nil {
				return 0, err
			}
			node = parent
		} else if right != nil && (node.EntryCount() == 0 || mergedChildrenSize(parent, step.childIndex, node, right) <= pageSize) {
			// A parent reached after a merge can still be a committed node.
			node, err = tree.store.EditNode(node.PageID())
			if err != nil {
				return 0, err
			}
			mergeChildren(parent, step.childIndex, node, right)
			if err := tree.reparentChildren(node); err != nil {
				return 0, err
			}
			if err := tree.store.FreePage(right.PageID()); err != nil {
				return 0, err
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
			node, err = tree.store.EditNode(node.PageID())
			if err != nil {
				return 0, err
			}
			if !useLeft {
				right, err = tree.store.EditNode(right.PageID())
				if err != nil {
					return 0, err
				}
				if !redistributeChildren(parent, step.childIndex, node, right, pageSize, minimumSize) {
					return 0, fmt.Errorf("selected right redistribution: %w", ErrRedistributionInvalid)
				}
				if err := tree.reparentChildren(node); err != nil {
					return 0, err
				}
				if err := tree.reparentChildren(right); err != nil {
					return 0, err
				}
			} else {
				left, err = tree.store.EditNode(left.PageID())
				if err != nil {
					return 0, err
				}
				if !redistributeChildren(parent, step.childIndex-1, left, node, pageSize, minimumSize) {
					return 0, fmt.Errorf("selected left redistribution: %w", ErrRedistributionInvalid)
				}
				if err := tree.reparentChildren(left); err != nil {
					return 0, err
				}
				if err := tree.reparentChildren(node); err != nil {
					return 0, err
				}
			}
			return root, nil
		}
	}
	// A branch with one child no longer needs to be a root.
	current, err := tree.store.ReadNode(root)
	if err != nil {
		return 0, err
	}
	if !current.IsLeaf() && current.EntryCount() == 0 {
		child, err := current.Child(0)
		if err != nil {
			return 0, err
		}
		if err := tree.store.FreePage(root); err != nil {
			return 0, err
		}
		root = child
		tree.setParent(root, 0)
	}
	return root, nil
}

// readSibling returns the decoded child at index for a merge or redistribution check.
// The result can borrow a committed node or an earlier callback's node, so the caller must not change it.
// To change a sibling, the caller first gets a private copy with [Store.EditNode].
func (tree *Tree) readSibling(parent *MutableNode, index int) (*MutableNode, error) {
	node, err := tree.readChild(parent.ReadOnly(), index)
	if err != nil {
		return nil, err
	}
	return node.decode(tree.store.PageSize())
}

// RepairMoved updates saved parent references after storage assigns new node heads.
// root identifies the catalog root; all other moved nodes must have a saved owner.
// It returns the relocated catalog root. The changes remain private in Store.
func (tree *Tree) RepairMoved(root page.ID, moved map[page.ID]page.ID) (page.ID, error) {
	parents := make([]page.ID, 0, len(moved))
	for old := range moved {
		parent, ok := tree.parents[old]
		if !ok || parent == 0 {
			if old != root {
				return 0, fmt.Errorf("moved node %d has no parent: %w", old, ErrInvalid)
			}
			continue
		}
		if replacement, ok := moved[parent]; ok {
			parent = replacement
		}
		parents = append(parents, parent)
	}
	slices.Sort(parents)
	for first := 0; first < len(parents); {
		last := first + 1
		for last < len(parents) && parents[last] == parents[first] {
			last++
		}
		parent, err := tree.store.EditNode(parents[first])
		if err != nil {
			return 0, err
		}
		repaired, err := parent.ReplaceReferences(moved)
		if err != nil {
			return 0, err
		}
		if repaired != last-first {
			return 0, fmt.Errorf("parent %d repaired %d moved children, want %d: %w", parent.PageID(), repaired, last-first, ErrInvalid)
		}
		first = last
	}
	// Apply the same relocation to the paths retained for subsequent preparation passes.
	for old, target := range moved {
		parent := tree.parents[old]
		if replacement, ok := moved[parent]; ok {
			parent = replacement
		}
		tree.setParent(target, parent)
	}
	for id, parent := range tree.parents {
		if replacement, ok := moved[parent]; ok {
			tree.setParent(id, replacement)
		}
	}
	if target, ok := moved[root]; ok {
		root = target
	}
	return root, nil
}
