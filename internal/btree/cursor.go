package btree

// cursorFrame records one branch-to-child move in a cursor path.
// A branch is a node that points to child nodes.
// A leaf is a node that stores ordered entries.
// The cursor stores frames in root-to-leaf order.
// It uses the child indexes to find the next or previous subtree without searching from the root again.
type cursorFrame struct {
	node       *Node // node is the branch that contains the selected child.
	childIndex int   // childIndex is the selected index in node.Children.
}

// Cursor reads entries in key order without changing the tree.
// It saves each branch-to-child move from the root to the current leaf.
// [Cursor.Next] and [Cursor.Prev] use this path when they reach a leaf boundary.
// This avoids a new root search for each entry.
//
// Create a Cursor with [Tree.Cursor].
// The zero value is not valid.
// A new cursor has no current entry until [Cursor.First], [Cursor.Last], or [Cursor.Seek] finds one.
// Do not copy a cursor after a positioning method uses it because a copy can share the saved path.
// Use a cursor from one goroutine at a time.
//
// For example, a cursor can reach one leaf through child 1 of the root and then child 2 of the next branch.
// Its path contains those two branch choices.
//
// Returned entries refer to node storage.
// The caller must not change the root or any node that the cursor can reach while the cursor is in use.
type Cursor struct {
	tree       *Tree         // tree reads child nodes through the store for this operation.
	root       *Node         // root is the fixed start of every new cursor search.
	path       []cursorFrame // path contains the branch choices from root to leaf.
	leaf       *Node         // leaf contains the current entry. A nil leaf means that the cursor has no current entry.
	entryIndex int           // entryIndex selects the current entry when leaf is not nil.
}

// Cursor returns an unpositioned cursor that reads the tree below root.
// The caller must pass a non-nil root that belongs to tree.
// The caller must keep root and every node returned by the tree store unchanged while it uses the cursor.
func (tree *Tree) Cursor(root *Node) *Cursor {
	// Leave the path and leaf empty until a positioning method selects an entry.
	return &Cursor{tree: tree, root: root}
}

// First moves to the entry with the smallest key.
// It replaces any earlier cursor position.
// First returns a nil entry and false when the tree is empty.
// It returns a store error if it cannot read a child node.
func (cursor *Cursor) First() (*Entry, bool, error) {
	// A search from the root makes every branch choice from the old position invalid.
	cursor.path = cursor.path[:0]

	// The smallest key is in the leaf reached through the first child of each branch.
	leaf, err := cursor.descendFirst(cursor.root)
	if err != nil {
		return nil, false, err
	}

	// Entry zero is the smallest entry in the leaf. position handles an empty leaf.
	return cursor.position(leaf, 0)
}

// Last moves to the entry with the largest key.
// It replaces any earlier cursor position.
// Last returns a nil entry and false when the tree is empty.
// It returns a store error if it cannot read a child node.
func (cursor *Cursor) Last() (*Entry, bool, error) {
	// A search from the root makes every branch choice from the old position invalid.
	cursor.path = cursor.path[:0]

	// The largest key is in the leaf reached through the last child of each branch.
	leaf, err := cursor.descendLast(cursor.root)
	if err != nil {
		return nil, false, err
	}

	// The final leaf entry is the largest entry in the tree. position handles an empty leaf.
	return cursor.position(leaf, len(leaf.entries)-1)
}

// Seek moves to the first entry whose key is equal to or greater than target.
// It replaces any earlier cursor position.
// A nil or empty target moves to the smallest key.
// Seek returns a nil entry and false when target sorts after every key.
// It returns a store error if it cannot read a child node.
func (cursor *Cursor) Seek(target []byte) (*Entry, bool, error) {
	// Seek starts at the root, so no branch choice from the old position can remain.
	cursor.path = cursor.path[:0]
	node := cursor.root

	// Follow the separator keys until node is the only leaf that can contain target.
	for !node.IsLeaf() {
		childIndex, err := node.FindChildIndex(target)
		if err != nil {
			return nil, false, err
		}

		// Save this branch choice before the descent. A later boundary move uses it to find an adjacent subtree.
		cursor.path = append(cursor.path, cursorFrame{node: node, childIndex: childIndex})
		node, err = cursor.readChild(node, childIndex)
		if err != nil {
			return nil, false, err
		}
	}

	// Leaf entries are sorted. findKeyIndex returns the first index whose key is not less than target.
	// An exact-match result is not needed because Seek accepts both an equal key and the next greater key.
	entryIndex, _, err := node.findKeyIndex(target)
	if err != nil {
		return nil, false, err
	}
	if entryIndex < len(node.entries) {
		return cursor.position(node, entryIndex)
	}

	// target sorts after this leaf. Use the saved path to find the first entry in the next leaf.
	cursor.leaf = nil
	return cursor.moveToNextLeaf()
}

// Next moves to the next entry in key order.
// It returns a nil entry and false when the cursor has no current entry or is at the end of the tree.
// It returns a store error if it must change leaves and cannot read a child node.
func (cursor *Cursor) Next() (*Entry, bool, error) {
	// A new cursor and an exhausted cursor have no entry from which to move.
	if cursor.leaf == nil {
		return nil, false, nil
	}

	// When the leaf has another entry, only the entry index must change.
	nextIndex := cursor.entryIndex + 1
	if nextIndex < len(cursor.leaf.entries) {
		cursor.entryIndex = nextIndex
		return &cursor.leaf.entries[nextIndex], true, nil
	}

	// The leaf has no later entry. The saved branch path identifies the next leaf.
	return cursor.moveToNextLeaf()
}

// Prev moves to the previous entry in key order.
// It returns a nil entry and false when the cursor has no current entry or is at the start of the tree.
// It returns a store error if it must change leaves and cannot read a child node.
func (cursor *Cursor) Prev() (*Entry, bool, error) {
	// A new cursor and an exhausted cursor have no entry from which to move.
	if cursor.leaf == nil {
		return nil, false, nil
	}

	// When the leaf has an earlier entry, only the entry index must change.
	if cursor.entryIndex > 0 {
		cursor.entryIndex--
		return &cursor.leaf.entries[cursor.entryIndex], true, nil
	}

	// The leaf has no earlier entry. The saved branch path identifies the previous leaf.
	return cursor.moveToPreviousLeaf()
}

// position selects entryIndex in leaf as the current cursor entry.
// It returns a nil entry and clears the current position when entryIndex is outside the leaf.
// This boundary rule handles an empty tree for First and Last.
func (cursor *Cursor) position(leaf *Node, entryIndex int) (*Entry, bool, error) {
	if entryIndex < 0 || entryIndex >= len(leaf.entries) {
		// A nil leaf is the single marker for a cursor that has no current entry.
		cursor.leaf = nil
		return nil, false, nil
	}

	// Publish the leaf and index only after the bounds check makes both values safe for current to use.
	cursor.leaf = leaf
	cursor.entryIndex = entryIndex
	return &leaf.entries[entryIndex], true, nil
}

// readChild returns the child selected by childIndex in parent.
// It reads through the tree store so the cursor sees the node version for the current operation.
// It returns [ErrKeyNotFound] when the selected page ID has no node.
// The caller must pass a branch node and a valid child index.
func (cursor *Cursor) readChild(parent *Node, childIndex int) (*Node, error) {
	// A branch stores page IDs instead of direct child pointers. Resolve the selected page through the store.
	child, err := cursor.tree.store.ReadNode(parent.Children[childIndex])
	if err != nil {
		return nil, err
	}
	if child == nil {
		// A branch that points to no node is an invalid tree, not the normal absence of a user key.
		return nil, ErrKeyNotFound
	}
	return child, nil
}

// descendFirst follows the first child of each branch until it reaches a leaf.
// It appends each branch choice to the existing cursor path.
// This lets First build a path from the root and lets a boundary move extend a retained path.
func (cursor *Cursor) descendFirst(node *Node) (*Node, error) {
	for !node.IsLeaf() {
		// Child zero contains the smallest keys below this branch.
		// Save the choice before reading the child so the complete path reaches the returned leaf.
		cursor.path = append(cursor.path, cursorFrame{node: node, childIndex: 0})
		var err error
		node, err = cursor.readChild(node, 0)
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

// descendLast follows the last child of each branch until it reaches a leaf.
// It appends each branch choice to the existing cursor path.
// This lets Last build a path from the root and lets a boundary move extend a retained path.
func (cursor *Cursor) descendLast(node *Node) (*Node, error) {
	for !node.IsLeaf() {
		// The final child contains the largest keys below this branch.
		childIndex := len(node.Children) - 1
		// Save the choice before reading the child so the complete path reaches the returned leaf.
		cursor.path = append(cursor.path, cursorFrame{node: node, childIndex: childIndex})
		var err error
		node, err = cursor.readChild(node, childIndex)
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

// moveToNextLeaf moves to the first entry in the next leaf.
// It searches the saved path from the current leaf toward the root.
// The first branch with a later child is the nearest subtree that can contain a greater key.
// It returns a nil entry and clears the current position when no later subtree exists.
func (cursor *Cursor) moveToNextLeaf() (*Entry, bool, error) {
	// Start with the parent nearest the leaf. It is the first place where a later subtree can exist.
	for index := len(cursor.path) - 1; index >= 0; index-- {
		frame := &cursor.path[index]
		if frame.childIndex+1 >= len(frame.node.Children) {
			// This branch has no later child. Continue with its parent.
			continue
		}

		// Select the next child at this branch. Every deeper frame still describes the old subtree and must be removed.
		frame.childIndex++
		cursor.path = cursor.path[:index+1]
		node, err := cursor.readChild(frame.node, frame.childIndex)
		if err != nil {
			return nil, false, err
		}

		// The first leaf below the selected subtree contains its smallest key. That key is the next key in the tree.
		leaf, err := cursor.descendFirst(node)
		if err != nil {
			return nil, false, err
		}
		return cursor.position(leaf, 0)
	}

	// Every saved branch selected its final child, so the cursor was at the largest key in the tree.
	cursor.leaf = nil
	return nil, false, nil
}

// moveToPreviousLeaf moves to the last entry in the previous leaf.
// It searches the saved path from the current leaf toward the root.
// The first branch with an earlier child is the nearest subtree that can contain a smaller key.
// It returns a nil entry and clears the current position when no earlier subtree exists.
func (cursor *Cursor) moveToPreviousLeaf() (*Entry, bool, error) {
	// Start with the parent nearest the leaf. It is the first place where an earlier subtree can exist.
	for index := len(cursor.path) - 1; index >= 0; index-- {
		frame := &cursor.path[index]
		if frame.childIndex == 0 {
			// This branch has no earlier child. Continue with its parent.
			continue
		}

		// Select the previous child at this branch. Every deeper frame still describes the old subtree and must be removed.
		frame.childIndex--
		cursor.path = cursor.path[:index+1]
		node, err := cursor.readChild(frame.node, frame.childIndex)
		if err != nil {
			return nil, false, err
		}

		// The last leaf below the selected subtree contains its largest key. That key is the previous key in the tree.
		leaf, err := cursor.descendLast(node)
		if err != nil {
			return nil, false, err
		}
		return cursor.position(leaf, leaf.EntryCount()-1)
	}

	// Every saved branch selected its first child, so the cursor was at the smallest key in the tree.
	cursor.leaf = nil
	return nil, false, nil
}
