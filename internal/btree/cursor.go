package btree

import (
	"bytes"

	"github.com/Issaminu/kvlite/internal/page"
)

// Cursor reads entries in key order without changing the tree.
// It saves each branch-to-child move from the root to the current leaf.
// [Cursor.Next] and [Cursor.Prev] use this path when they reach a leaf boundary.
// This avoids a new root search for each entry.
//
// Create a Cursor with [NewCursor].
// The zero value is not valid.
// A new cursor has no current entry until [Cursor.First], [Cursor.Last], or [Cursor.Seek] finds one.
// Do not copy a cursor after a positioning method uses it because a copy can share the saved path.
// Use a cursor from one goroutine at a time.
//
// For example, a cursor can reach one leaf through child 1 of the root and then child 2 of the next branch.
// Its path contains those two branch choices.
//
// [Cursor.Entry] returns read-only bytes that refer to node storage.
// The caller must not change the root or any node that the cursor can reach while the cursor is in use.
type Cursor struct {
	reader    NodeReader     // reader returns the node for each page ID.
	rootID    page.ID        // rootID is the fixed start of every search from the root.
	path      []treePathStep // path contains the branch choices from the root to leaf.
	leaf      Node           // leaf is the leaf that contains the current entry.
	leafCount int            // leafCount is the entry count of leaf.
	entries   nodeEntries    // entries holds the checked bounds of the current entry. If entries.valid is false, the cursor has no current entry.
}

// NewCursor returns an unpositioned cursor that starts at the root page root.
// reader and the bytes that it returns must stay valid and unchanged while the cursor is in use.
func NewCursor(reader NodeReader, root page.ID) Cursor { return Cursor{reader: reader, rootID: root} }

// First moves to the smallest key and replaces the previous position.
// It returns found false for an empty tree, or an error if a page cannot be read or validated.
func (cursor *Cursor) First() (bool, error) { return cursor.edge(false) }

// Last moves to the largest key and replaces the previous position.
// It returns found false for an empty tree, or an error if a page cannot be read or validated.
func (cursor *Cursor) Last() (bool, error) { return cursor.edge(true) }

// edge moves to the smallest key, or to the largest key when reverse is true.
func (cursor *Cursor) edge(reverse bool) (bool, error) {
	// A search from the root makes every branch choice from the old position invalid.
	cursor.path = cursor.path[:0]
	cursor.entries.valid = false
	root, err := cursor.reader.ReadNode(cursor.rootID)
	if err != nil {
		return false, err
	}
	found, err := cursor.descend(root, reverse)
	if err != nil || found {
		return found, err
	}
	// The edge leaf is empty. An empty leaf does not mark the end of the tree.
	return cursor.moveLeaf(reverse)
}

// Seek moves to the first key greater than or equal to target, replacing the previous position.
// A nil or empty target selects the smallest key. A target after the last key returns found false.
// A page read or validation failure returns an error and clears the current position.
func (cursor *Cursor) Seek(target []byte) (bool, error) {
	cursor.entries.valid = false
	root, err := cursor.reader.ReadNode(cursor.rootID)
	if err != nil {
		return false, err
	}
	leaf, path, err := findLeaf(cursor.reader, root, target, cursor.path[:0])
	cursor.path = path
	if err != nil {
		return false, err
	}
	cursor.setLeaf(leaf)

	// search returns the first index whose key is not less than target.
	// Seek accepts an equal key and the next greater key, so it does not use the exact-match result.
	index, _, err := leaf.search(target)
	if err != nil {
		return false, err
	}
	if index == cursor.leafCount {
		// target sorts after this leaf. The next leaf contains the next greater key.
		return cursor.moveLeaf(false)
	}
	return cursor.position(index)
}

// SeekForward reuses the current leaf when target lies within its key range.
// Other targets use Seek, including targets that move backward. Results follow Seek's rules.
func (cursor *Cursor) SeekForward(target []byte) (bool, error) {
	if cursor.entries.valid {
		first, err := cursor.leaf.Entry(0)
		if err != nil {
			cursor.entries.valid = false
			return false, err
		}
		last, err := cursor.leaf.Entry(cursor.leaf.EntryCount() - 1)
		if err != nil {
			cursor.entries.valid = false
			return false, err
		}
		if bytes.Compare(target, first.key) >= 0 && bytes.Compare(target, last.key) <= 0 {
			index, _, err := cursor.leaf.search(target)
			if err != nil {
				cursor.entries.valid = false
				return false, err
			}
			return cursor.position(index)
		}
	}
	return cursor.Seek(target)
}

// Next moves to the next key using the saved branch path at leaf boundaries.
// It returns found false when unpositioned or exhausted, or an error if a page cannot be read or validated.
func (cursor *Cursor) Next() (found bool, err error) {
	if cursor.entries.valid {
		found, err = cursor.position(cursor.entries.index + 1)
	}
	return
}

// Prev moves to the previous key using the saved branch path at leaf boundaries.
// It returns found false when unpositioned or exhausted, or an error if a page cannot be read or validated.
func (cursor *Cursor) Prev() (found bool, err error) {
	if cursor.entries.valid {
		found, err = cursor.position(cursor.entries.index - 1)
	}
	return
}

// setLeaf makes leaf the current leaf.
// The cursor has no current entry until position selects one.
func (cursor *Cursor) setLeaf(leaf Node) {
	cursor.leaf, cursor.leafCount = leaf, leaf.EntryCount()
	// Clear the old bounds so that position does not compare entries from two different leaves.
	cursor.entries = nodeEntries{}
}

// position selects the entry at index in the current leaf.
// An index before or after the leaf moves to the adjacent leaf in that direction.
// A validation failure clears the current position.
func (cursor *Cursor) position(index int) (bool, error) {
	if index < 0 || index >= cursor.leafCount {
		return cursor.moveLeaf(index < 0)
	}
	if cursor.leaf.node != nil {
		cursor.entries.index, cursor.entries.valid = index, true
		return true, nil
	}
	// Adjacent movement reuses the entry bounds that the previous step already checked.
	forward := cursor.entries.valid && index == cursor.entries.index+1
	backward := cursor.entries.valid && index == cursor.entries.index-1
	cursor.entries.valid = false
	image := cursor.leaf.image
	flags, start, keyEnd, end := leafSpan(image, index)
	dataStart := uint64(NodeHeaderSize + cursor.leafCount*leafEntryDescriptorSize)
	if !validLeaf(flags, start, keyEnd, end, dataStart, uint64(len(image))) {
		return false, entryError(index)
	}
	if index == 0 && start != dataStart || index+1 == cursor.leafCount && end != uint64(len(image)) {
		return false, entryError(index)
	}
	if forward {
		if start != uint64(cursor.entries.valueEnd) || bytes.Compare(image[cursor.entries.keyStart:cursor.entries.keyEnd], image[start:keyEnd]) >= 0 {
			return false, entryError(index)
		}
	} else if backward {
		if end != uint64(cursor.entries.keyStart) || bytes.Compare(image[start:keyEnd], image[cursor.entries.keyStart:cursor.entries.keyEnd]) >= 0 {
			return false, entryError(index)
		}
	} else if index > 0 {
		previousFlags, previousStart, previousKeyEnd, previousEnd := leafSpan(image, index-1)
		if !validLeaf(previousFlags, previousStart, previousKeyEnd, previousEnd, dataStart, uint64(len(image))) || previousEnd != start || bytes.Compare(image[previousStart:previousKeyEnd], image[start:keyEnd]) >= 0 {
			return false, entryError(index)
		}
	}
	cursor.entries.keyStart, cursor.entries.keyEnd, cursor.entries.valueEnd = uint32(start), uint32(keyEnd), uint32(end)
	cursor.entries.index, cursor.entries.valid = index, true
	cursor.entries.flags = flags
	return true, nil
}

// readChild reads the child at index in the branch view.
func (cursor *Cursor) readChild(view Node, index int) (Node, error) {
	id, err := view.Child(index)
	if err != nil {
		return Node{}, err
	}
	return cursor.reader.ReadNode(id)
}

// descend follows the first child of each branch down to a leaf, or the last child when reverse is true.
// It appends each branch choice to the path and then selects the first or last entry of the leaf.
// It returns found false for an empty leaf, so that the caller can move to the adjacent leaf.
func (cursor *Cursor) descend(view Node, reverse bool) (bool, error) {
	for !view.IsLeaf() {
		index := 0
		if reverse {
			// A branch with n separator keys has n+1 children, so child n is the last child.
			index = view.EntryCount()
		}
		cursor.path = append(cursor.path, treePathStep{parent: view, childIndex: index})
		var err error
		view, err = cursor.readChild(view, index)
		if err != nil {
			return false, err
		}
	}
	cursor.setLeaf(view)
	if cursor.leafCount == 0 {
		return false, nil
	}
	index := 0
	if reverse {
		index = cursor.leafCount - 1
	}
	return cursor.position(index)
}

// moveLeaf moves to the first entry of the next leaf, or to the last entry of the previous leaf when reverse is true.
// It walks the saved path from the current leaf toward the root.
// The first branch that has a child in that direction holds the nearest subtree with the adjacent key.
// It returns found false and clears the current position when no such subtree exists.
func (cursor *Cursor) moveLeaf(reverse bool) (bool, error) {
	cursor.entries.valid = false
	for len(cursor.path) > 0 {
		last := len(cursor.path) - 1
		frame := &cursor.path[last]
		next := frame.childIndex + 1
		if reverse {
			next = frame.childIndex - 1
		}
		if next < 0 || next > frame.parent.EntryCount() {
			// This branch has no child in that direction. Continue with its parent.
			cursor.path = cursor.path[:last]
			continue
		}
		frame.childIndex = next
		view, err := cursor.readChild(frame.parent, next)
		if err != nil {
			return false, err
		}
		found, err := cursor.descend(view, reverse)
		if err != nil || found {
			return found, err
		}
		// The selected subtree ends in an empty leaf. An empty leaf does not mark the end of the tree.
	}
	return false, nil
}

// Entry returns the current flags, key, and value, or zero and nil slices when unpositioned.
// The bytes borrow reader's storage and remain valid while that storage stays unchanged.
func (cursor *Cursor) Entry() (flags uint32, key, value []byte) {
	if !cursor.entries.valid {
		return 0, nil, nil
	}
	// Return the fields directly, because Next calls Entry for every key of a scan.
	if cursor.leaf.node != nil {
		entry := &cursor.leaf.node.entries[cursor.entries.index]
		return entry.flags, entry.key, entry.value
	}
	position, image := &cursor.entries, cursor.leaf.image
	return position.flags, image[position.keyStart:position.keyEnd:position.keyEnd], image[position.keyEnd:position.valueEnd:position.valueEnd]
}

// Find searches for an exact key without changing the cursor's iteration position.
// includeKey permits encoded results to omit the matched key.
func (cursor *Cursor) Find(key []byte, includeKey bool) (Entry, bool, error) {
	if err := validateLookupKey(key); err != nil {
		return Entry{}, false, err
	}
	return FindEntry(cursor.reader, cursor.rootID, key, includeKey)
}
