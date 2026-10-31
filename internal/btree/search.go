package btree

import (
	"bytes"

	"github.com/Issaminu/kvlite/internal/page"
)

// FindEntry searches for an exact key without allocating cursor iteration state.
// Entry bytes borrow the reader; includeKey permits omitting the matched key.
// The caller must validate key.
func FindEntry(reader NodeReader, id page.ID, key []byte, includeKey bool) (entry Entry, found bool, err error) {
	for {
		node, readErr := reader.ReadNode(id)
		if readErr != nil {
			return Entry{}, false, readErr
		}
		entry, found, id, err = node.Lookup(key, includeKey)
		if err != nil || id == 0 {
			return
		}
	}
}

// findLeaf follows the separator keys from view to the only leaf that can contain key.
// It appends each branch choice to path and returns the leaf and the longer path.
// After an error, it returns the path that it made before the error.
func findLeaf(reader NodeReader, view Node, key []byte, path []treePathStep) (Node, []treePathStep, error) {
	for !view.IsLeaf() {
		index, _, err := view.search(key)
		if err != nil {
			return Node{}, path, err
		}
		id, err := view.Child(index)
		if err != nil {
			return Node{}, path, err
		}
		child, err := reader.ReadNode(id)
		if err != nil {
			return Node{}, path, err
		}
		if cap(path) == 0 {
			// Reserve the next branch frame too when the child already needs one.
			capacity := 1
			if !child.IsLeaf() {
				capacity = 2
			}
			path = make([]treePathStep, 0, capacity)
		}
		path = append(path, treePathStep{parent: view, childIndex: index})
		view = child
	}
	return view, path, nil
}

// ReadBatch reads sorted request positions into their original result slots.
// keys and order remain unchanged. Missing keys leave nil values; bucket names
// return ErrIncompatibleValue. Values borrow reader's storage.
func ReadBatch(reader NodeReader, root page.ID, keys [][]byte, order []int, values [][]byte) error {
	if len(order) == 0 {
		return nil
	}
	node, err := reader.ReadNode(root)
	if err != nil {
		return err
	}
	if node.IsLeaf() {
		for position, index := range order {
			if position > 0 && bytes.Equal(keys[index], keys[order[position-1]]) {
				values[index] = values[order[position-1]]
				continue
			}
			entry, found, _, err := node.Lookup(keys[index], false)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if entry.Flags()&BucketLeafFlag != 0 {
				return ErrIncompatibleValue
			}
			values[index] = entry.Value()
		}
		return nil
	}
	for start := 0; start < len(order); {
		index, _, err := node.search(keys[order[start]])
		if err != nil {
			return err
		}
		end := len(order)
		if index < node.EntryCount() {
			separator, err := node.separator(index)
			if err != nil {
				return err
			}
			end = start + 1
			for end < len(order) && bytes.Compare(keys[order[end]], separator) < 0 {
				end++
			}
		}
		child, err := node.Child(index)
		if err != nil {
			return err
		}
		if err := ReadBatch(reader, child, keys, order[start:end], values); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// ScanRange visits entries in [start,end) in key order. Empty bounds are unbounded.
// The callback borrows read-only node bytes and can stop traversal with an error.
func ScanRange(reader NodeReader, root page.ID, start, end []byte, visit func(uint32, []byte, []byte) error) error {
	node, err := reader.ReadNode(root)
	if err != nil {
		return err
	}
	if node.IsLeaf() {
		return node.visitRange(start, end, visit)
	}
	children, err := node.children(start, end, false)
	if err != nil {
		return err
	}
	for child, ok := children.next(); ok; child, ok = children.next() {
		if err := ScanRange(reader, child, start, end, visit); err != nil {
			return err
		}
	}
	return nil
}

// ScanReverse visits all entries from largest to smallest key.
// Callback bytes borrow reader's storage; a callback error stops traversal.
func ScanReverse(reader NodeReader, root page.ID, visit func(uint32, []byte, []byte) error) error {
	node, err := reader.ReadNode(root)
	if err != nil {
		return err
	}
	if node.IsLeaf() {
		return node.visitReverse(visit)
	}
	children, err := node.children(nil, nil, true)
	if err != nil {
		return err
	}
	for child, ok := children.next(); ok; child, ok = children.next() {
		if err := ScanReverse(reader, child, visit); err != nil {
			return err
		}
	}
	return nil
}
