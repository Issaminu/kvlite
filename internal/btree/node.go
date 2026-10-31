package btree

import (
	"bytes"
	"encoding/binary"

	"github.com/Issaminu/kvlite/internal/page"
)

// NodeReader searches the pages visible to one database operation.
// Implementations select committed storage or a transaction's private changes.
// The caller must keep the visible tree unchanged during a traversal.
type NodeReader interface {
	// ReadNode returns the read-only node whose first storage page is id.
	// It borrows storage for the enclosing operation's lifetime. An unreadable,
	// unallocated node returns an error, rather than a missing key.
	// Acquisition checks the header and checksum; operations check the entries they use.
	ReadNode(id page.ID) (Node, error)
}

// Node provides read-only access to one leaf or branch without exposing its representation.
// Its entries borrow the owner's storage and remain valid only while that storage stays unchanged.
// Obtain a Node through a NodeReader, MutableNode.ReadOnly, or ReadNodeImage. The zero value is invalid.
type Node struct {
	node  *MutableNode
	image []byte
}

// EntryCount returns the number of leaf entries or branch separators.
func (view Node) EntryCount() int {
	if view.node != nil {
		return len(view.node.entries)
	}
	return int(binary.LittleEndian.Uint32(view.image[10:14]))
}

// IsLeaf reports whether the node contains entries rather than child references.
func (view Node) IsLeaf() bool {
	if view.node != nil {
		return view.node.IsLeaf()
	}
	return mappedNodeType(view.image) == NodeTypeLeaf
}

// Entry returns the checked leaf entry at index. Its bytes are read-only and
// borrow the node. An invalid index or descriptor returns ErrInvalid.
func (view Node) Entry(index int) (Entry, error) {
	if !view.IsLeaf() || index < 0 || index >= view.EntryCount() {
		return Entry{}, ErrInvalid
	}
	var entries nodeEntries
	if err := entries.position(view, index); err != nil {
		return Entry{}, err
	}
	return view.entryAtPosition(entries), nil
}

// Child returns the checked branch child at index, from zero through EntryCount.
// A leaf, invalid index, or invalid encoded child reference returns ErrInvalid.
func (view Node) Child(index int) (page.ID, error) {
	if view.IsLeaf() || index < 0 || index > view.EntryCount() {
		return 0, ErrInvalid
	}
	if view.node != nil {
		return view.node.Children[index], nil
	}
	if count := view.EntryCount(); count > 0 {
		if _, err := view.separator(min(index, count-1)); err != nil {
			return 0, err
		}
	}
	id, ok := branchChild(view.image, view.EntryCount(), index)
	if !ok {
		return 0, childError(index)
	}
	return id, nil
}

// Lookup searches one node. A leaf returns an entry and found; a branch returns
// the child page to search next. A missing leaf key returns found false and child zero.
// includeKey allows an encoded leaf to omit the matched key. Values remain read-only.
func (view Node) Lookup(key []byte, includeKey bool) (entry Entry, found bool, child page.ID, err error) {
	if err = validateLookupKey(key); err != nil {
		return
	}
	if view.node != nil {
		return LookupNode(view.node, key)
	}
	if view.IsLeaf() {
		_, found, err = lookupLeafBody(view.image, uint32(view.EntryCount()), key, includeKey, &entry)
	} else {
		child, err = lookupBranchBody(view.image, uint32(view.EntryCount()), key)
	}
	return
}

// Clone returns a private decoded node with independent entry and child lists.
// Keys and values remain read-only and can borrow the source storage. Call
// OwnImageBytes before persistence can overwrite or release an encoded source.
// The result never aliases the MutableNode backing this read-only view.
func (view Node) Clone(pageSize int64) (*MutableNode, error) {
	if view.node != nil {
		return view.node.Clone(), nil
	}
	return view.decode(pageSize)
}

// decode borrows decoded lists for read-only work inside this package.
// Mutations must use Store.EditNode; callers outside btree use Clone.
func (view Node) decode(pageSize int64) (*MutableNode, error) {
	if view.node != nil {
		return view.node, nil
	}
	image := view.image
	h := NodeHeader{Type: mappedNodeType(image), PageID: view.PageID(), EntryCount: uint32(view.EntryCount()), BodyLength: uint32(len(image) - NodeHeaderSize)}
	node, err := decodeBody(image, h, true)
	if err != nil {
		return nil, err
	}
	node.SetStoragePageCount((len(image) + int(pageSize) - 1) / int(pageSize))
	for index := range node.entries {
		node.entries[index].borrowedImage = true
		node.entries[index].borrowedKey = true
	}
	return node, nil
}

// EncodedSize returns the logical byte size, excluding storage-page padding.
func (view Node) EncodedSize() int {
	if view.node != nil {
		return view.node.EncodedSize()
	}
	return len(view.image)
}

// StoragePageCount returns the currently allocated span length, including the head.
func (view Node) StoragePageCount(pageSize int64) int {
	if view.node != nil {
		return view.node.StoragePageCount()
	}
	return (len(view.image) + int(pageSize) - 1) / int(pageSize)
}

// separator returns the checked branch separator at index.
// It borrows node storage; an invalid index or encoded key returns ErrInvalid.
func (view Node) separator(index int) ([]byte, error) {
	if view.IsLeaf() || index < 0 || index >= view.EntryCount() {
		return nil, ErrInvalid
	}
	if view.node != nil {
		return view.node.entries[index].key, nil
	}
	dataStart, err := branchDataStart(uint64(len(view.image)), uint32(view.EntryCount()))
	if err != nil {
		return nil, err
	}
	start, end := branchKey(view.image, index)
	if !validKey(start, end, dataStart, uint64(len(view.image))) {
		return nil, entryError(index)
	}
	if index == 0 && start != dataStart || index+1 == view.EntryCount() && end != uint64(len(view.image)) {
		return nil, entryError(index)
	}
	if index > 0 {
		previousStart, previousEnd := branchKey(view.image, index-1)
		if !validKey(previousStart, previousEnd, dataStart, uint64(len(view.image))) || previousEnd != start || bytes.Compare(view.image[previousStart:previousEnd], view.image[start:end]) >= 0 {
			return nil, entryError(index)
		}
	}
	return view.image[start:end:end], nil
}

func (view Node) search(key []byte) (int, bool, error) {
	if view.node != nil {
		if view.IsLeaf() {
			return view.node.findKeyIndex(key)
		}
		index, err := view.node.FindChildIndex(key)
		return index, false, err
	}
	if view.IsLeaf() {
		var entry Entry
		return lookupLeafBody(view.image, uint32(view.EntryCount()), key, false, &entry)
	}
	index, _, err := searchBranchBody(view.image, uint32(view.EntryCount()), key)
	return index, false, err
}

func (view Node) prepareDelete(key []byte, bucket bool) (int, bool, error) {
	index, found, err := view.search(key)
	if err != nil || !found {
		return 0, found, err
	}
	entry, err := view.Entry(index)
	if err != nil {
		return 0, false, err
	}
	if (entry.flags&BucketLeafFlag != 0) != bucket {
		return 0, false, ErrIncompatibleValue
	}
	return index, true, nil
}

// ReadOnly borrows the node contents without permitting mutation through the result.
// The owner must keep the contents unchanged while the result is in use.
func (node *MutableNode) ReadOnly() Node {
	return Node{node: node}
}

// PageID returns the head page of this node.
func (view Node) PageID() page.ID {
	if view.node != nil {
		return view.node.PageID()
	}
	return page.ID(binary.LittleEndian.Uint64(view.image[2:10]))
}

// VisitReferences checks and visits child nodes, then physical continuation pages.
// pageSize is the storage unit used by the reader; callbacks can stop with an error.
func (view Node) VisitReferences(pageSize int64, visit, visitContinuation func(page.ID) error) error {
	if view.node != nil {
		return VisitNodeReferences(view.node, visit, visitContinuation)
	}
	if pageSize < NodeHeaderSize {
		return ErrInvalid
	}
	if err := checkBody(view.image, NodeHeader{Type: mappedNodeType(view.image), EntryCount: uint32(view.EntryCount())}, true, visit); err != nil {
		return err
	}
	if visitContinuation != nil {
		count := (uint64(len(view.image)) + uint64(pageSize) - 1) / uint64(pageSize)
		for offset := uint64(1); offset < count; offset++ {
			if err := visitContinuation(view.PageID() + page.ID(offset)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ReadNodeImage checks the encoded header and borrows its logical image.
// Node operations check the directories and entries they use.
// data contains the complete padded main-file span or compact WAL payload.
// wal selects the compact format; checksums applies only to main-file spans.
func ReadNodeImage(data []byte, id page.ID, pageSize int64, wal, checksums bool) (Node, error) {
	var image []byte
	if _, _, ok := plainNodeHeader(data, id, pageSize, wal, checksums); ok {
		size := NodeHeaderSize + int(binary.LittleEndian.Uint32(data[18:22]))
		image = data[:size:len(data)]
	} else {
		checked, _, err := nodeImage(data, id, pageSize, nil, wal, checksums)
		if err != nil {
			return Node{}, err
		}
		image = data[:len(checked):len(data)]
	}
	return nodeFromImage(image)
}

func nodeFromImage(image []byte) (Node, error) {
	// Empty nodes have no entry access through which to check unused body bytes.
	if binary.LittleEndian.Uint32(image[10:14]) == 0 {
		emptySize := NodeHeaderSize
		if mappedNodeType(image) == NodeTypeBranch {
			emptySize += page.IDSize
		}
		if len(image) != emptySize {
			return Node{}, errBodySize
		}
	}
	return Node{image: image}, nil
}

// nodeEntries keeps the checked bounds of one leaf entry. Adjacent moves reuse
// those bounds without reading the previous descriptor again.
type nodeEntries struct {
	index                      int
	valid                      bool
	flags                      uint32
	keyStart, keyEnd, valueEnd uint32
}

func (entries *nodeEntries) position(node Node, index int) error {
	if node.node != nil {
		entries.index, entries.valid = index, true
		return nil
	}
	if !entries.valid {
		if _, err := leafDataStart(uint64(len(node.image)), uint32(node.EntryCount())); err != nil {
			return err
		}
	}
	// Adjacent movement reuses the entry bounds that the previous step already checked.
	forward := entries.valid && index == entries.index+1
	backward := entries.valid && index == entries.index-1
	entries.valid = false
	image := node.image
	flags, start, keyEnd, end := leafSpan(image, index)
	dataStart := uint64(NodeHeaderSize + node.EntryCount()*leafEntryDescriptorSize)
	if !validLeaf(flags, start, keyEnd, end, dataStart, uint64(len(image))) {
		return entryError(index)
	}
	if index == 0 && start != dataStart || index+1 == node.EntryCount() && end != uint64(len(image)) {
		return entryError(index)
	}
	if forward {
		if start != uint64(entries.valueEnd) || bytes.Compare(image[entries.keyStart:entries.keyEnd], image[start:keyEnd]) >= 0 {
			return entryError(index)
		}
	} else if backward {
		if end != uint64(entries.keyStart) || bytes.Compare(image[start:keyEnd], image[entries.keyStart:entries.keyEnd]) >= 0 {
			return entryError(index)
		}
	} else if index > 0 {
		previousFlags, previousStart, previousKeyEnd, previousEnd := leafSpan(image, index-1)
		if !validLeaf(previousFlags, previousStart, previousKeyEnd, previousEnd, dataStart, uint64(len(image))) || previousEnd != start || bytes.Compare(image[previousStart:previousKeyEnd], image[start:keyEnd]) >= 0 {
			return entryError(index)
		}
	}
	entries.keyStart, entries.keyEnd, entries.valueEnd = uint32(start), uint32(keyEnd), uint32(end)
	entries.index, entries.valid = index, true
	entries.flags = flags
	return nil
}

// visitRange visits this leaf's entries in [start,end) without decoding an entry list.
// Empty bounds are unbounded. The callback receives read-only borrowed bytes.
func (node Node) visitRange(start, end []byte, visit func(uint32, []byte, []byte) error) error {
	if !node.IsLeaf() {
		return ErrNotLeafNode
	}
	if node.node == nil {
		return visitLeafRange(node.image, start, end, visit)
	}
	first, _, err := node.search(start)
	if err != nil {
		return err
	}
	for _, entry := range node.node.entries[first:] {
		if len(end) > 0 && bytes.Compare(entry.key, end) >= 0 {
			break
		}
		if err := visit(entry.flags, entry.key, entry.value); err != nil {
			return err
		}
	}
	return nil
}

// visitReverse visits this leaf's entries from largest to smallest key.
// A callback error stops iteration. The callback bytes borrow the node.
func (node Node) visitReverse(visit func(uint32, []byte, []byte) error) error {
	if !node.IsLeaf() {
		return ErrNotLeafNode
	}
	if node.node == nil {
		return visitLeafReverse(node.image, visit)
	}
	for index := len(node.node.entries) - 1; index >= 0; index-- {
		entry := node.node.entries[index]
		if err := visit(entry.flags, entry.key, entry.value); err != nil {
			return err
		}
	}
	return nil
}

// children returns an iterator over branch children whose ranges intersect [start,end).
// Empty bounds are unbounded. reverse selects descending key order.
// Encoded child IDs are checked before iteration. Bounded ranges also check separator keys;
// unbounded traversal does not read them. VisitReferences checks the full node.
func (node Node) children(start, end []byte, reverse bool) (childIterator, error) {
	if node.IsLeaf() {
		return childIterator{}, ErrNotBranchNode
	}
	count := node.EntryCount()
	if node.node == nil {
		if len(start) != 0 || len(end) != 0 {
			if err := checkBranchBody(node.image, NodeHeader{EntryCount: uint32(count)}, true, nil); err != nil {
				return childIterator{}, err
			}
		} else {
			if _, err := branchDataStart(uint64(len(node.image)), uint32(count)); err != nil {
				return childIterator{}, err
			}
			for index := 0; index <= count; index++ {
				if _, ok := branchChild(node.image, count, index); !ok {
					return childIterator{}, childError(index)
				}
			}
		}
	}
	first, last := 0, count
	if len(start) > 0 {
		index, _, err := node.search(start)
		if err != nil {
			return childIterator{}, err
		}
		first = index
	}
	if len(end) > 0 {
		for index := first; index < count; index++ {
			var separator []byte
			if node.node != nil {
				separator = node.node.entries[index].key
			} else {
				from, to := branchKey(node.image, index)
				separator = node.image[from:to]
			}
			if bytes.Compare(separator, end) >= 0 {
				last = index
				break
			}
		}
	}
	step := 1
	if reverse {
		first, last, step = last, first, -1
	}
	result := childIterator{index: first, end: last + step, step: step}
	if node.node != nil {
		if len(node.node.Children) != count+1 {
			return childIterator{}, ErrInvalid
		}
		result.decoded = node.node.Children
	} else {
		offset := NodeHeaderSize + count*branchEntryDescriptorSize
		result.encoded = node.image[offset : offset+(count+1)*page.IDSize]
	}
	return result, nil
}

// childIterator holds a position in a node's immutable child list.
// It borrows node storage. Its zero value is exhausted.
type childIterator struct {
	decoded          []page.ID
	encoded          []byte
	index, end, step int
}

// next returns the next child page ID, or false when exhausted.
func (children *childIterator) next() (page.ID, bool) {
	if children.index == children.end {
		return 0, false
	}
	index := children.index
	children.index += children.step
	if children.decoded != nil {
		return children.decoded[index], true
	}
	offset := index * page.IDSize
	return page.ID(binary.LittleEndian.Uint64(children.encoded[offset : offset+page.IDSize])), true
}

// entryAtPosition borrows the entry whose bounds position already checked.
func (node Node) entryAtPosition(position nodeEntries) Entry {
	if node.node != nil {
		return node.node.entries[position.index]
	}
	return Entry{
		flags: position.flags,
		key:   node.image[position.keyStart:position.keyEnd:position.keyEnd],
		value: node.image[position.keyEnd:position.valueEnd:position.valueEnd],
	}
}
