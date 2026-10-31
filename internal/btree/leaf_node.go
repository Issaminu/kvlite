package btree

import (
	"bytes"
	"slices"
	"sort"

	"github.com/Issaminu/kvlite/internal/page"
)

// BucketLeafFlag marks a leaf value that stores a nested bucket root page ID.
const BucketLeafFlag uint32 = 0x01

// NewLeafNode creates an empty private leaf at head page pgid.
func NewLeafNode(pgid page.ID) *MutableNode {
	return &MutableNode{
		header:   newNodeHeader(NodeTypeLeaf, pgid),
		Children: []page.ID{},
		entries:  []Entry{},
	}
}

// Find the index of the key in it's leaf node.
// Returns (index, isFound, err)
func (n *MutableNode) findKeyIndex(key []byte) (int, bool, error) {
	if !n.IsLeaf() {
		return -1, false, ErrNotLeafNode
	}
	index := sort.Search(len(n.entries), func(index int) bool {
		return bytes.Compare(n.entries[index].key, key) >= 0
	})
	found := index < len(n.entries) && bytes.Equal(n.entries[index].key, key)
	return index, found, nil
}

// FindEntryRef looks a key up in this leaf without copying its key or value.
// The returned entry refers to storage owned by the node.
func (n *MutableNode) FindEntryRef(key []byte) (entry Entry, found bool, err error) {
	if !n.IsLeaf() {
		err = ErrNotLeafNode
		return
	}
	entry, found, _, err = LookupNode(n, key)
	return
}

func cloneValue(value []byte) []byte {
	value = slices.Clone(value)
	if value == nil {
		return []byte{}
	}
	return value
}

type leafInsert struct {
	entry Entry
	index int
	found bool
}

// prepareInsert checks a leaf insertion without changing the leaf.
func (n *MutableNode) prepareInsert(entry Entry) (leafInsert, error) {
	if !n.IsLeaf() {
		return leafInsert{}, ErrNotLeafNode
	}

	idx, found, err := n.findKeyIndex(entry.key)
	if err != nil {
		return leafInsert{}, err
	}
	if found {
		existingIsBucket := n.entries[idx].flags&BucketLeafFlag != 0
		newIsBucket := entry.flags&BucketLeafFlag != 0
		if existingIsBucket != newIsBucket {
			return leafInsert{}, ErrIncompatibleValue
		}
	}
	return leafInsert{entry: entry, index: idx, found: found}, nil
}

// applyInsert changes a leaf after prepareInsert has accepted the operation.
func (n *MutableNode) applyInsert(prepared leafInsert) {
	// Keep a known size exact so a batch does not rescan the leaf after each put.
	// A replacement keeps its key and descriptor; only its value length changes.
	if n.encodedSize != 0 {
		if prepared.found {
			n.encodedSize += len(prepared.entry.value) - len(n.entries[prepared.index].value)
		} else {
			n.encodedSize += prepared.entry.EncodedSize(true)
		}
	}
	n.header.Checksum = 0
	if prepared.found {
		n.entries[prepared.index].flags = prepared.entry.flags
		n.entries[prepared.index].borrowValue = prepared.entry.borrowValue
		n.entries[prepared.index].borrowedImage = false
		if prepared.entry.borrowValue {
			n.entries[prepared.index].value = prepared.entry.value
		} else {
			n.entries[prepared.index].value = cloneValue(prepared.entry.value)
		}
		return
	}

	entry := Entry{
		flags:       prepared.entry.flags,
		borrowValue: prepared.entry.borrowValue,
		key:         slices.Clone(prepared.entry.key),
		value:       prepared.entry.value,
	}
	if !entry.borrowValue {
		entry.value = cloneValue(entry.value)
	}

	n.entries = append(n.entries, Entry{})
	copy(n.entries[prepared.index+1:], n.entries[prepared.index:])
	n.entries[prepared.index] = entry
}

func (n *MutableNode) InsertEntry(entry Entry) error {
	prepared, err := n.prepareInsert(entry)
	if err != nil {
		return err
	}
	n.applyInsert(prepared)
	return nil
}

func (n *MutableNode) prepareDelete(key []byte, bucket bool) (int, bool, error) {
	index, found, err := n.findKeyIndex(key)
	if err != nil || !found {
		return 0, found, err
	}
	if (n.entries[index].flags&BucketLeafFlag != 0) != bucket {
		return 0, false, ErrIncompatibleValue
	}
	return index, true, nil
}

// DeleteLeafEntries removes selected entries from a private leaf in one pass.
// n must be a leaf owned by the caller. firstIndex and rest must contain
// distinct, ascending entry indexes. It clears unused entry slots.
func (n *MutableNode) DeleteLeafEntries(firstIndex int, rest []int) {
	remaining := len(n.entries) - 1 - len(rest)
	copyLeafEntriesExcept(n.entries[:remaining], n.entries, firstIndex, rest)
	clear(n.entries[remaining:])
	n.entries = n.entries[:remaining]
	n.encodedSize = 0
	n.header.Checksum = 0
}

// copyLeafEntriesExcept copies the ranges between removed entries.
// dst has room for the remaining entries. It can start at entries[0].
func copyLeafEntriesExcept(dst, entries []Entry, firstIndex int, rest []int) {
	write := copy(dst, entries[:firstIndex])
	read := firstIndex + 1
	for _, index := range rest {
		write += copy(dst[write:], entries[read:index])
		read = index + 1
	}
	copy(dst[write:], entries[read:])
}

// DeleteKey removes the leaf entry that has key. It returns [ErrKeyNotFound] when the leaf has no such entry.
// WAL recovery uses it to replay an entry patch. It removes the entry with DeleteLeafEntries, as the normal delete path does.
func (n *MutableNode) DeleteKey(key []byte) error {
	index, found, err := n.findKeyIndex(key)
	if err != nil {
		return err
	}
	if !found {
		return ErrKeyNotFound
	}
	n.DeleteLeafEntries(index, nil)
	return nil
}

// InsertOrReplaceLeafEntry inserts entry, or it replaces the entry that has the same key.
// Unlike InsertEntry, it does not compare the bucket flags of the two entries: an entry patch can replace a value with a nested bucket in one change, or the reverse.
// WAL recovery uses it to replay an entry patch. It copies the key and value bytes.
func (n *MutableNode) InsertOrReplaceLeafEntry(entry Entry) error {
	index, found, err := n.findKeyIndex(entry.key)
	if err != nil {
		return err
	}
	n.applyInsert(leafInsert{entry: entry, index: index, found: found})
	return nil
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

// leafSeparator returns the shortest prefix of rightMinimum above leftMaximum.
// Both inputs must be nonempty and leftMaximum must sort before rightMinimum.
// The result borrows rightMinimum. Leaf keys stay unchanged.
func leafSeparator(leftMaximum, rightMinimum []byte) []byte {
	common := 0
	for common < len(leftMaximum) && common < len(rightMinimum) && leftMaximum[common] == rightMinimum[common] {
		common++
	}
	return rightMinimum[:common+1]
}

// redistributeLeaves changes the entry lists and the parent separator if both pages fit.
func redistributeLeaves(parent *MutableNode, separatorIndex int, left, right *MutableNode, pageSize, minimumSize int64) bool {
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
	parent.entries[separatorIndex] = Entry{key: slices.Clone(leafSeparator(left.entries[len(left.entries)-1].key, right.entries[0].key))}
	left.encodedSize, right.encodedSize, parent.encodedSize = 0, 0, 0

	return true
}

// splitLeaf moves entries from index onward into the new right leaf.
func (n *MutableNode) splitLeaf(right *MutableNode, index int) {
	n.encodedSize, right.encodedSize = 0, 0
	right.entries = slices.Clone(n.entries[index:])
	n.entries = n.entries[:index]
}

// visitLeafReferences visits roots of nested buckets stored as leaf values.
func visitLeafReferences(node *MutableNode, visit func(page.ID) error) error {
	for _, entry := range node.entries {
		if entry.flags&BucketLeafFlag == 0 {
			continue
		}
		child, err := page.DecodeID(entry.value)
		if err != nil {
			return err
		}
		if err := visit(child); err != nil {
			return err
		}
	}
	return nil
}
