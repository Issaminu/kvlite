package btree

import (
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/page"
)

// NodeHeaderSize is the encoded size of a B+tree node head.
const NodeHeaderSize = 22

const (
	nodeTypeOffset       = 0
	nodePageIDOffset     = 2
	nodeEntryCountOffset = 10
	nodeChecksumOffset   = 14
)

// NodeType identifies the 16-bit type field of a tree file page.
type NodeType uint16

const (
	// NodeTypeLeaf identifies a node that stores key and value entries.
	NodeTypeLeaf NodeType = 1
	// NodeTypeBranch identifies a node that stores separator keys and child page IDs.
	NodeTypeBranch NodeType = 2
)

// NodeHeader identifies one complete leaf or branch span. Fields are little-endian.
//
// [0:2] type; [2:10] head page ID; [10:14] entry count;
// [14:18] whole-span CRC32C; [18:22] logical body length.
// The header and body length determine the number of storage pages.
// Only the first page has a header. All following pages contain raw body bytes.
// Zero disables the data checksum. A calculated CRC of zero is stored as one.
type NodeHeader struct {
	Type       NodeType
	PageID     page.ID
	EntryCount uint32
	Checksum   uint32
	BodyLength uint32
}

// MutableNode contains one logical B+tree node. PageID names the head of its consecutive span.
// Its keys and values can borrow a complete immutable encoded image.
type MutableNode struct {
	header           NodeHeader
	entries          []Entry
	Children         []page.ID // for non-leaf nodes: len(Children) == len(entries)+1
	encodedSize      int       // Encoded bytes, or zero when the current size has not been calculated.
	storageTailPages uint32    // Allocated pages after the head. Memory only; kept until commit preparation.
}

// StoragePageCount returns the allocated span length, including the head.
// A private change keeps its previous span length until commit preparation.
func (n *MutableNode) StoragePageCount() int { return int(n.storageTailPages) + 1 }

// SetStoragePageCount records the allocated consecutive span length.
// count includes the head and must be positive.
func (n *MutableNode) SetStoragePageCount(count int) {
	if count < 1 {
		panic("btree: empty node span")
	}
	n.storageTailPages = uint32(count - 1)
}

// OverflowPages derives the tail page IDs from the head and count.
// The returned list is private to the caller. The node stores no ID list.
func (n *MutableNode) OverflowPages() []page.ID {
	ids := make([]page.ID, n.StoragePageCount()-1)
	for i := range ids {
		ids[i] = n.PageID() + page.ID(i) + 1
	}
	return ids
}

func newNodeHeader(nodeType NodeType, pgid page.ID) NodeHeader {
	return NodeHeader{
		Type:   nodeType,
		PageID: pgid,
	}
}

// IsLeaf reports whether n stores key and value entries.
func (n *MutableNode) IsLeaf() bool {
	return n.kind() == NodeTypeLeaf
}

// kind returns NodeTypeLeaf or NodeTypeBranch.
func (n *MutableNode) kind() NodeType {
	return n.header.Type
}

func (n *MutableNode) PageID() page.ID {
	return n.header.PageID
}

func (n *MutableNode) SetPageID(pgid page.ID) {
	n.header.PageID = pgid
	n.header.Checksum = 0
}

func (n *MutableNode) EntryCount() int {
	return len(n.entries)
}

// EntryAt returns the entry at index without copying it.
// The returned entry refers to storage owned by the node.
// It remains valid until the node changes its entry list.
// The caller must not replace the entry or change its slices.
func (n *MutableNode) EntryAt(index int) *Entry {
	return &n.entries[index]
}

// Clone returns a structural copy of n with the same page ID.
// The clone owns its entry and child lists.
// It shares the immutable key and value bytes with n.
// Changes to the node must replace an entry or byte slice instead of changing shared bytes in place.
func (n *MutableNode) Clone() *MutableNode {
	entries := make([]Entry, len(n.entries), max(len(n.entries)+1, cap(n.entries)))
	copy(entries, n.entries)
	return &MutableNode{
		header:           n.header,
		entries:          entries,
		Children:         slices.Clone(n.Children),
		storageTailPages: n.storageTailPages,
	}
}

func (n *MutableNode) firstKey() []byte {
	if len(n.entries) == 0 {
		return nil
	}
	return n.entries[0].key
}

const (
	MaxKeySize   = 32768         // 32 KiB
	MaxValueSize = (1 << 31) - 2 // ~2 GiB
)

// HasBorrowedValues reports whether a node retains values owned by its caller.
func (n *MutableNode) HasBorrowedValues() bool {
	for _, entry := range n.entries {
		if entry.borrowValue {
			return true
		}
	}
	return false
}

func validateLookupKey(key []byte) error {
	if len(key) == 0 {
		return ErrKeyRequired
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	return nil
}

// LookupNode searches one decoded node.
// A leaf returns an entry and a zero child page ID.
// A branch returns the selected child page ID.
// The caller must validate key.
func LookupNode(node *MutableNode, key []byte) (Entry, bool, page.ID, error) {
	if node.IsLeaf() {
		index, found, err := node.findKeyIndex(key)
		if err != nil || !found {
			return Entry{}, found, 0, err
		}
		entry := node.entries[index]
		if entry.value == nil {
			entry.value = []byte{}
		}
		return entry, true, 0, nil
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

// NeedsSplit reports whether the node exceeds one page and has enough entries to split.
// A leaf needs two entries. A branch needs three keys so both results keep at
// least two children. Fewer large entries use continuation pages. Splitting
// a two-key branch would create a one-child side and make large-key inserts
// grow the tree by one level at each split.
// This saves the size for commit preparation. The caller must own the node.
func (n *MutableNode) NeedsSplit(pageSize int64) bool {
	minimum := 2
	if !n.IsLeaf() {
		minimum = 3
	}
	return len(n.entries) >= minimum && int64(n.CacheEncodedSize()) > pageSize
}

// EncodedSize reports the exact number of bytes that [EncodeNode] writes for n.
func (n *MutableNode) EncodedSize() int {
	if n.encodedSize != 0 {
		return n.encodedSize
	}
	return n.computeEncodedSize()
}

// computeEncodedSize adds the encoded size of every entry.
// It is not inlined, so EncodedSize stays small enough to inline at its callers.
// Most calls find the saved size and do not need this loop.
//
//go:noinline
func (n *MutableNode) computeEncodedSize() int {
	return NodeHeaderSize + nodePayloadSize(n)
}

// CacheEncodedSize saves the current size for page counting and encoding.
// The caller must have exclusive access to n. Readers use EncodedSize, which changes no state.
// Mutations update the saved size or clear it when the layout changes. Clone starts without a saved size.
func (n *MutableNode) CacheEncodedSize() int {
	n.encodedSize = n.EncodedSize()
	return n.encodedSize
}

// chooseSplitIndex selects a separator near half a page based on encoded entry bytes while preserving valid left and right node shapes.
// A leaf keeps its separator as the first entry of the right node, while a branch promotes and removes its separator.
func (n *MutableNode) chooseSplitIndex(pageSize int64) (int, error) {
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
// A leaf keeps all entry keys in the two resulting leaves.
// A branch promotes the separator and removes it from both child nodes.
// If n has too few entries to split, Split returns [ErrNodeNotSaturated] without changing n.
func (n *MutableNode) Split(newPageID page.ID, pageSize int64) (*MutableNode, []byte, error) {
	separatorIndex, err := n.chooseSplitIndex(pageSize)
	if err != nil {
		return nil, nil, err
	}

	rightNode := &MutableNode{
		header:   newNodeHeader(n.kind(), newPageID),
		Children: []page.ID{},
	}

	separator := n.entries[separatorIndex].key
	if n.IsLeaf() {
		separator = leafSeparator(n.entries[separatorIndex-1].key, separator)
		n.splitLeaf(rightNode, separatorIndex)
	} else {
		n.splitBranch(rightNode, separatorIndex)
	}
	n.header.Checksum = 0

	return rightNode, separator, nil
}

// redistributionScore describes the two pages we would get by moving entries
// between neighbors. The score is valid only if both pages fit. underfull says
// how many more bytes the pages need to reach the target size. imbalance says
// how far apart their sizes are. Smaller values are better.
type redistributionScore struct {
	underfull int64 // for nuking the node and putting it's content in the left/right nodes. underfull is "how much of the left/right node's size remains to reach the 25% target (a quarter of a page's size)"
	imbalance int64 // for NOT nuking the node, and taking some of the left/right node's content into the node. imbalance is "if we were to distribute some of the left/right node's content into the node, what's the difference would be between the left/right size and the node's size?"
	valid     bool
}

// chooseSiblingRedistribution decides which neighbor should share entries with
// node. It tries left with node and node with right on copies. The pages stay
// unchanged until the caller makes the chosen move.
//
// First prefer the pair that leaves fewer bytes below the target size. If the
// pairs tie, prefer the pair with sizes closer together. An exact tie selects
// left. ok is false if neither pair can make two pages that fit.
func chooseSiblingRedistribution(parent *MutableNode, childIndex int, left, node, right *MutableNode, pageSize, minimumSize int64) (useLeft, ok bool) {
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
func scoreRedistribution(parent *MutableNode, separatorIndex int, left, right *MutableNode, pageSize, minimumSize int64) redistributionScore {
	parent = parent.Clone()
	left = left.Clone()
	right = right.Clone()
	if !redistributeChildren(parent, separatorIndex, left, right, pageSize, minimumSize) {
		return redistributionScore{}
	}
	underfull, imbalance := redistributionPenalty(int64(left.EncodedSize()), int64(right.EncodedSize()), minimumSize)
	return redistributionScore{underfull: underfull, imbalance: imbalance, valid: true}
}

// mergedChildrenSize checks if left can hold all of right before a merge.
// It counts the bytes that the merged node would write. It changes no node.
// For leaves, the parent key already occurs in right. For branches, the parent
// key moves into the merged node, along with the child page IDs.
func mergedChildrenSize(parent *MutableNode, separatorIndex int, left, right *MutableNode) int64 {
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
func redistributeChildren(parent *MutableNode, separatorIndex int, left, right *MutableNode, pageSize, minimumSize int64) bool {
	var ok bool
	if left.IsLeaf() {
		ok = redistributeLeaves(parent, separatorIndex, left, right, pageSize, minimumSize)
	} else {
		ok = redistributeBranches(parent, separatorIndex, left, right, pageSize, minimumSize)
	}
	if !ok {
		return false
	}
	// These three pages now have different bytes. Their old checksums no
	// longer match them.
	left.header.Checksum = 0
	right.header.Checksum = 0
	parent.header.Checksum = 0
	return true
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
// mergeChildren only reads right, so right can borrow committed storage.
//
// For leaves, the parent's key is already the first key in right. For
// branches, that key exists only in the parent. When branches join, the key
// moves down between their two groups of keys.
func mergeChildren(parent *MutableNode, separatorIndex int, left, right *MutableNode) {
	left.encodedSize = 0
	parent.encodedSize = 0
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

// WALBodyOffset selects the type-specific start of the compact WAL payload.
func (n *MutableNode) WALBodyOffset() int {
	if n.IsLeaf() {
		return leafBodyOffset(n)
	}
	return branchBodyOffset(n)
}

// VisitNodeReferences visits child nodes and continuation pages separately.
// visit receives branch children or nested bucket roots. visitContinuation
// receives storage pages of this node; these pages are not tree children.
// Pass nil for visitContinuation when only child references are needed.
func VisitNodeReferences(node *MutableNode, visit, visitContinuation func(page.ID) error) error {
	if visitContinuation != nil {
		for offset := 1; offset < node.StoragePageCount(); offset++ {
			if err := visitContinuation(node.PageID() + page.ID(offset)); err != nil {
				return err
			}
		}
	}
	if node.IsLeaf() {
		return visitLeafReferences(node, visit)
	}
	return visitBranchReferences(node, visit)
}

// OwnBorrowedValues copies temporary values so the node can enter committed memory.
func (n *MutableNode) OwnBorrowedValues() {
	for i := range n.entries {
		if n.entries[i].borrowValue {
			n.entries[i].value = cloneValue(n.entries[i].value)
			n.entries[i].borrowValue = false
		}
	}
}

// OwnImageBytes copies entries borrowed from an encoded node image.
// Call it before persistence can overwrite or unmap that image. Entries replaced
// or deleted during the transaction no longer need a copy.
func (n *MutableNode) OwnImageBytes() {
	for index := range n.entries {
		entry := &n.entries[index]
		if entry.borrowedKey {
			entry.key = slices.Clone(entry.key)
			entry.borrowedKey = false
		}
		if entry.borrowedImage {
			entry.value = cloneValue(entry.value)
			entry.borrowedImage = false
		}
	}
}

// ReplaceReferences changes child or nested-bucket roots named in moved.
// It returns the number of references changed. The caller must own n.
func (n *MutableNode) ReplaceReferences(moved map[page.ID]page.ID) (int, error) {
	repaired := 0
	if !n.IsLeaf() {
		for index, child := range n.Children {
			if target, ok := moved[child]; ok {
				n.Children[index] = target
				repaired++
			}
		}
		n.header.Checksum = 0
		return repaired, nil
	}
	for index := range n.entries {
		entry := n.entries[index]
		if entry.flags&BucketLeafFlag == 0 {
			continue
		}
		root, err := page.DecodeID(entry.value)
		if err != nil {
			return 0, err
		}
		if target, ok := moved[root]; ok {
			if err := n.InsertOrReplaceLeafEntry(NewEntry(entry.flags, entry.key, page.EncodeID(target))); err != nil {
				return 0, err
			}
			repaired++
		}
	}
	return repaired, nil
}
