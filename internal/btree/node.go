package btree

import (
	"fmt"
	"slices"

	"github.com/Issaminu/kvlite/internal/page"
)

// NodeHeaderSize is the 18-byte encoded size of a B+tree node header.
const NodeHeaderSize = 18

// NodeType identifies the body layout of a B+tree node.
type NodeType uint16

const (
	// NodeTypeLeaf identifies a node that stores key and value entries.
	NodeTypeLeaf NodeType = 1
	// NodeTypeBranch identifies a node that stores separator keys and child page IDs.
	NodeTypeBranch NodeType = 2
)

// NodeHeader contains the fixed fields that identify and protect one B+tree node.
// All fields use little-endian encoding:
//
//	[0:2]  node type, uint16
//	[2:10] page ID, uint64
//	[10:14] entry count, uint32
//	[14:18] CRC32C checksum, uint32
//
// Checksum covers bytes [0:14] and [18:pageSize]. It includes the node body and zero padding.
// A node change can make Checksum and EntryCount stale. [EncodeNode] and [EncodeWALNode] write the current entry count. [EncodeNode] also calculates a new checksum before it returns encoded bytes.
type NodeHeader struct {
	Type       NodeType
	PageID     page.ID
	EntryCount uint32
	Checksum   uint32
}

// Node contains one B+tree node and its header.
type Node struct {
	header   *NodeHeader
	entries  []Entry
	Children []page.ID // for non-leaf nodes: len(Children) == len(entries)+1
}

func newNodeHeader(nodeType NodeType, pgid page.ID) *NodeHeader {
	return &NodeHeader{
		Type:   nodeType,
		PageID: pgid,
	}
}

// IsLeaf reports whether n stores key and value entries.
func (n *Node) IsLeaf() bool {
	return n.header.Type == NodeTypeLeaf
}

func (n *Node) PageID() page.ID {
	return n.header.PageID
}

func (n *Node) SetPageID(pgid page.ID) {
	n.header.PageID = pgid
	n.header.Checksum = 0
}

func (n *Node) EntryCount() int {
	return len(n.entries)
}

// EntryAt returns the entry at index without copying it.
// The returned entry refers to storage owned by the node.
// It remains valid until the node changes its entry list.
func (n *Node) EntryAt(index int) *Entry {
	return &n.entries[index]
}

// Clone returns a structural copy of n with the same page ID.
// The clone owns its entry and child lists but shares the immutable key and value bytes with n.
// Node changes must replace an entry or byte slice instead of changing shared bytes in place.
func (n *Node) Clone() *Node {
	header := *n.header
	return &Node{
		header:   &header,
		entries:  slices.Clone(n.entries),
		Children: slices.Clone(n.Children),
	}
}

func (n *Node) firstKey() []byte {
	if len(n.entries) == 0 {
		return nil
	}
	return n.entries[0].key
}

const (
	MaxKeySize   = 32768         // 32 KiB
	MaxValueSize = (1 << 31) - 2 // ~2 GiB
)

type Entry struct {
	flags uint32
	key   []byte
	value []byte
}

func NewEntry(flags uint32, key, value []byte) Entry {
	return Entry{flags: flags, key: key, value: value}
}

func (e Entry) Flags() uint32 {
	return e.flags
}

func (e Entry) Key() []byte {
	return e.key
}

func (e Entry) Value() []byte {
	return e.value
}

// EncodedSize reports how many bytes this entry occupies inside an encoded node.
// A leaf uses one 16-byte descriptor followed by its key and value.
// A branch uses one 8-byte descriptor, one child page ID, and its separator key.
// The branch node also stores one extra child page ID.
func (e Entry) EncodedSize(isLeaf bool) int {
	size := branchEntryDescriptorSize + page.IDSize + len(e.key)
	if isLeaf {
		size = leafEntryDescriptorSize + len(e.key) + len(e.value)
	}
	return size
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
