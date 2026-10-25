// Package btree implements B+tree nodes and tree operations through a Store.
//
// Leaf code is in leaf_node.go and leaf_codec.go.
// Branch code is in branch_node.go and branch_codec.go.
// Shared node data, decoded reads, and balance rules are in node.go.
// node_codec.go contains what leaves and branches share: the node header, checksums, page encoding, the entry rules, and the entry points for decode, lookup, and reference checks.
// leaf_codec.go and branch_codec.go contain the descriptor layout of their type, the code that writes it, and one entry loop for each read operation.
// Tree reads, inserts, deletes, and repair are in tree.go.
// Ordered reads are in cursor.go.
//
// The body codecs support both decoded nodes and direct reads from encoded bytes.
// A node uses one header followed by its body in consecutive pages.
// Tail pages contain raw bytes without headers or page ID lists.
// The fixed 22-byte header stores type, first page ID, entry count, checksum, and body length.
// Storage page count is (22 + body length) divided by page size, rounded up.
// The last physical page ends with zero padding. A compact WAL image omits that padding.
// Direct point reads use binary search and borrow bytes from the complete node image.
// A caller with only the first page supplies a reader for the remaining pages.
//
// Main-file readers accept an explicit checksum-check option.
// When true, they verify the nonzero checksum over the complete padded node.
// When false, they ignore the checksum field and retain structural checks.
// Zero marks an unchecked page. A calculated CRC32C of zero is stored as one.
// The database requires nonzero fields in enabled files.
// Disabled files can contain zero or residual nonzero fields.
// Compact WAL node images have zero data checksums. WAL records protect their bytes.
package btree
