// Package btree implements B+tree nodes and tree operations through a Store.
//
// Leaf code is in leaf_node.go and leaf_codec.go.
// Branch code is in branch_node.go and branch_codec.go.
// Continuation pages use continuation_node.go and continuation_codec.go.
// Shared node data, decoded reads, and balance rules are in node.go.
// node_codec.go contains what leaves and branches share: the node header, checksums, page encoding, the entry rules, and the entry points for decode, lookup, and reference checks.
// leaf_codec.go and branch_codec.go contain the descriptor layout of their type, the code that writes it, and one entry loop for each read operation.
// continuation_codec.go reads and writes the pages of a continued node, and it reads a continued body in place.
// Tree reads, inserts, deletes, and repair are in tree.go.
// Ordered reads are in cursor.go.
//
// The body codecs support both decoded nodes and direct reads from encoded bytes.
// Direct point reads of a node in one page use binary search and return slices of the input bytes.
// Direct point reads of a continued node read only the pages that they need and return copies.
//
// Main-file readers accept an explicit checksum-check option.
// When true, they verify nonzero fields before using page bytes.
// When false, they ignore the checksum field and retain structural checks.
// Zero marks an unchecked page. A calculated CRC32C of zero is stored as one.
// The database requires nonzero fields in enabled files.
// Disabled files can contain zero or residual nonzero fields.
// Compact WAL pages also have zero page checksums. WAL records protect their bytes.
package btree
