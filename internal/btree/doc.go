// Package btree implements B+tree nodes and tree operations through a Store.
//
// Leaf code is in leaf_node.go and leaf_codec.go.
// Branch code is in branch_node.go and branch_codec.go.
// Continuation pages use continuation_node.go and continuation_codec.go.
// Shared node data, decoded reads, and balance rules are in node.go.
// Each type owns its page encoding, decoding, and mapped reads.
// Leaf and branch codecs also own their compact WAL images.
// node_codec.go selects a codec and provides shared byte and checksum operations.
// Tree reads, inserts, deletes, and repair are in tree.go.
// Ordered reads are in cursor.go.
//
// The body codecs support both decoded nodes and direct reads from encoded bytes.
// Direct point reads use binary search and return slices of the input bytes.
//
// A main-file page with a nonzero checksum is checked before its bytes are used.
// Zero marks an unchecked page. A calculated CRC32C of zero is stored as one.
// The database checks that each page matches its stored checksum mode.
// Compact WAL pages also have zero page checksums. WAL records protect their bytes.
package btree
