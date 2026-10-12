// Package btree implements B+tree nodes and tree operations through a Store.
//
// Leaf code is in leaf_node.go and leaf_codec.go.
// Branch code is in branch_node.go and branch_codec.go.
// Shared node data, decoded reads, and balance rules are in node.go.
// Shared page and WAL encoding, checksums, and reference reads are in node_codec.go.
// Tree reads, inserts, deletes, and repair are in tree.go.
// Ordered reads are in cursor.go.
//
// The body codecs support both decoded nodes and direct reads from encoded bytes.
// Direct point reads use binary search and return slices of the input bytes.
package btree
