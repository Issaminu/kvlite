// Package btree implements B+tree nodes and tree operations through a Store.
//
// Node provides read-only search and entry access over encoded or decoded storage.
// MutableNode holds private editable lists. A transaction supplies permission through Store.EditNode and owns surviving image bytes before persistence.
// Entry keeps each leaf key, value, and kind together.
//
// NodeReader has one read operation. Storage resolves committed memory, WAL, and main-file data. The transaction overlays its own and earlier batch edits.
// Tree owns traversal, parent links, splitting, merging, and reference repair.
// Cursor keeps only a path and position; nodeEntries owns encoded entry bounds.
//
// node.go contains read-facing operations; mutable_node.go contains editable node data and local balance operations. Node encoding stays in node_codec.go, leaf_codec.go, and branch_codec.go. Tree operations live in tree.go.
// Searches that keep no state (point, batch, and range reads) live in search.go.
// Ordered cursor movement lives in cursor.go.
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
