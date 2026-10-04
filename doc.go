// Package kvlite provides an embedded key/value database.
//
// # Databases and buckets
//
// A [DB] stores data in a file. Each key and value belongs to a named [Bucket]. A bucket can also contain other buckets.
//
// Use [Open] to open a database. [DB.Get], [DB.Put], and [DB.Delete] read, write, and remove one value in an existing top-level bucket. [DB.DeleteBucket] removes a top-level bucket.
//
// # Transactions
//
// A transaction groups related database operations into one unit. [DB.View] starts a read-only transaction. [DB.Update] starts a transaction that can read and write.
//
// View gives its callback one unchanged view of the database. Update lets its callback read its own writes, including deletes. If an Update callback returns nil, KVLite commits all of its changes together. If it returns an error or panics, KVLite discards all of its changes.
//
// The callback receives a [Tx]. Buckets, cursors, keys, and values obtained from that Tx belong to the transaction. Use them only before the callback returns.
//
// Do not start another transaction from inside a View or Update callback. Use the Tx and buckets that the callback already has.
//
// Several View callbacks can run at the same time. Update waits for active View callbacks to finish. New View callbacks wait while Update runs. Keep transaction callbacks short when other code must use the same DB.
//
// # Key order and byte slices
//
// KVLite compares keys one byte at a time. [Cursor], [Bucket.ScanPrefix], and [Bucket.ScanRange] use this order.
//
// Use [Bucket.ScanPrefix] or [Bucket.ScanRange] inside [DB.View] to scan one unchanged database state. Use them inside [DB.Update] when the scan belongs to a transaction that also writes. A scan does not change its bucket. Do not change that bucket until the scan returns.
//
// KVLite copies keys and values when it stores them. The caller can reuse or change its input slices after a write returns. [DB.Get] also returns a copy. [Bucket.Get] and [Cursor] return read-only data owned by their transaction. Use [bytes.Clone] to keep that data after the transaction ends.
//
// # Delete and page reuse
//
// [Bucket.Delete] removes a plain key from a bucket in a write transaction. [DB.Delete] does the same in an existing top-level bucket. A missing key causes no change. Delete does not remove a nested bucket.
// [Bucket.DeleteBatch] removes several plain keys in one write transaction. It groups the keys by tree leaf.
//
// [Tx.DeleteBucket] removes a top-level bucket in a write transaction. [Bucket.DeleteBucket] removes a nested bucket. Both operations remove all child buckets and values. A handle to a removed bucket cannot be used again in that transaction.
//
// A delete can make tree pages free. KVLite records free pages in the database file and can use them for later writes. A write cannot reuse a page retired by the same transaction. If the free pages are at the end of the file, a checkpoint can shorten the file. [DB.Stats] reports page counts and page reuse.
//
// Each key and value entry must fit in one database page. KVLite does not store large entries across pages.
//
// # Errors
//
// KVLite defines named errors for its own failure conditions. Use [errors.Is] to check a named error, including one that has more detail. File operations can return operating-system errors. A transaction can return an error from its callback.
//
// # Files and closing
//
// KVLite locks the database file while a [DB] is open. A writable DB prevents another KVLite DB from opening the same file. Read-only DBs can open the same file together.
//
// Committed writes first go to a write-ahead log beside the database file. [DB.Close] moves committed data into the database file and removes the log. Always check the error from Close.
package kvlite
