// Package kvlite provides an embedded key/value database with B+tree storage.
//
// A database stores values in its top-level key space or in named, nested buckets. Open a database with [Open], use [DB.Update] for atomic writes, and use [DB.View] for read-only access. [DB.Put] and [DB.Get] are convenience methods for one value in the top-level key space.
//
// Update and View create managed transactions. A [Tx] and every [Bucket] obtained from it are valid only while the transaction callback runs. Update commits only when the callback returns nil. It rolls back when the callback returns an error.
//
// KVLite copies the keys and values that it stores. Each successful lookup also returns a new value slice. Callers can reuse or change their byte slices after a call returns.
//
// A [DB] is not safe for concurrent use. The caller must serialize all operations, including [DB.Close]. KVLite does not lock database files, so an application must not open the same path more than once at the same time.
//
// Writes first go to a write-ahead log beside the database file. Close checkpoints committed data into the database file and removes the log. Callers must check the error from Close.
package kvlite
