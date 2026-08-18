// Package kvlite provides an embedded key/value database with B+tree storage.
//
// A database stores every user key/value pair inside a named bucket. After opening one with [Open], callers can use [DB.Update] for atomic writes and [DB.View] for read-only access. For one value in a top-level bucket, [DB.Put] and [DB.Get] provide a simpler form of those transactions.
//
// Update and View manage each transaction from start to finish, so a [Tx] and every [Bucket] obtained from it are valid only while the callback runs. If an Update callback returns an error, KVLite rolls back its changes instead of committing them.
//
// KVLite copies the keys and values that it stores, so callers can reuse or change those byte slices after a write returns. A successful lookup returns a new value slice for the same reason.
//
// A [DB] is not safe for concurrent use, so the caller must serialize all operations, including [DB.Close]. KVLite also does not lock database files, which means an application must not open the same path more than once at the same time.
//
// Writes first go to a write-ahead log beside the database file. When Close succeeds, it checkpoints committed data into the database file and removes the log, so callers must check its error.
package kvlite
