// Package kvlite provides an embedded key/value database with B+tree storage.
//
// A database stores every user key/value pair inside a named bucket. After opening one with [Open], callers can use [DB.Update] for atomic writes and [DB.View] for read-only access. For one value in a top-level bucket, [DB.Put] and [DB.Get] provide a simpler form of those transactions.
//
// Update and View manage each transaction from start to finish, so a [Tx] and every [Bucket] obtained from it are valid only while the callback runs. If an Update callback returns an error, KVLite discards its changes instead of committing them.
//
// KVLite copies the keys and values that it stores, so callers can reuse or change those byte slices after a write returns. [DB.Get] returns a new value slice. [Bucket.Get] returns a read-only value that is valid only while its transaction callback runs.
//
// A [DB] accepts concurrent method calls. Read-only transaction callbacks can run together, but a write callback does not overlap another transaction callback. Concurrent [DB.Update] calls in [SyncFull] mode can share one write-ahead log append and storage synchronization.
//
// KVLite does not lock database files. An application must not open the same path more than once at the same time.
//
// Writes first go to a write-ahead log beside the database file. When Close succeeds, it checkpoints committed data into the database file and removes the log, so callers must check its error.
package kvlite
