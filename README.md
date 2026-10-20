<p align="center">
  <img src="assets/logo/kvlite-logo.svg" alt="KVLite logo" width="480">
</p>


A blazingly-fast embedded key/value database for Go.

[![Go Reference](https://pkg.go.dev/badge/github.com/Issaminu/kvlite.svg)](https://pkg.go.dev/github.com/Issaminu/kvlite) [![Go 1.26](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go)](go.mod) [![Benchmark results](https://img.shields.io/badge/benchmarks-results-2563EB)](BENCHMARKS.md) [![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE) [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Issaminu/kvlite)

KVLite uses a paged B+ tree. It supports transactions, nested buckets, key and bucket deletion, page reuse, ordered cursors, prefix scans, range scans, and read-only database handles.

> [!WARNING]
> KVLite does not have a stable release yet. Its API and file format can change between pre-release versions. For production usage, wait for the 1.0 release.



## Install

KVLite requires Go 1.26 or a later compatible release.

```sh
go get github.com/Issaminu/kvlite@latest
```



## Quick start

This program creates a bucket, stores one value, and reads it back.

```go
package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/Issaminu/kvlite"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	db, err := kvlite.Open("app.db", 0o600, nil)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	err = db.Update(func(tx *kvlite.Tx) error {
		users, err := tx.Bucket([]byte("users"))
		if errors.Is(err, kvlite.ErrBucketNotFound) {
			users, err = tx.CreateBucket([]byte("users"))
		}
		if err != nil {
			return err
		}
		return users.Put([]byte("42"), []byte("Ada"))
	})
	if err != nil {
		return err
	}

	value, err := db.Get([]byte("users"), []byte("42"))
	if err != nil {
		return err
	}
	fmt.Println(string(value))
	return nil
}
```

The program prints:

```text
Ada
```

Always check the error from `DB.Close`. A writable file close moves committed data into the main database file and removes the write-ahead log.

## Memory storage

Use `OpenMemory` to create an empty database with no file operations:

```go
db := kvlite.OpenMemory()
```

Use the same buckets, transactions, cursors, and scans as in the quick start. Each call creates an independent, writable database. Call `DB.Close` when finished. Closing the database discards all data. Data is also lost when the process ends. `DB.Path` returns an empty string.

Memory storage supports large keys and values with the same limits as file storage. It keeps complete decoded tree nodes in memory. It supports transaction rollback and concurrent calls on one `DB`.

`DB.Stats` and `Bucket.Stats` report logical page sizes, page counts, and page reuse. These counts include reserved metadata and allocation pages. They do not measure Go heap use. File, WAL, and checkpoint fields are zero. `DB.Backup` returns an error that matches `errors.ErrUnsupported` without file operations.

Storage and synchronization are separate choices. `Open` selects file storage. Its `Synchronous` option controls when file data is synchronized to storage. `SyncNone` still writes files. `OpenMemory` selects memory storage and needs no sync options.

## Features

- Named and nested buckets store byte keys and values.
- File storage and memory storage use the same data operations.
- Delete plain keys or whole buckets. Later writes can reuse freed pages.
- Read-only and read-write transactions provide a consistent view and atomic changes.
- Cursors move forward or backward in byte-key order.
- Prefix and half-open range scans visit entries in byte-key order.
- A redo write-ahead log supports recovery after an unclean close.
- CRC32C checksums protect metadata, allocation pages, and log records. Data-page checksums are optional.
- Two metadata pages let KVLite recover when one valid copy remains.
- One open `DB` accepts concurrent method calls.
- Durable concurrent updates can share one log write and storage sync.
- Read-only handles use shared file locks. Writable handles use an exclusive file lock.

After a bucket exists, use `DB.Put`, `DB.Get`, and `DB.Delete` for one-key operations:

```go
if err := db.Put([]byte("users"), []byte("42"), []byte("Ada")); err != nil {
	return err
}

value, err := db.Get([]byte("users"), []byte("42"))
if err != nil {
	return err
}
fmt.Println(string(value))
```



## Data model

A database contains named top-level buckets. A bucket can contain key/value pairs and nested buckets. A user value always belongs to a bucket.

`DB.Get`, `DB.Put`, and `DB.Delete` provide short operations for an existing top-level bucket. `DB.DeleteBucket` removes a top-level bucket and its contents. Use `DB.View` or `DB.Update` when several operations must use one database state.

`DB.Delete` removes one plain key. A missing key causes no change. Use `Bucket.Delete` in `DB.Update` when the delete must be in the same transaction as other writes.

Use `Bucket.DeleteBatch` in `DB.Update` to remove several keys at once. KVLite groups the keys by tree leaf before it removes them.

```go
if err := db.Delete([]byte("users"), []byte("42")); err != nil {
	return err
}
```

Use `Tx.DeleteBucket` to remove a top-level bucket within a write transaction. Use `Bucket.DeleteBucket` to remove a nested bucket. Both methods also remove child buckets. A missing bucket returns `ErrBucketNotFound`.

Keys use byte order. A shorter key sorts before a longer key when the shorter key is its prefix. For example, `user/1` sorts before `user/10`.

Create a nested bucket inside a write transaction:

```go
err := db.Update(func(tx *kvlite.Tx) error {
	users, err := tx.CreateBucket([]byte("users"))
	if err != nil {
		return err
	}

	sessions, err := users.CreateBucket([]byte("sessions"))
	if err != nil {
		return err
	}
	return sessions.Put([]byte("current"), []byte("session-1"))
})
if err != nil {
	return err
}
```



## Transactions

`DB.View` starts a read-only transaction. Each read in its callback sees the same database state.

`DB.Update` starts a read-write transaction. Its callback can read its own writes. KVLite commits all changes when the callback returns `nil`. It discards all changes when the callback returns an error or panics.

Do not start a transaction inside another transaction callback. Use the `Tx` value that the callback receives.

A `Tx`, `Bucket`, or `Cursor` is valid only until its transaction callback returns. Do not save it or share it with another goroutine.

Both writes in this callback commit together:

```go
err := db.Update(func(tx *kvlite.Tx) error {
	users, err := tx.Bucket([]byte("users"))
	if err != nil {
		return err
	}
	if err := users.Put([]byte("42/name"), []byte("Ada")); err != nil {
		return err
	}
	return users.Put([]byte("42/role"), []byte("admin"))
})
if err != nil {
	return err
}
```



## Ordered reads

A cursor visits values and nested bucket names in key order. A nested bucket has a nil value. A stored empty value has a non-nil value with length zero.

Use `Cursor.First`, `Cursor.Last`, or `Cursor.Seek` to select an entry. Use `Cursor.Next` or `Cursor.Prev` to continue.

`Bucket.ScanPrefix` visits keys that start with one prefix. `Bucket.ScanRange` visits a half-open range. It includes the start key and excludes the end key. `Bucket.ScanReverse` visits every entry from the largest key to the smallest key.

Do not change a bucket while one of its cursors or scans is in use. Create a new cursor after a bucket change.

Scan only the entries below `user/`:

```go
err := db.View(func(tx *kvlite.Tx) error {
	users, err := tx.Bucket([]byte("users"))
	if err != nil {
		return err
	}
	return users.ScanPrefix([]byte("user/"), func(key, value []byte) error {
		fmt.Printf("%s=%s\n", key, value)
		return nil
	})
})
if err != nil {
	return err
}
```



## Byte-slice ownership

KVLite copies keys and values during a write. The caller can reuse or change the input slices after the write returns.

`DB.Get` returns a copy. The caller can keep and change it.

`Bucket.Get`, `Cursor`, `Bucket.ScanPrefix`, `Bucket.ScanRange`, and `Bucket.ScanReverse` return or pass read-only slices owned by the transaction. Use `bytes.Clone` when data must remain valid after the callback returns.

Clone a transaction-owned value before the transaction ends:

```go
var saved []byte
err := db.View(func(tx *kvlite.Tx) error {
	users, err := tx.Bucket([]byte("users"))
	if err != nil {
		return err
	}
	value, err := users.Get([]byte("42"))
	if err != nil {
		return err
	}
	saved = bytes.Clone(value)
	return nil
})
if err != nil {
	return err
}
fmt.Println(string(saved))
```



## Durability and recovery

This section applies to file databases opened with `Open`.

For a database at `app.db`, KVLite can use two files:


| File         | Purpose                                                                      |
| ------------ | ---------------------------------------------------------------------------- |
| `app.db`     | Stores metadata, page allocation data, and fixed-size B+ tree pages.         |
| `app.db-wal` | Stores committed tree, allocation, and metadata changes until a checkpoint. |


A writable open creates the log. A clean writable close removes it. An application or system failure can leave the log beside the main file.

`Open` checks WAL records in order. It stops at the first invalid record or transaction and recovers only earlier complete transactions. Unsupported file formats, invalid main-file data, and file I/O errors still return an error.

Each time the WAL starts again after a checkpoint or open, KVLite writes a new random salt in the WAL header. Every record checksum covers that salt. If a crash keeps new records but loses the step that cleared the WAL, the older records after them fail their checksums, and recovery stops before them. A zero-filled or incomplete WAL header holds no finished commit, so `Open` treats that WAL as empty.

Writable recovery follows the configured synchronization mode. With `SyncFull` or `SyncNormal`, it synchronizes the WAL, writes the recovered pages to the main file, synchronizes the main file, and then clears the WAL. With `SyncNone`, it writes the recovered pages and clears the WAL without synchronization. A write or required synchronization failure keeps the WAL for another attempt. Read-only recovery keeps the recovered pages in memory and does not change either file.

If a WAL write or sync fails during a commit, the commit returns an error that matches `ErrWALWriteFailed`. The failed transaction can still reach storage, so the commit can be saved or lost. The handle then stops: transactions, reads, writes, and backups return the same error. `Stats`, `Path`, and `Close` still work. Close the handle and open the database again. Recovery then uses the files to decide the result. Check the data before you repeat a change that is not safe to apply twice. A failed checkpoint does not stop the handle, because a later checkpoint writes every page again from memory.

Data-page checksum calculation and checks are disabled by default. Enable them when you create a file:

```go
db, err := kvlite.Open("app.db", 0o600, &kvlite.Options{
	DataPageChecksums: true,
})
```

This option covers leaf, branch, and continuation pages. The file stores the mode. `Open` uses that stored mode for an existing file and ignores this option. Backup preserves the mode. Use `DB.SetDataPageChecksums` to change an existing file:

```go
if err := db.SetDataPageChecksums(false); err != nil {
	return err
}
```

Disabling commits the metadata flag without scanning data pages. Untouched pages keep their old checksum fields. Changed pages receive zero. Disabled reads ignore either value.

Enabling waits for active backups and blocks other operations while it checks and protects allocated data pages. It checks retained nonzero checksums and calculates missing checksums. It commits the enabled flag only after conversion finishes. A conversion error leaves the mode disabled and permits a later retry. Checksums cannot prove that previously unchecked data was correct. Selecting the current mode does no conversion. Do not call this method inside a transaction callback. Read-only and memory databases cannot change the mode.

Without data-page checksums, damaged key or value bytes can pass unnoticed. Metadata, allocation pages, and WAL records retain their checksums in both modes. The option does not change storage synchronization or checkpoint ordering.

KVLite records which database pages are in use. A later transaction can reuse a tree page that an earlier transaction freed. When free pages reach the end of the file, a checkpoint can shorten the main file. `DB.Stats` reports allocated pages, reusable pages, retired pages, and reused pages.


| Mode         | Behavior                                                                                                                                                  |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SyncFull`   | Default. A non-empty update returns after KVLite synchronizes its log data to storage. Concurrent updates can share this work.                            |
| `SyncNormal` | Synchronizes at a checkpoint or clean close. A system failure can lose recent committed updates from the operating-system cache.                          |
| `SyncNone`   | Writes files without synchronization, including during recovery. A system failure can lose updates or damage the database. Use it only for data that can be rebuilt. |


These modes select when operations synchronize committed data to storage.

Select the durability mode in the open options:

```go
options := &kvlite.Options{
	Synchronous: kvlite.SyncFull,
}
```



## Statistics

`DB.Stats()` returns storage state and cumulative write counters for the current open handle. Counters cover work by this handle, including recovery checkpoints during open. They start again with each new handle. Reading statistics scans allocation metadata and checks the main-file and WAL sizes. It does not start a checkpoint or scan tree pages. Do not call it from a transaction callback.

The result includes page counts, logical database size, main-file size, WAL size, bytes appended since checkpoint, distinct pending WAL pages, and the checkpoint threshold. File sizes are byte lengths. They do not measure file-system block allocation. WAL bytes measure successful log appends, not physical storage writes.

A memory database reports logical page counts and `DataSize`. These do not measure Go heap use. Its file, WAL, and checkpoint fields are zero. `Bucket.Stats` uses the same logical page rules in both storage modes.

Normal reads do not collect statistics. There are no statistics locks, transaction counters, or phase timers. Write counters use the existing write lock. Detailed transaction collection is deferred.

Use `Stats.Sub` to measure work between two readings from the same open handle:

```go
before, err := db.Stats()
if err != nil {
	return err
}
if err := db.Put([]byte("users"), []byte("alice"), []byte("active")); err != nil {
	return err
}
after, err := db.Stats()
if err != nil {
	return err
}
work := after.Sub(&before)
fmt.Printf("WAL bytes: %d, commits: %d\n", work.WALBytesWritten, work.WALCommitCount)
```

`Sub` subtracts cumulative counters. It keeps the current storage state and checkpoint error. `WALCommitCount` counts successful nonempty commits. Several concurrent updates can share one commit. Failed commits and recovered transactions do not count. `WALBytesSinceCheckpoint` counts bytes appended in this handle, including a new WAL header. `WALSize` also includes any bytes recovered during open.

`CheckpointFailureCount` counts failed checkpoint attempts, including WAL cleanup and mapping failures. `LastCheckpointError` reports the last nonempty attempt. A later successful attempt clears the error but keeps the failure count. An automatic checkpoint failure does not fail a write that already committed. Close returns a checkpoint failure so the caller can retry it.

Inside a callback, use `bucket.Stats()` for page counts, plain entry counts, tree depth, and used bytes. It scans the selected bucket and all nested buckets. It includes earlier writes in the same transaction. A large scan can delay a writer. `KeyN` excludes bucket names. `BucketN` includes the selected bucket. `Depth` is the greatest individual tree depth. Bucket nesting does not add tree levels. `KeyBytes` includes leaf keys and bucket names. `ValueBytes` includes plain values. Page counts exclude continuation pages. Storage byte counts include their headers, page IDs, and required page space. Inside a write callback, these counts include continuation pages needed at commit. Used page bytes include headers and exclude padding. Subtract used bytes from reserved bytes to calculate unused space.

## Concurrency and file locks

Several `DB.View` callbacks can run at the same time. A `DB.Update` callback runs alone. It waits for active views. New views wait for it.

A writable handle takes an exclusive file lock. Read-only handles take shared locks and can run together. `Options.LockTimeout` limits how long `Open` waits for a conflicting lock. Its zero value waits without a time limit.

KVLite implements file locks for macOS, Linux, BSD systems, Solaris, and Windows. `Open` returns an unsupported-operation error on other systems.

Run independent read transactions on one open database:

```go
results := make(chan error, 2)
for range 2 {
	go func() {
		results <- db.View(func(tx *kvlite.Tx) error {
			_, err := tx.Bucket([]byte("users"))
			return err
		})
	}()
}

for range 2 {
	if err := <-results; err != nil {
		return err
	}
}
```



## Options

`Open` accepts `nil` options. It then uses these defaults:


| Option                     | Default                            | Effect                                                             |
| -------------------------- | ---------------------------------- | ------------------------------------------------------------------ |
| `ReadOnly`                 | `false`                            | Opens a writable database and creates it when needed.              |
| `LockTimeout`              | `0`                                | Waits without a time limit for a conflicting file lock.            |
| `Synchronous`              | `SyncFull`                         | Synchronizes log data before a non-empty update returns.           |
| `MaxWriteBatchSize`        | `100`                              | Limits the write callbacks in one `SyncFull` WAL commit.            |
| `CheckpointThresholdBytes` | About 1,000 operating-system pages | Starts a checkpoint after the committed log reaches the threshold. |

`MaxWriteBatchSize` counts callbacks, not bytes. Set it to `1` to prevent callbacks from sharing a WAL commit.

See the [package documentation](https://pkg.go.dev/github.com/Issaminu/kvlite) for the complete API and error contracts.

Set only the options that must differ from their defaults. Pass the value to `Open`:

```go
options := &kvlite.Options{
	LockTimeout:              2 * time.Second,
	CheckpointThresholdBytes: 64 << 20,
}
db, err := kvlite.Open("app.db", 0o600, options)
```

## Benchmarks

The current [KVBench report](BENCHMARKS.md) contains the median of 15 measured `large --workloads=all --storage=volume` runs at commit `689a4dc`. At eight clients, KVLite delivered **6.06x the point-read throughput of bbolt** and **24.29x the throughput of Redis**. Its 792 ns p99 latency gave it a **6.73x advantage over bbolt** and a **125.84x advantage over Redis**. KVLite also led the highlighted durable point-update, durable point-insert, mixed-work, no-commit-sync key-delete, large-key read, and clean-open cases.

#### Highlights

| Workload | KVLite | bbolt | Redis | Highlight |
| --- | ---: | ---: | ---: | --- |
| Random point reads, eight clients | 5,985,140 keys/s | 988,029 keys/s | 246,384 keys/s | **KVLite: 6.06x bbolt; 24.29x Redis** |
| Point-read p99, eight clients | 792 ns | 5.33 µs | 99.67 µs | **KVLite: 6.73x advantage over bbolt; 125.84x over Redis** |
| Durable random point updates, batch of 100 | 113,511 keys/s | 27,053 keys/s | 39,352 keys/s | **KVLite: 4.20x bbolt; 2.88x Redis** |
| Durable random point inserts, batch of 100 | 125,512 keys/s | 28,999 keys/s | 42,909 keys/s | **KVLite: 4.33x bbolt; 2.93x Redis** |
| Durable mixed work, 95% reads | 148,031 operations/s | 8,111 operations/s | 14,266 operations/s | **KVLite: 18.25x bbolt; 10.38x Redis** |
| No-commit-sync mixed work, 95% reads | 2,116,491 operations/s | 358,264 operations/s | 224,632 operations/s | **KVLite: 5.91x bbolt; 9.42x Redis** |
| No-commit-sync random key deletes, one client | 403,867 keys/s | 80,063 keys/s | 110,016 keys/s | **KVLite: 5.04x bbolt; 3.67x Redis** |
| Random reads with 32 KiB keys | 77,414 keys/s | 62,654 keys/s | 45,132 keys/s | **KVLite: 1.24x bbolt; 1.72x Redis** |
| Open an existing clean database | 76.70 µs | 2.96 ms | Not comparable | **KVLite: 38.62x advantage** |

The results apply only to the tested machine and workloads. The Docker volume path includes the OrbStack virtual machine and host storage path. It does not prove bare-metal device performance. Durable and no-commit-sync results have different guarantees. KVLite ran with the default `DataPageChecksums=false`. bbolt read 64 KiB and 1 MiB values faster than KVLite. See the [complete benchmark report](BENCHMARKS.md) and the [benchmark suite guide](benchmarks/kvbench/README.md).

Run the light benchmark profile in its Linux containers:

```sh
cd benchmarks/kvbench
./run-docker.sh
```




## Backup

Use `DB.Backup` to save one committed file database state to a new file. Reads and writes can continue during the copy. Writable and read-only file handles can both create backups. Memory databases return an error that matches `errors.ErrUnsupported` without file operations.

```go
if err := db.Backup("backup.db"); err != nil {
	return err
}
```

The backup contains the committed state selected before the copy starts. The backup includes committed data still in the write-ahead log (WAL). Writes committed after that point do not appear in the backup. The result is one database file that `Open` can use without a WAL file. When `Backup` returns success, all backup work is complete.

To restore, copy `backup.db` to a new path, such as `restored.db`, and open that copy with `Open`. Keep the backup if you need to restore it again. A writable handle can change the file that it opens.

The source handle's `Synchronous` option also controls the backup. With `SyncFull` or `SyncNormal`, Backup syncs the backup file and, except on Windows, its parent directory before it returns success. With `SyncNone`, it skips both sync calls. A system failure can then lose or damage the backup.

A checkpoint moves committed WAL data into the main database file. Backup pauses checkpoints until all active backups finish. The WAL can grow during that time. Saved WAL page data also remains in memory until the backup finishes. `Close` waits for active backups. Do not call `Backup` inside a transaction callback.

The destination and its `-wal` path must not exist. The parent directory must exist. Keep both paths unused by other processes until `Backup` returns. The file system must support hard links. Except on Windows, `SyncFull` and `SyncNormal` also require directory sync.

If the application stops during the copy, a temporary `.kvlite-backup-*` file can remain. Remove that incomplete file. The source uses normal WAL recovery on its next open. A failure after the destination becomes visible leaves a complete file at that path, but `Backup` still returns an error.

## Development

Run the main checks from the repository root:

```sh
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

These commands skip the Blackout tests: the simulation in `blackout_test.go`, the simulated-disk tests, and the `internal/blackout` packages. Set `BLACKOUT=1` to run them too:

```sh
BLACKOUT=1 go test ./... -count=1
```



## License

KVLite is available under the [MIT License](LICENSE).
