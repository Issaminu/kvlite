![KVLite logo](assets/logo/kvlite-logo.svg)

A blazingly-fast embedded key/value database for Go.

![Go Reference](https://pkg.go.dev/badge/github.com/Issaminu/kvlite.svg) ![Go 1.26](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go) ![Benchmark results](https://img.shields.io/badge/benchmarks-results-2563EB) ![MIT License](https://img.shields.io/badge/license-MIT-green) ![Ask DeepWiki](https://deepwiki.com/badge.svg)

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

Always check the error from `DB.Close`. With the WAL enabled, a writable close moves committed data into the main file and removes the WAL. `SyncNone` with `DisableWAL: true` applies changes to the main file during each write.

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

## Batch operations

Use `DB.GetBatch`, `DB.PutBatch`, and `DB.DeleteBatch` for several keys in one existing top-level bucket.
Each call uses one transaction.
Batch reads return owned values in the input order, with nil for missing keys and non-nil empty slices for stored empty values.
For repeated put keys, the last entry supplies the stored value.

```go
bucket := []byte("users")
if err := db.PutBatch(bucket, []kvlite.KeyValue{
    {Key: []byte("42"), Value: []byte("Ada")},
    {Key: []byte("43"), Value: []byte("Grace")},
}); err != nil {
    return err
}
values, err := db.GetBatch(bucket, [][]byte{[]byte("43"), []byte("42")})
if err != nil {
    return err
}
fmt.Println(string(values[0]), string(values[1]))
if err := db.DeleteBatch(bucket, [][]byte{[]byte("42"), []byte("43")}); err != nil {
    return err
}
```

Inside an existing `View` or `Update` callback, use the corresponding `Bucket` methods.
`Bucket.GetBatch` returns read-only values borrowed until the callback ends.
`Bucket.PutBatch` and `Bucket.DeleteBatch` require `Update`; return their errors from the callback to discard the transaction.
Batch methods preserve caller input order and do not retain inputs after returning.

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

A write that fails after it changed the transaction, for example because of a storage read error, stops the transaction. Every later operation in the callback returns `ErrTxFailed`, and `DB.Update` discards the changes and returns `ErrTxFailed`, also when the callback returns `nil`. A write that rejects its input, for example with `ErrKeyTooLarge`, changes nothing, so the callback can continue. LMDB and PostgreSQL treat a failed write in a transaction the same way.

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

This option covers each complete leaf or branch node, including its tail pages and final padding. The file stores the mode. `Open` uses that stored mode for an existing file and ignores this option. Backup preserves the mode. Use `DB.SetDataPageChecksums` to change an existing file:

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
| `SyncNone`   | Uses the WAL without automatic storage synchronization. A system crash or power failure can lose data or corrupt the database. Use it only for data that can be rebuilt. |

These modes select when operations synchronize committed data to storage.

Use `DB.Sync` to flush committed main-file and WAL data without closing the database or forcing a checkpoint. It works in `SyncFull`, `SyncNormal`, and `SyncNone`, so you can explicitly request a storage sync regardless of the configured mode. It does not change the synchronization mode for later writes, although please call it outside transaction callbacks. Also, it is a no-op for memory databases.

Use `DB.Checkpoint` to move committed WAL data into the main file and clear the WAL without closing the database. In `SyncFull` and `SyncNormal`, it synchronizes the WAL and main file before clearing the WAL, even below the automatic checkpoint threshold. It waits for active transactions and backups, and blocks database operations during the checkpoint. Call it outside transaction callbacks. In `SyncNone`, it moves the data without storage synchronization. It is a no-op with `DisableWAL: true` and in memory storage; use `DB.Sync` when you need an explicit storage sync.

Select the durability mode in the open options:

```go
options := &kvlite.Options{
	Synchronous: kvlite.SyncFull,
}
```

All synchronization modes use the WAL by default. To bypass it for disposable data, select `SyncNone` and set `DisableWAL: true`:

```go
options := &kvlite.Options{
	Synchronous: kvlite.SyncNone,
	DisableWAL:  true,
}
```

`DisableWAL` defaults to `false` and can only be enabled with `SyncNone`; enabling it with any other mode returns `ErrInvalid`. With the WAL enabled, writes can proceed during backups and `CheckpointThresholdBytes` controls automatic checkpoints. Disabling the WAL writes directly to the main file, waits for active backups, and ignores the checkpoint threshold. A failed direct write or a process crash can then damage earlier data. `SyncNone` skips automatic storage synchronization in both cases; enabling the WAL does not provide power-loss protection.

## Crash testing with Blackout

Blackout is KVLite's deterministic simulation test. It runs KVLite on simulated storage, crashes the process, cuts the power, and damages the files. After each crash, it opens the database again and compares the result with a model. It is inspired by the designs of [TigerBeetle's VOPR](https://docs.tigerbeetle.com/concepts/safety/).

One 64-bit seed selects the database options, the workload, the time of each crash, and each decision of the simulated storage. The same seed and code give the same run, so each failure has a command that replays it.

#### Faults

| Fault             | What the simulated storage does                                                                                                                                                        |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Process crash     | Stops the process between two operations or inside a file call. Completed writes stay in the simulated operating-system cache.                                                         |
| Power failure     | Keeps the synced bytes and a random part of the unsynced writes. Each sector of 512 bytes or 4 KiB keeps a prefix of its writes, and sectors have no order. Unsynced truncates, file lengths, and names can stay or go. |
| Failed call       | A read, write, sync, truncate, open, or remove returns an error. A failed write can still write a prefix, and a failed truncate or remove can still take effect.                        |
| Failed sync       | Some unsynced changes become durable, and the others are lost for good, as on Linux after a writeback error. Some runs use the ext4 and XFS behavior, and others use the Btrfs behavior. |
| Full disk         | For a short time, each write that grows a file fails with "no space left on device".                                                                                                  |
| Read corruption   | A read of the main file returns one flipped bit. The stored bytes stay correct.                                                                                                        |
| Bit rot           | Bits of the stored main file change between a stop and the next open.                                                                                                                 |
| Misdirected write | A power failure moves a kept write of a file that is not the WAL 1 to 4 blocks of 4 KiB from its correct offset.                                                                                    |

#### Checks

- A model holds the latest state and each older state that recovery can still return.
- Each read must match the model.
- Each recovery must return exactly one state that the model allows. After a power failure, a commit that returned success under `SyncFull` must still be there.
- After bit rot or a misdirected write, the next open must fail or return an allowed state. It must never return wrong data.
- A read-only recovery and a writable recovery of the same files must show the same state.
- At the end of each run, all faults stop. Each operation must then succeed, and a clean close and reopen must keep the latest state.

`SyncNone` is generated with the WAL enabled and disabled. WAL-backed sessions retain process crashes and I/O faults. Direct-write sessions use healthy storage without injected crashes. Both exclude power failures and failed syncs until a syncing session makes the storage durable. Coverage requires both WAL settings with data-page checksums on and off, plus rejection of invalid `DisableWAL` combinations without file changes. Failed runs do not count toward coverage.

#### Continuous, non-stop chaos simulations

- Each push to `main` and each pull request runs 200 new seeds, the fixed seeds, and the tests of Blackout itself ([workflow](.github/workflows/blackout.yml)).
- The Blackout [supervisor](internal/blackout/supervisor/doc.go) runs random seeds without end on a small server with 2 ARM CPUs. It tests each new commit of `main`. It makes a failing seed shorter, and it opens a GitHub issue with the seed, the report, and the replay command. These issues have the label [`blackout`](https://github.com/Issaminu/kvlite/issues?q=label%3Ablackout).

#### Run Blackout

```sh
# Run seeds 1 to 32.
BLACKOUT=1 go test -run 'TestBlackout$' .

# Run 200 seeds from seed 1000.
BLACKOUT=1 go test -run 'TestBlackout$' -blackout.start=1000 -blackout.seeds=200 .

# Replay one seed and print its last 200 steps.
go test -run 'TestBlackout$' -blackout.seed=SEED -blackout.steps=STEPS -blackout.trace=200 -v .

# Run Blackout supervisor with random seeds without end with 2 workers.
go run ./internal/blackout/supervisor -workers 2 -out /tmp/blackout
```

#### Limits

- The faults come from a model of storage. Real hardware can fail in ways that the model does not include.
- The simulated storage has no file locks and no renames.
- When the data-page checksums option is disabled, KVLite cannot find a damaged key or value byte. Blackout then puts bit rot only on the metadata pages and the first allocation page, which always have checksums.
- A seed runs one goroutine at a time, so that it can replay. `TestBlackoutStress` runs goroutines at the same time, but its schedule does not repeat, so its failures may not replay.

## Statistics

`DB.Stats()` returns storage state and cumulative write counters for the current open handle. Counters cover work by this handle, including recovery checkpoints during open. They start again with each new handle. Reading statistics scans allocation metadata, counts the pages covered by committed WAL images, and checks the main-file and WAL sizes. It does not start a checkpoint or scan tree pages. Do not call it from a transaction callback.

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

Inside a callback, use `bucket.Stats()` for page counts, plain entry counts, tree depth, and used bytes. It scans the selected bucket and all nested buckets. It includes earlier writes in the same transaction. A large scan can delay a writer. `KeyN` excludes bucket names. `BucketN` includes the selected bucket. `Depth` is the greatest individual tree depth. Bucket nesting does not add tree levels. `KeyBytes` includes leaf keys and bucket names. `ValueBytes` includes plain values. Page counts exclude continuation pages. Storage byte counts include all required pages. Each node has one header; its tail pages have no separate header or page ID list. Inside a write callback, these counts include continuation pages needed at commit. Used page bytes include headers and exclude padding. Subtract used bytes from reserved bytes to calculate unused space.

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
| `DisableWAL`               | `false`                            | Keeps WAL enabled; `true` bypasses it only with `SyncNone`.         |
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

The current [KVBench report](BENCHMARKS.md) contains the median of 15 measured `large --workloads=all --storage=volume` runs at commit `2196993`. At eight clients, KVLite delivered **6.25x the point-read throughput of bbolt** and **24.64x the throughput of Redis**. Its 1.00 µs p99 latency gave it a **5.71x advantage over bbolt** and a **100.42x advantage over Redis**. KVLite also led the highlighted durable point-update, durable point-insert, mixed-work, no-commit-sync key-delete, large-key read, and clean-open cases.

#### Highlights

| Workload | KVLite | bbolt | Redis | Highlight |
| --- | ---: | ---: | ---: | --- |
| Random point reads, eight clients | 5,892,288 keys/s | 942,708 keys/s | 239,126 keys/s | **KVLite: 6.25x bbolt; 24.64x Redis** |
| Point-read p99, eight clients | 1.00 µs | 5.71 µs | 100.42 µs | **KVLite: 5.71x advantage over bbolt; 100.42x over Redis** |
| Durable random point updates, batch of 100 | 107,962 keys/s | 22,566 keys/s | 51,327 keys/s | **KVLite: 4.78x bbolt; 2.10x Redis** |
| Durable random point inserts, batch of 100 | 95,009 keys/s | 18,065 keys/s | 47,223 keys/s | **KVLite: 5.26x bbolt; 2.01x Redis** |
| Durable mixed work, 95% reads | 96,657 operations/s | 8,584 operations/s | 13,956 operations/s | **KVLite: 11.26x bbolt; 6.93x Redis** |
| No-commit-sync mixed work, 95% reads | 1,835,915 operations/s | 363,121 operations/s | 226,411 operations/s | **KVLite: 5.06x bbolt; 8.11x Redis** |
| No-commit-sync random key deletes, one client | 385,900 keys/s | 77,531 keys/s | 107,542 keys/s | **KVLite: 4.98x bbolt; 3.59x Redis** |
| Random reads with 32 KiB keys | 75,805 keys/s | 64,127 keys/s | 37,584 keys/s | **KVLite: 1.18x bbolt; 2.02x Redis** |
| Open an existing clean database | 91.55 µs | 2.54 ms | Not comparable | **KVLite: 27.69x advantage** |

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

A checkpoint moves committed WAL data into the main database file. Backup pauses checkpoints until all active backups finish. The WAL can grow during that time. Saved WAL page data also remains in memory until the backup finishes. `Close` waits for active backups. Writes with `DisableWAL: true` also wait for active backups before their callbacks run. Do not call `Backup` inside a transaction callback.

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

To run more seeds or replay a failure, see [Crash testing with Blackout](#crash-testing-with-blackout).

CI also runs the tests on Linux, Windows, and macOS, on amd64 and Arm, and on 32-bit builds. It runs `go vet` on Linux amd64, and it builds KVLite for FreeBSD, OpenBSD, NetBSD, Solaris, and illumos. A Linux amd64 machine can run the 32-bit tests directly. On any machine, `go vet` with another `GOOS` and `GOARCH` checks that the code builds for that platform:

```sh
GOARCH=386 go test ./... -count=1
GOOS=windows GOARCH=386 go vet ./...
GOOS=linux GOARCH=arm go vet ./...
```

## License

KVLite is available under the [MIT License](LICENSE).
