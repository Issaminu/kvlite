# KVLite benchmark suite

This module compares database operations through the public Go APIs of KVLite, bbolt, and Redis.

The suite uses Go's standard `testing.B` runner. This gives the suite automatic run calibration, allocation reports, repeated runs, workload filters, and Go profiles.

It also measures operation time at the client API boundary. It does not measure database open, initial data load, final validation, close, or deferred checkpoint work.

## Quick start

Run the adapter tests:

```sh
cd benchmarks/kvbench
go test ./...
```

Run all KVLite and bbolt benchmarks in durable mode:

```sh
go test -run '^$' -bench '^BenchmarkAcknowledgedOperations$' -benchmem -count=5
```

The default durability mode is `durable`. Redis is disabled unless `KVBENCH_REDIS_ADDR` is set.

## Requirements

- Go 1.26 or later.
- The KVLite source tree that contains this module.
- A dedicated Redis server only when Redis results are required.

The module uses the current KVLite checkout through a local `replace` directive. It pins bbolt 1.4.3 and go-redis 9.22.0. The Redis server version is not fixed by the suite. Record it with every saved result.

## What the suite measures

Each benchmark reports these values:


| Metric       | Meaning                                                                                |
| ------------ | -------------------------------------------------------------------------------------- |
| `ns/op`      | Nanoseconds for one benchmark operation. A batch is one operation.                     |
| `ack-keys/s` | Keys acknowledged per second. A batch counts each key.                                 |
| `MB/s`       | Value payload bytes per second. It excludes keys, protocol data, and storage metadata. |
| `B/op`       | Go heap bytes allocated per benchmark operation.                                       |
| `allocs/op`  | Go heap allocations per benchmark operation.                                           |


An acknowledged operation is an operation that returned success to the caller. The exact durability guarantee depends on the selected durability mode.

## Execution flow

Each sub-benchmark performs these steps:

1. Generate 10,000 deterministic key and value pairs.
2. Open a new KVLite or bbolt database in a temporary directory. For Redis, connect to the configured server and clear database 0.
3. Create the logical bucket when the engine needs one.
4. Load all 10,000 records in batches of 1,000.
5. Read one record and check its value.
6. Start the Go benchmark timer.
7. Run exactly `b.N` benchmark operations.
8. Stop the timer.
9. Count the stored keys and require exactly 10,000 keys.
10. Close the client or database during benchmark cleanup.

Only step 7 is measured.

All point writes overwrite existing keys. The current suite does not measure database growth from new inserts.

## Data set

Each key is 16 bytes. It contains two unsigned 64-bit integers in big-endian order. The first integer identifies the workload. The second integer identifies the record.

Values use deterministic byte patterns. The configured value size is the exact value slice length.

Random workloads use one fixed shuffle seed. This makes each engine receive the same key order in every run.

## Workloads

The suite currently runs these cases for each enabled engine:


| Operation   | Key order  | Value bytes           | Clients | Batch size or mix        |
| ----------- | ---------- | --------------------- | ------- | ------------------------ |
| Point read  | Random     | 32, 128, 1,024, 3,072 | 1       | One key                  |
| Point write | Random     | 32, 128, 1,024, 3,072 | 1       | One key                  |
| Point read  | Sequential | 128                   | 1       | One key                  |
| Point read  | Random     | 128                   | 8       | One key                  |
| Point write | Random     | 128                   | 32      | One key                  |
| Mixed       | Random     | 128                   | 8       | 95% reads and 5% writes  |
| Mixed       | Random     | 128                   | 8       | 50% reads and 50% writes |
| Batch write | Random     | 128                   | 1       | 10, 100, or 1,000 keys   |


The mixed workloads use a deterministic operation sequence. They do not use random decisions during the measured run.

For concurrent cases, the suite starts the requested number of goroutines behind one barrier. It divides `b.N` operations across those goroutines. The timer starts before the barrier releases them and stops after every goroutine returns.

## Matched engine operations

The adapters use the closest public operation and transaction boundary available in each engine:


| Logical operation | KVLite                | bbolt                 | Redis                                |
| ----------------- | --------------------- | --------------------- | ------------------------------------ |
| Point read        | `DB.Get`              | One read transaction  | `GET`                                |
| Point write       | `DB.Put`              | One write transaction | `SET`                                |
| Batch write       | One write transaction | One write transaction | `MULTI`/`EXEC` through `TxPipelined` |
| Key count         | Bucket cursor scan    | Bucket cursor scan    | `DBSIZE`                             |


KVLite and bbolt use one bucket named `kvbench`. Redis uses database 0.

Redis operations include the Go client and local network path. KVLite and bbolt are embedded libraries. The results therefore compare caller-visible operation cost, not storage-engine CPU cost alone.

## Durability modes

Set `KVBENCH_DURABILITY` before the run.


| Mode             | KVLite       | bbolt                      | Redis                                 |
| ---------------- | ------------ | -------------------------- | ------------------------------------- |
| `durable`        | `SyncFull`   | Default synchronous writes | AOF enabled with `appendfsync always` |
| `no-commit-sync` | `SyncNone`   | `NoSync=true`              | AOF enabled with `appendfsync no`     |


Run the relaxed mode:

```sh
KVBENCH_DURABILITY=no-commit-sync \
go test -run '^$' -bench '^BenchmarkAcknowledgedOperations$' -benchmem -count=5
```

`no-commit-sync` measures the time until the API returns success without an explicit storage sync. KVLite can perform checkpoint work during measured calls and during `Close`, but `SyncNone` does not sync storage. Close is outside the benchmark timer. Do not use this mode to compare total lifecycle cost.

## Redis setup and safety

Use a dedicated Redis instance. The benchmark runs `FLUSHDB` and `BGREWRITEAOF` before it loads data. These commands clear Redis database 0 and rewrite the server AOF.

The suite requires both environment variables below for a Redis run:

```sh
KVBENCH_REDIS_ADDR=127.0.0.1:6380
KVBENCH_REDIS_FLUSHDB=1
```

`KVBENCH_REDIS_FLUSHDB=1` is a required acknowledgement that the suite can clear the selected database.

The Redis server must allow `PING`, `CONFIG GET`, `INFO`, `FLUSHDB`, `BGREWRITEAOF`, `GET`, `SET`, `MULTI`, `EXEC`, and `DBSIZE`.

For durable mode, the suite requires these Redis settings:

```text
appendonly yes
appendfsync always
save ""
auto-aof-rewrite-percentage 0
no-appendfsync-on-rewrite no
```

For `no-commit-sync`, change only this setting:

```text
appendfsync no
```

The suite checks these settings before it clears or measures the database. Redis persistence must also be idle. No load, RDB save, or AOF rewrite can be active.

Example durable run against a dedicated Redis server:

```sh
KVBENCH_REDIS_ADDR=127.0.0.1:6380 \
KVBENCH_REDIS_FLUSHDB=1 \
go test -run '^$' -bench '^BenchmarkAcknowledgedOperations$' -benchmem -count=5
```



## Select a smaller workload

Go treats each slash in a sub-benchmark name as a filter level. Quote the regular expression so the shell does not change it.

Run one value size for point reads:

```sh
go test -run '^$' \
  -bench 'BenchmarkAcknowledgedOperations/durable/read/random/value=128/clients=1/(kvlite|bbolt)$' \
  -benchmem -count=5
```

Run the 32-client point-write case:

```sh
go test -run '^$' \
  -bench 'BenchmarkAcknowledgedOperations/durable/write/random/value=128/clients=32/(kvlite|bbolt)$' \
  -benchmem -count=5
```

Use `-benchtime=100x` when you need an exact operation count. Use a time value such as `-benchtime=3s` when you need a longer calibrated run.

## Repeat and compare results

Use more than one run for performance comparisons:

```sh
go test -run '^$' -bench '^BenchmarkAcknowledgedOperations$' -benchmem -count=10 > results.txt
```

Keep the hardware, operating system, Go version, database versions, durability mode, Redis configuration, and background load unchanged between compared runs.

The output uses the standard Go benchmark format. Tools such as `benchstat` can compare two saved output files.

## Profiles

Filter to one workload before profiling. This keeps setup and unrelated workloads out of the profile as much as possible.

Create a CPU profile:

```sh
go test -run '^$' \
  -bench 'BenchmarkAcknowledgedOperations/durable/write/random/value=128/clients=32/kvlite$' \
  -benchtime=10s -cpuprofile cpu.out
```

Create a memory profile:

```sh
go test -run '^$' \
  -bench 'BenchmarkAcknowledgedOperations/durable/write/random/value=128/clients=32/kvlite$' \
  -benchtime=10s -memprofile mem.out
```



## Current limits

The suite does not currently measure:

- Delete operations or delete churn.
- New-key insert growth.
- Cursor, range, or prefix scans.
- Open, recovery, close, checkpoint, or file-compaction time.
- Database or AOF file size.
- Per-operation latency percentiles.
- Operating system I/O, CPU, or memory counters.
- Redis server CPU time without client and network cost.
- Multiple processes or multiple database handles.

Add a workload only when all compared engines can use matched data, durability rules, and transaction boundaries.

## Source layout


| File                | Purpose                                                          |
| ------------------- | ---------------------------------------------------------------- |
| `benchmark_test.go` | Workload matrix, timing, concurrency, and benchmark registration |
| `engine.go`         | Shared engine contract and engine selection                      |
| `data.go`           | Deterministic key and value generation                           |
| `kvlite_engine.go`  | KVLite adapter                                                   |
| `bbolt_engine.go`   | bbolt adapter                                                    |
| `redis_engine.go`   | Redis adapter and persistence validation                         |
| `*_test.go`         | Data, adapter, and safety checks                                 |

