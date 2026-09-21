# KVLite benchmark suite

This suite compares KVLite, bbolt, and Redis at the caller API boundary. It uses fixed work. It checks each durability mode before and after each timed write case.

The suite has three result groups:

- Matched three-engine tests cover point operations, transactions, enumeration, scale, latency, and storage size.
- Ordered tests compare KVLite and bbolt. Redis does not provide the required ordered cursor API.
- Life-cycle tests compare the two embedded engines. The Redis server life cycle does not fit inside the Go client benchmark.


## Run the suite

Run the light profile during normal development:

```sh
cd benchmarks/kvbench
./run-docker.sh
```

Select a more thorough profile when you need more stable results:

```sh
./run-docker.sh medium
./run-docker.sh large
./run-docker.sh heavy
```

| Profile | Benchmark groups | Measured runs | One-million-key cases | Use |
| --- | --- | ---: | --- | --- |
| `light` | All | 1 | No | Common development work |
| `medium` | All | 3 | No | A development check with repeated results |
| `large` | All | 10 | No | Release reports and matched comparisons |
| `heavy` | All | 10 | Yes | The widest supported scale coverage |

Every profile runs acknowledged operations, transactions, enumeration, ordered operations, scale, latency, collections, and life-cycle cases. The light profile has no repetitions. A light result can show a large change, but it cannot prove a small performance change.

The runner uses pinned Go and Redis images. The Go module pins bbolt and go-redis. The runner uses the current KVLite checkout. It runs all engines on Linux. It puts database files on one temporary Docker volume.

The runner uses up to four Docker CPUs by default. Advanced runs can set `KVBENCH_CPUSET` to select another shared CPU set. They can set `KVBENCH_RESULTS_DIR` to select the output directory.

## Fairness contract

The suite applies these rules:

1. Every engine receives the same fixed operation schedule. Go benchmark calibration cannot give one engine less work.
2. Every data set starts from the same sequential key load. The measured key order does not change the tree build order.
3. The fixture load uses durable mode. KVLite and bbolt then close and reopen. Redis changes to the selected mode only after the durable load is complete.
4. Insert cases use keys that are not in the fixture. Update cases use existing keys and new values.
5. Mixed operation type and key selection use separate deterministic sequences.
6. Each engine runs in a separate Go process. The runner changes engine order and durability-mode order across repetitions.
7. The Redis server and Go client use the same CPU set. Redis still includes a client, server, and network path. The embedded engines do not.
8. The suite checks the stored key count and values after each core case.
9. Scan tests count every result and consume each key and value through a checksum.
10. The runner saves the commit, working-tree patch, source hashes, versions, Docker details, CPU set, and raw output.


## Benchmark groups

### Acknowledged operations

The core group covers:

- Random point reads with 32, 128, 1,024, and 3,072 byte values.
- Sequential point reads.
- Read hit rates of 100%, 50%, and 0%.
- Missing keys below, between, and above the stored key range.
- Random same-size updates with 32, 128, 1,024, and 3,072 byte values.
- Value growth from 32 to 1,024 bytes.
- Value shrink from 1,024 to 32 bytes.
- Sequential and random new-key inserts.
- Point reads, updates, inserts, and mixed work at 1, 8, or 32 clients where applicable.
- Sequential and random update and insert batches of 10, 100, and 1,000 keys.
- Value-growth and value-shrink batches of 100 keys.
- Concurrent 100-key update and insert batches at 8 and 32 clients.
- Mixed work with 95% or 50% reads.

Each case starts with 10,000 keys. Point read cases run 100,000 operations. One-client point write cases run 1,000 operations. Concurrent point write cases run 3,200 operations. Mixed cases run 10,000 operations. Batch cases process 10,000 keys.

### Read and mixed transactions

`BenchmarkReadTransactions` reads 1, 10, 100, or 1,000 keys per transaction. Redis uses `MGET`. KVLite and bbolt use one read transaction.

`BenchmarkMixedTransactions` puts 10, 100, or 1,000 operations in one transaction. It tests 95% and 50% reads. Redis uses `MULTI` and `EXEC`. KVLite and bbolt use one write transaction.

### Enumeration and ordered operations

`BenchmarkEnumeration` runs on all three engines. It tests prefix selectivity of 0%, 1%, 10%, and 100%. Redis uses `SCAN` and `MGET`. Redis result order is not part of the contract.

`BenchmarkOrderedOperations` runs only on KVLite and bbolt. It tests:

- Random seek and 1, 10, 100, or 1,000 returned entries.
- Range selectivity of 0%, 1%, 10%, and 100%.
- Full forward iteration.
- Full reverse iteration.

### Scale and access distribution

`BenchmarkScaleAndAccessDistribution` tests 10,000 and 100,000 keys. It tests uniform reads and an 80/20 hot set. The heavy profile adds one million keys.

These are warm operating-system-cache tests. A reopen does not make a reliable cold-cache test. Redis also keeps its data in memory. The suite does not label any case as cold unless the runner can enforce the same memory pressure for the complete Redis server and each embedded process.

### Latency

`BenchmarkLatency` reports `p50-ns`, `p95-ns`, `p99-ns`, and `max-ns`. It tests reads, updates, and 95/5 mixed work at 1, 8, and 32 clients.

The latency timer calls `time.Now` for each operation. Use the core group for throughput. Use the latency group for the latency distribution. Do not use latency-group throughput as the primary throughput result.

### Collections

`BenchmarkCollections` tests 1 and 100 collections at depth 1 and 3. KVLite and bbolt use native buckets. Redis uses logical key prefixes. The benchmark name states this model difference. Do not present Redis logical prefixes as nested buckets.

### Life cycle and recovery

`BenchmarkLifecycle` compares KVLite and bbolt for:

- Create, load 10,000 keys, and close.
- Open a clean database.
- Close after fixed writes.
- Open and verify a database after its writer process is killed.

The last case measures recovery after acknowledged durable writes. It kills the writer process after the write returns. It does not simulate loss of operating-system cache or a power failure. Redis restart validation runs in the Docker runner, but it is not ranked against embedded open time. KVLite performs its final checkpoint during close, so the close-after-writes case includes that work.

Deletion and delete churn are not in this version.

## Metrics

| Metric | Meaning |
| --- | --- |
| `ns/op` | Time for one logical benchmark iteration. Fixed-work subtests also report a named rate. |
| `ack-keys/s` | Keys acknowledged each second. A write batch counts each key. |
| `keys/s`, `reads/s`, `entries/s`, `operations/s` | Completed work for the named suite. |
| `p50-ns`, `p95-ns`, `p99-ns`, `max-ns` | Caller-visible operation latency in the latency suite. |
| `primary-B` | Main database-file bytes. Redis reports zero because AOF is its tested persistent file. |
| `log-B` | KVLite WAL bytes or Redis AOF bytes. bbolt reports zero because it has no separate log file. |
| `persistent-B` | `primary-B + log-B`. |
| `wal-written-B` | Successful KVLite WAL transaction bytes since the benchmark opened the database. A checkpoint does not decrease this value. |
| `checkpoints` | Completed KVLite checkpoints since the benchmark opened the database. The value excludes the final close. |
| `dataset-memory-B` | Redis `used_memory_dataset`. It is an extra Redis-only value. |
| `checksum` | A deterministic value that proves scan output was consumed. |

The suite does not report cross-engine Go allocation values. Redis server allocations do not appear in the Go client process.

`wal-written-B` and `checkpoints` explain KVLite work. They are not cross-engine storage metrics. The suite does not yet report a fair combined CPU value, peak resident memory value, operating-system write-byte count, or sync-call count. A Go-process value would omit the Redis server. A Redis-server value would omit the Go client. Collect these values at the container boundary before you use them for a cross-engine claim.

## Matched API boundaries

| Logical operation | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| Point read | `DB.Get` | One read transaction | `GET` |
| Grouped read | One read transaction | One read transaction | `MGET` |
| Point write, one client | `DB.Put` | `DB.Update` | `SET` |
| Point write, many clients | `DB.Put` | `DB.Batch` | `SET` |
| Batch write | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Mixed transaction | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Prefix enumeration | Ordered prefix cursor | Ordered prefix cursor | `SCAN` and `MGET` |
| Key count | Bucket cursor scan | Bucket cursor scan | `DBSIZE` |

KVLite groups concurrent `SyncFull` updates before one WAL sync. Redis can group AOF writes from several clients before one reply group. The bbolt adapter uses `DB.Batch` for concurrent durable point writes. These are native group-commit paths. Their group limits are not equal. The suite does not hide this engine behavior.

## Durability modes

| Mode | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| `durable` | `SyncFull` | `NoSync=false`; `NoGrowSync=false`; `NoFreelistSync=false` | AOF on; `appendfsync always` |
| `no-commit-sync` | `SyncNone` | `NoSync=true`; `NoGrowSync=true`; `NoFreelistSync=false` | AOF on; `appendfsync no` |

On Linux, KVLite uses `fdatasync` for WAL and main-file data. bbolt uses `fdatasync` for transaction data and metadata. Redis uses its Linux data-sync path for AOF. bbolt can issue two data syncs for one transaction. The suite keeps that native design.

In durable mode, a successful write waits for the engine durability boundary. Redis `appendfsync always` can group commands that arrive in one event-loop cycle. See the [Redis persistence guide](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) and the [bbolt transaction source](https://github.com/etcd-io/bbolt/blob/v1.4.3/tx.go).

In `no-commit-sync` mode, no engine asks storage to sync during a commit. KVLite also disables sync at checkpoint and close. bbolt `NoGrowSync=true` prevents a separate file-growth sync. Redis still writes AOF data, but the operating system selects the flush time.

The suite keeps bbolt freelist persistence on in both modes.

## No shared relaxed mode

The suite does not put these settings in one shared `relaxed` result:

- KVLite `SyncNormal` syncs at checkpoint or close.
- Redis `appendfsync everysec` uses a time-based policy.
- bbolt has no matching automatic policy. A caller can use `NoSync` and call `DB.Sync` at an application-selected time.

These policies have different acknowledgement and loss windows. A common name would not make them equal.

The suite does not use Redis `WAIT`. It checks replica receipt, not local disk sync. The suite does not add `WAITAOF` after `SET`. With `appendfsync always`, that would add another command and another network round trip.

## Runtime checks

The adapters run these checks:

- KVLite checks the selected `Sync` mapping.
- bbolt checks `NoSync`, `NoGrowSync`, `NoFreelistSync`, the array freelist, and concurrent batch limits.
- Redis reads its live persistence configuration. It checks that no save, rewrite, or pending AOF sync is active.
- Redis checks that the client pool has the requested connection count.

The contract tests check grouped reads, mixed transactions, enumeration, stored-value ownership, collections, storage statistics, and ordered visits. They reopen durable embedded databases without a clean close. The Docker runner kills and restarts Redis after an acknowledged durable probe.

## Measured boundary

Core timers include only selected API operations and required acknowledgements. They exclude open, fixture load, final validation, and close. The life-cycle group measures excluded work separately.

Use raw files with `benchstat`. Keep hardware, CPU set, Docker system, source hashes, versions, suite, and benchmark filter unchanged.
