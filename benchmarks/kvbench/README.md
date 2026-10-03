# KVLite benchmark suite

This suite compares KVLite, bbolt, and Redis at the caller API boundary. It uses fixed work and checks each durability mode before and after each timed write case.

The suite has three result groups:

- Matched three-engine tests cover point operations, key deletion, transactions, enumeration, access distribution, latency, and storage size.
- Ordered and native-bucket tests compare KVLite and bbolt. Redis does not provide the required ordered cursor or bucket API.
- Life-cycle tests compare the two embedded engines. The Redis server life cycle does not fit inside the Go client benchmark.

See the repository [benchmark report](../../BENCHMARKS.md) for the latest full run. The 2026-10-03 report uses `large --workloads=all --storage=volume`. It contains the complete case matrix and the median of 15 measured rounds.

## Run benchmarks on GitHub Actions

The [test workflow](../../.github/workflows/ci.yml) tests both Go modules on each pull request and push to main. The [KVBench workflow](../../.github/workflows/kvbench.yml) runs the Docker runner below on a remote machine. Pull requests use `light`, `focused`, and all three engines. Pushes to main use `medium`, `all`, and all three engines. Each run measures both durability modes and saves the raw results and environment record as a workflow artifact.

To run a selected branch away from your computer, commit and push its changes. GitHub Actions cannot read changes that stay on your computer. After the workflow is on the default branch, start a manual run with the profile, workload, and engines you need:

```sh
gh workflow run kvbench.yml --ref my-benchmark-branch -f profile=light -f workload=focused -f engines=kvlite
gh workflow run kvbench.yml --ref my-benchmark-branch -f profile=medium -f workload=all -f engines=kvlite,bbolt,redis
gh run list --workflow kvbench.yml --branch my-benchmark-branch
gh run watch 123456789
gh run download 123456789
```

Replace the branch and run ID with your values. A manual run uses `medium`, `all`, and all three engines by default. The workflow accepts the `light`, `medium`, or `large` profile and the `focused`, `reads`, `writes`, `deletes`, `mixed`, or `all` workload. Select one or more engines with a comma-separated list. Each workflow run gets its own GitHub-hosted machine. Runs from different branches can start at the same time, subject to GitHub runner capacity.

Use the saved environment record and raw samples when you compare results. Shared runners can change timing between runs. Treat small differences between separate workflow runs as uncertain. The Docker runner changes engine and durability order within each run.

## Run the suite

Run the light profile during normal development:

```sh
cd benchmarks/kvbench
./run-docker.sh
```

Select another profile when you need repeated results or complete case coverage:

```sh
./run-docker.sh medium
./run-docker.sh large
```

All profiles run all three engines by default. Select a smaller engine set when needed:

```sh
./run-docker.sh light --engines=kvlite,bbolt
./run-docker.sh light --engines=kvlite
./run-docker.sh --engines=kvlite
```

When you omit the profile, the runner uses light. Light uses the focused workload and tmpfs by default.

Select the measured workload when you do not need the complete suite:

```sh
./run-docker.sh light --workloads=focused
./run-docker.sh light --workloads=reads
./run-docker.sh medium --workloads=writes
./run-docker.sh medium --workloads=deletes
./run-docker.sh medium --workloads=mixed
```

The workload values have these meanings:

- `focused` runs one random point read, one combined point-update, point-insert, and 10-key batch-update cycle, and one random point delete in both durability modes. It is the light default.
- `reads` runs pure read workloads.
- `writes` runs update, insert, delete, reuse, and space-recovery workloads.
- `deletes` runs key delete, bucket delete, reuse, and space-recovery workloads.
- `mixed` runs workloads that combine reads and writes.
- `all` runs every workload, including life-cycle cases. It is the medium and large default.

The modifier selects measured work. Read fixtures still use the same durable setup before measurement. Options can appear in any order.

Select where the runner stores benchmark data:

```sh
./run-docker.sh light --workloads=focused --storage=tmpfs
./run-docker.sh light --workloads=focused --storage=volume
./run-docker.sh medium --workloads=all --storage=tmpfs
```

The `storage` modifier is independent of the profile and workload. `tmpfs` uses container memory. It reduces host storage variation, but it does not measure physical-device sync latency or survive a container restart. The runner therefore skips the Redis restart durability probe when it uses tmpfs. `volume` uses a temporary Docker volume. It includes the Docker host or virtual-machine storage path. It is still specific to that environment.

| Profile | Scope | Warm-up | Measurement | Default workload | Default storage | Use |
| --- | --- | ---: | --- | --- | --- | --- |
| `light` | One representative case from every comparison group | 1 | 3 rounds with 2 samples each | `focused` | `tmpfs` | Quick repeated comparison |
| `medium` | A selected decision set from every comparison group | 1 | 10 fixed-work runs | `all` | `volume` | Repeated engineering comparisons |
| `large` | Every case variant at the standard work size | 1 | 15 fixed-work runs | `all` | `volume` | Release and publication results |

The light profile compares the selected engines in Docker. It skips pre-run test passes. Its wider workloads use 1,000 records, 1,000 point reads, 100 point writes, 1,000 mixed operations, and 320 concurrent point writes. It runs one warm-up and records six samples for each selected engine and durability mode.

The focused workload selects five data paths: random point read, random point update, random point insert, random 10-key batch update, and random point delete of an existing key with one client. Each sample uses at least 100,000 reads, 1,000 fixed write cycles, and 1,000 point deletes in the light profile. The three write paths run in one cycle and report one combined signal. Point deletes report a separate result. The cases run in durable and no-sync modes. KVLite, bbolt, and Redis run the same operations. Focused uses one CPU by default. `KVBENCH_CPUSET` can select a different CPU set. Mixed, scan, lifecycle, and collection work remain in the wider workloads. Use `light --workloads=focused` for quick repeated code-regression comparisons.

The medium profile uses 3,000 records for its main cases. It uses 10,000 point reads, 200 single-client point writes, 640 concurrent point writes, and 2,000 mixed operations. It keeps 15 core cases. These cases cover misses, sequential and random reads, large values, value growth, inserts, updates, concurrency, mixed work, and batches.

The large profile uses the full case matrix. It uses 10,000 records, 100,000 point reads, 1,000 single-client point writes, 3,200 concurrent point writes, and 10,000 mixed operations.

All profiles run one unrecorded pass before measurement. Light records two samples in each of three rounds. Medium records ten runs. Large records fifteen runs. The runner changes engine order and durability-mode order across the measured rounds. The focused workload runs reads in both modes. Other read workloads do not repeat the no-sync mode because commit sync does not affect reads.

A large run is suitable for environment-specific publication. Keep the raw values and environment record with every published report.

All profiles use pinned Go and Redis images. The Go module pins bbolt and go-redis. The runner uses the current KVLite checkout and runs the selected engines on Linux. Light uses `tmpfs` by default for quick development feedback. Medium and large use a Docker volume by default for storage-sensitive results. An explicit `--storage` value overrides these defaults. The runner keeps the Go module and build caches in the `kvlite-kvbench-go-cache` Docker volume. The cache reduces repeat-run setup time. It does not contain benchmark data.

The full warm-up pass prepares the executable, container, and shared operating-system state. It does not reuse a measured database fixture. Read-latency cases also perform unrecorded operations against their own loaded fixture before they start the timer. Medium and large run the correctness tests before warm-up and measurement. Light skips these tests.

The Docker profiles use up to four CPUs by default. Advanced runs can set `KVBENCH_CPUSET` to select another shared CPU set. All profiles can set `KVBENCH_RESULTS_DIR` to select the output directory.

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

The medium profile uses the selected cases and work sizes listed above. Large uses every case and the standard work sizes. Light uses the smallest fixed workloads.

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

### Access distribution

`BenchmarkScaleAndAccessDistribution` tests 1,000 keys in light, 10,000 keys in medium, and 10,000 and 100,000 keys in large. Each profile tests uniform reads and an 80/20 hot set. Each distribution uses a separate fixture.

These are warm operating-system-cache tests. A reopen does not make a reliable cold-cache test. Redis also keeps its data in memory. The suite does not label any case as cold unless the runner can enforce the same memory pressure for the complete Redis server and each embedded process.

### Latency

`BenchmarkLatency` reports `p50-ns`, `p95-ns`, `p99-ns`, and `max-ns`. It tests reads, updates, and 95/5 mixed work at 1, 8, and 32 clients.

Light measures 10,000 reads, 1,000 updates, and 1,000 mixed operations in each selected latency case. Medium measures 100,000 reads, 1,000 updates, and 10,000 mixed operations. Large measures 1,000,000 reads, 10,000 updates, and 100,000 mixed operations. Read-latency cases first run 100, 1,000, or 10,000 unrecorded operations for light, medium, or large.

The latency timer calls `time.Now` for each operation. Use the core group for throughput. Use the latency group for the latency distribution. Do not use latency-group throughput as the primary throughput result.

Point-read latency uses each engine's single-call caller boundary. KVLite uses `DB.Get`. bbolt uses one `DB.View` transaction for each read because bbolt does not provide a direct `DB.Get`. Redis uses one client request. Use `BenchmarkReadTransactions` when transaction setup must be amortized across several reads. State this boundary in every latency claim.

### Collections

`BenchmarkCollections` tests 1 and 100 collections at depth 1 and 3. KVLite and bbolt use native buckets. Redis uses logical key prefixes. The benchmark name states this model difference. Do not present Redis logical prefixes as nested buckets.

### Life cycle and recovery

`BenchmarkLifecycle` compares KVLite and bbolt for:

- Create, load keys, and close. Light uses 1,000 keys, medium uses 3,000 keys, and large uses 10,000 keys.
- Open a clean database.
- Close after fixed writes.
- Open and verify a database after its writer process is killed.

The last case measures recovery after acknowledged durable writes. It kills the writer process after the write returns. It does not simulate loss of operating-system cache or a power failure. The Docker runner checks Redis restart behavior, but it does not rank that check against embedded open time. KVLite performs its final checkpoint during close, so the close-after-writes case includes that work.

### Delete and space recovery

The delete group runs the same fixed key operations through all three engines. It measures existing and missing deletes, random and sequential order, one and eight clients, and single-key and batch calls. It also measures a read after each acknowledged delete.

The group tests one delete-and-insert cycle and ten repeated cycles. It measures file growth before and after close and reopen. A separate case deletes upper keys and measures logical file size and operating-system file blocks. Another case measures reopen after alternating keys are deleted.

`BenchmarkDeleteBuckets` removes 100 populated top-level buckets. Each bucket has 10, 30, or 100 keys in the light, medium, or large profile. It times one committed delete call per bucket. Setup and checks run outside the timer. This case compares KVLite and bbolt. Redis has no native buckets, so it does not run this case. The focused workload runs only the random one-client key-delete case.

KVLite can reuse free pages inside its file. Checkpoint can release a free file suffix to the operating system. The cases do not run bbolt compaction or Redis AOF rewrite. `BenchmarkDeleteVisibility` measures the next read in the same client. It does not measure replication.

## Metrics

| Metric                                           | Meaning                                                                                                                     |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `ns/op`                                          | Time for one logical benchmark iteration. Fixed-work subtests also report a named rate.                                     |
| `ack-keys/s`                                     | Keys acknowledged each second. A write batch counts each key.                                                               |
| `transactions/s`, `cycle-keys/s`                 | Delete calls or changed keys each second in the named case.                                                                  |
| `delete-ack-ns`, `post-ack-miss-p50-ns`, `post-ack-miss-p95-ns`, `post-ack-miss-p99-ns` | Delete time and time for the next read to return a miss.                            |
| `keys/s`, `reads/s`, `entries/s`, `operations/s` | Completed work for the named suite.                                                                                         |
| `p50-ns`, `p95-ns`, `p99-ns`, `max-ns`           | Caller-visible operation latency in the latency suite.                                                                      |
| `primary-B`                                      | Main database-file bytes. Redis reports zero because AOF is its tested persistent file.                                     |
| `log-B`                                          | KVLite WAL bytes or Redis AOF bytes. bbolt reports zero because it has no separate log file.                                |
| `persistent-B`                                   | `primary-B + log-B`.                                                                                                        |
| `os-allocated-B`                                 | File-system blocks for embedded database and log files. Redis does not expose its AOF path to this client.                  |
| `loaded-persistent-B`, `final-persistent-B`      | File bytes before delete and after the selected close or reopen boundary.                                                   |
| `post-delete-persistent-B`                       | File bytes after delete and before any later close or reopen.                                                                |
| `active-post-reinsert-persistent-B`, `stable-post-reinsert-persistent-B` | File bytes before and after the embedded close and reopen boundary.                    |
| `reuse-active-growth-B`, `reuse-stable-growth-B` | File growth from the loaded state before and after that boundary.                                                            |
| `peak-persistent-B`, `active-final-persistent-B`, `stable-final-persistent-B` | File bytes during and after repeated delete and insert cycles.                 |
| `loaded-os-allocated-B`, `post-delete-os-allocated-B`, `active-post-reinsert-os-allocated-B`, `stable-post-reinsert-os-allocated-B` | Allocated file blocks at each named point. |
| `reinsert-pages-reused`                          | KVLite pages reused during the reinsertion part of one cycle.                                                                |
| `pages-reused`                                   | KVLite pages reused across repeated delete and insert cycles.                                                                |
| `allocated-pages`, `reusable-pages`              | KVLite pages in use and free pages inside the logical file.                                                                  |
| `pages-retired`, `tail-pages-reclaimed`          | KVLite pages freed by committed writes and pages removed from the logical file tail.                                        |
| `persistent-change-B`, `os-change-B`             | Signed change in file bytes or allocated blocks. Positive values mean growth.                                                |
| `persistent-reclaimed-B`, `os-reclaimed-B`       | Nonnegative number of file bytes or allocated blocks released.                                                              |
| `persistent-growth-B`, `os-growth-B`             | Nonnegative number of file bytes or allocated blocks added.                                                                 |
| `close-ns`, `reopen-ns`                          | Time to close and reopen an embedded database in the named case.                                                             |
| `wal-written-B`                                  | Successful KVLite WAL transaction bytes since the benchmark opened the database. A checkpoint does not decrease this value. |
| `checkpoints`                                    | Completed KVLite checkpoints since the benchmark opened the database. The value excludes the final close.                   |
| `dataset-memory-B`                               | Redis `used_memory_dataset`. It is an extra Redis-only value.                                                               |
| `checksum`                                       | A deterministic value that proves scan output was consumed.                                                                 |

The suite does not report cross-engine Go allocation values. Redis server allocations do not appear in the Go client process.

`wal-written-B` and `checkpoints` explain KVLite work. They are not cross-engine storage metrics. The suite does not yet report a fair combined CPU value, peak resident memory value, operating-system write-byte count, or sync-call count. A Go-process value would omit the Redis server. A Redis-server value would omit the Go client. Collect these values at the container boundary before you use them for a cross-engine claim.

## Matched API boundaries

| Logical operation         | KVLite                | bbolt                 | Redis              |
| ------------------------- | --------------------- | --------------------- | ------------------ |
| Point read                | `DB.Get`              | One read transaction  | `GET`              |
| Grouped read              | One read transaction  | One read transaction  | `MGET`             |
| Point write, one client   | `DB.Put`              | `DB.Update`           | `SET`              |
| Point write, many clients | `DB.Put`              | `DB.Batch`            | `SET`              |
| Batch write               | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Point delete, one client   | `DB.Delete`           | `DB.Update`           | `DEL`              |
| Point delete, many clients | `DB.Delete`           | `DB.Batch`            | `DEL`              |
| Batch delete              | One write transaction | One write transaction | One variadic `DEL` |
| Whole top-level bucket delete | `DB.DeleteBucket` | `DB.Update` with `Tx.DeleteBucket` | No native bucket |
| Mixed transaction         | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Prefix enumeration        | Ordered prefix cursor | Ordered prefix cursor | `SCAN` and `MGET`  |
| Key count                 | Bucket cursor scan    | Bucket cursor scan    | `DBSIZE`           |

KVLite groups concurrent `SyncFull` updates before one WAL sync. Redis can group AOF writes from several clients before one reply group. The bbolt adapter uses `DB.Batch` for concurrent durable point writes. These are native group-commit paths. Their group limits are not equal. The suite does not hide this engine behavior.

## Durability modes

| Mode             | KVLite     | bbolt                                                      | Redis                        |
| ---------------- | ---------- | ---------------------------------------------------------- | ---------------------------- |
| `durable`        | `SyncFull` | `NoSync=false`; `NoGrowSync=false`; `NoFreelistSync=false` | AOF on; `appendfsync always` |
| `no-commit-sync` | `SyncNone` | `NoSync=true`; `NoGrowSync=true`; `NoFreelistSync=false`   | AOF on; `appendfsync no`     |

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

Core timers include only selected API operations and required acknowledgements. They exclude open, fixture load, final stored-data checks, and close. The life-cycle group measures excluded work separately.

Keep hardware, CPU set, Docker system, source hashes, versions, suite, and benchmark filter unchanged when you compare runs.
