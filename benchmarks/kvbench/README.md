# KVLite benchmark suite

This suite compares KVLite, bbolt, and Redis at the caller API boundary. It uses fixed work. It checks each durability mode before and after each timed case.

Use this suite for caller-visible latency and throughput. Do not use it to compare engine CPU use, memory use, or full database life-cycle cost.

## Run the suite

```sh
cd benchmarks/kvbench
./run-docker.sh
```

The runner uses pinned Go 1.26.7, Redis 8.8.0, bbolt 1.4.3, and go-redis 9.22.0. It uses the current KVLite checkout. It runs all three engines on Linux. It puts all database files on one temporary Docker volume.

The runner uses up to four Docker CPUs by default. Set `KVBENCH_CPUSET` to select another shared CPU set. Set `KVBENCH_RESULTS_DIR` to select the output directory. Set `KVBENCH_COUNT` to change the default ten repetitions. Set `KVBENCH_BENCH` to select benchmark cases.

Do not compare direct host runs with Docker runs. The kernel, file system, and sync system call can differ.

## Fairness contract

The suite applies these rules:

1. Every engine receives the same fixed operation schedule. Go benchmark calibration cannot give one engine less work.
2. Every data set starts from the same sequential key load. The measured key order does not change the tree build order.
3. The fixture load uses durable mode. KVLite and bbolt then close and reopen. Redis changes to the selected mode only after the durable load is complete.
4. Measured writes use values that differ from the fixture values.
5. The mixed operation type and key choice use separate deterministic sequences. Reads and writes can use the same keys.
6. Each engine runs in a separate Go process. The runner changes the engine order across repetitions. It also changes the durability-mode order across repetitions.
7. The Redis server and the Go process use the same CPU set. Redis still includes a client, a server, and a network path. The embedded engines do not.
8. The runner checks all 10,000 stored values after each case. A key count alone cannot make a failed update pass.
9. The runner saves the commit, working-tree patch, source hashes, versions, Docker details, CPU set, and raw output.

The old report files in this directory used the earlier method. Do not compare their values with values from this runner.

## Fixed work

The runner requires `-benchtime=1x`. Each Go benchmark iteration contains the full fixed workload.

| Case | Logical operations | Keys per operation | Total key operations |
| --- | ---: | ---: | ---: |
| One-client point read | 100,000 | 1 | 100,000 |
| Eight-client point read | 100,000 | 1 | 100,000 |
| One-client point write | 1,000 | 1 | 1,000 |
| Thirty-two-client point write | 3,200 | 1 | 3,200 |
| Eight-client mixed | 10,000 | 1 | 10,000 |
| Batch write | 10, 100, or 1,000 | 1,000, 100, or 10 | 10,000 |

The suite runs read-only cases only in durable mode. The write sync mode does not change read behavior.

## Metrics

| Metric | Meaning |
| --- | --- |
| `ns/op` | Time for one logical operation. One batch is one logical operation. |
| `ack-keys/s` | Keys acknowledged each second. A batch counts each key. |
| `MB/s` | Value bytes each second. It excludes keys, protocol bytes, and storage metadata. |

The suite does not report cross-engine Go allocation values. Redis server allocations do not appear in the Go client process. Such values would not be comparable.

## Workloads

Each case starts with 10,000 keys. Each key is 16 bytes. Values are deterministic. Point writes overwrite existing keys.

| Operation | Order | Value bytes | Clients | Batch or mix |
| --- | --- | --- | ---: | --- |
| Point read | Random | 32, 128, 1,024, 3,072 | 1 | One key |
| Point write | Random | 32, 128, 1,024, 3,072 | 1 | One key |
| Point read | Sequential | 128 | 1 | One key |
| Point read | Random | 128 | 8 | One key |
| Point write | Random | 128 | 32 | One key |
| Mixed | Random | 128 | 8 | 95% reads and 5% writes |
| Mixed | Random | 128 | 8 | 50% reads and 50% writes |
| Batch write | Random | 128 | 1 | 10, 100, or 1,000 keys |

The read cases use a warm operating-system cache. This is a hot-read test. It is not a cold-storage test.

## Matched API boundaries

| Logical operation | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| Point read | `DB.Get` | One read transaction | `GET` |
| Point write, one client | `DB.Put` | `DB.Update` | `SET` |
| Point write, many clients | `DB.Put` | `DB.Batch` | `SET` |
| Batch write | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Key count | Bucket cursor scan | Bucket cursor scan | `DBSIZE` |

KVLite groups concurrent `SyncFull` updates before one WAL sync. Redis can group AOF writes from several clients before one reply group. The bbolt adapter uses its public `DB.Batch` call for concurrent durable point writes. bbolt uses a default maximum batch size of 1,000 calls and a default maximum delay of 10 milliseconds. Its callback can run more than once. The adapter callback only repeats the same `Put`.

These are native group-commit paths. Their group limits are not equal. The suite does not hide this engine behavior.

## Durability modes

The suite compares two modes.

| Mode | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| `durable` | `SyncFull` | `NoSync=false`; `NoGrowSync=false`; `NoFreelistSync=false` | AOF on; `appendfsync always` |
| `no-commit-sync` | `SyncNone` | `NoSync=true`; `NoGrowSync=true`; `NoFreelistSync=false` | AOF on; `appendfsync no` |

On Linux, KVLite now uses `fdatasync` for WAL and main-file data. bbolt uses `fdatasync` for transaction data and metadata. Redis uses its Linux data-sync path for the AOF. bbolt can issue two data syncs for one transaction. The suite keeps that native design.

In durable mode, a successful write waits for the engine durability boundary. Redis `appendfsync always` can group commands that arrive in one event-loop cycle. See the [Redis persistence guide](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) and the [bbolt transaction source](https://github.com/etcd-io/bbolt/blob/v1.4.3/tx.go).

In `no-commit-sync` mode, no engine asks storage to sync during a commit. KVLite also disables sync at checkpoint and close. bbolt `NoGrowSync=true` prevents a separate file-growth sync. bbolt documents this setting as unsafe on ext3 and ext4. The benchmark uses disposable files. Redis still writes AOF data, but the operating system selects the flush time.

The suite keeps bbolt freelist persistence on in both modes. This keeps the bbolt data path constant when the suite changes its sync policy.

## No shared relaxed mode

The suite does not put these settings in one shared `relaxed` result:

- KVLite `SyncNormal` syncs at checkpoint or close.
- Redis `appendfsync everysec` uses a time-based policy.
- bbolt has no matching automatic policy. A caller can use `NoSync` and call `DB.Sync` at an application-selected time.

These policies have different acknowledgement and loss windows. A common name would not make them equal.

The suite does not use Redis `WAIT`. It checks replica receipt, not local disk sync. The suite does not add `WAITAOF` after `SET`. With `appendfsync always`, that would add a second command and a second network round trip.

## Runtime checks

The adapters run these checks before and after measured work:

- KVLite checks the exact selected `Sync` mapping.
- bbolt checks `NoSync`, `NoGrowSync`, `NoFreelistSync`, the array freelist, and the concurrent batch limits on the open database.
- Redis reads its live persistence configuration. It also checks that no save, rewrite, or pending AOF sync is active.
- Redis checks that the client pool has the requested connection count before a concurrent case.

The test suite also checks adapter value ownership and exact stored values. It reopens durable embedded databases without a clean close. The Docker runner kills and restarts Redis after an acknowledged durable probe. These reopen checks do not simulate a power loss. The source and runtime checks define the storage-sync guarantee.

## Measured boundary

The timer includes only the selected API operations and their required acknowledgements. It excludes open, fixture load, final validation, close, checkpoint after the final call, Redis restart, and file compaction.

This boundary cannot rank full life-cycle cost. KVLite can checkpoint during a measured call. It can also checkpoint on close. bbolt closes its file and mapping. Redis closes only the benchmark client while the server stays active.

The suite also does not measure cold reads, new-key growth, delete churn, scans, recovery time, file size, latency percentiles, operating-system counters, or server-only CPU use.

## Select cases

Run one point-write value size:

```sh
KVBENCH_BENCH='BenchmarkAcknowledgedOperations/.*/write/random/value=128/clients=1/.*$' \
  ./run-docker.sh
```

Run the concurrent point-write case:

```sh
KVBENCH_BENCH='BenchmarkAcknowledgedOperations/.*/write/random/value=128/clients=32/.*$' \
  ./run-docker.sh
```

Use the raw files with `benchstat`. Keep the hardware, CPU set, Docker system, source hashes, versions, and benchmark filter unchanged.
