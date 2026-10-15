# KVLite benchmark suite

KVBench measures database operations through the public APIs of KVLite, bbolt, and Redis. Each engine receives the same keys, values, and operation sequence. The suite checks stored results and the configured write synchronization settings.

A case defines one measurement, such as random reads with 128-byte values and one client. A client runs a sequence of database calls. Several clients can make calls at the same time.

A workload selects operation groups, such as reads or writes. A scale selects cases, data sizes, and the number of samples.

A fixture is the initial data loaded before measurement. One sample times the fixed operation sequence for one case.

One round runs the selected engines and modes. A warm-up round runs first. Its samples do not enter the reported results.

The durability mode selects whether a successful write waits for storage synchronization.

KVLite and bbolt run inside the Go process. Redis uses a separate server and a network connection.

The suite has three result groups:

- All three engines run point operations, key deletion, transactions, scans, latency tests, and storage-size checks. A point operation reads or changes one key.
- KVLite and bbolt also run ordered reads and bucket tests. A bucket contains named key/value entries. The Redis API does not provide these operations.
- KVLite and bbolt run open, close, and recovery tests. These tests cannot measure the separate Redis server through a Go client call.

See the repository [benchmark report](../../BENCHMARKS.md) for the latest full run. The 2026-10-15 report uses `large --workloads=all --storage=volume --profile-engines=kvlite` at commit `ca4fdae`. It contains the complete case matrix, including the large-entry cases, and the median of 15 measured rounds.

## Run benchmarks on GitHub Actions

The [test workflow](../../.github/workflows/ci.yml) tests both Go modules on each pull request and push to main. The [KVBench workflow](../../.github/workflows/kvbench.yml) runs the Docker runner below on a remote machine. Pull requests use `light`, `focused`, and all three engines. Pushes to main use `medium`, `all`, and all three engines. Normal runs measure both durability modes. Each run saves the raw results and environment record as a workflow artifact.

To run a selected branch away from your computer, commit and push its changes. GitHub Actions cannot read changes that stay on your computer. After the workflow is on the default branch, start a manual run with the scale, workload, and engines you need:

```sh
gh workflow run kvbench.yml --ref my-benchmark-branch -f scale=light -f workload=focused -f engines=kvlite
gh workflow run kvbench.yml --ref my-benchmark-branch -f scale=medium -f workload=all -f engines=kvlite,bbolt,redis
gh workflow run kvbench.yml --ref my-benchmark-branch -f scale=light -f workload=focused -f engines=kvlite -f profiling=true
gh workflow run kvbench.yml --ref my-benchmark-branch -f scale=medium -f workload=all -f engines=kvlite,bbolt -f case='durable:BenchmarkLifecycle/scale=medium/close-after-writes' -f profiling=true
gh run list --workflow kvbench.yml --branch my-benchmark-branch
gh run watch 123456789
gh run download 123456789
```

Replace the branch and run ID with your values. A manual run uses `medium`, `all`, and all three engines by default. The workflow accepts the `light`, `medium`, or `large` scale and the `focused`, `reads`, `writes`, `deletes`, `mixed`, or `all` workload. Select one or more engines with a comma-separated list. Each workflow run gets its own GitHub-hosted machine. Runs from different branches can start at the same time, subject to GitHub runner capacity.

Set `profiling=true` to profile every selected case after the normal timing run. Leave it false for an ordinary timing run. The workflow saves the report and profiles in the same result artifact. Profiling a large, all-workload run can take much longer and produce large artifacts.

Set `case` to time and profile one exact case. Copy the mode and case name from a report, without the final engine name. The mode is `durable` or `no-commit-sync`. The case must match the selected scale and workload. Select the engines to compare with `engines`. A case run records only the selected mode.

Use the saved environment record and raw samples when you compare results. Shared runners can change timing between runs. Treat small differences between separate workflow runs as uncertain. The Docker runner changes engine and durability order within each run.

## Run the suite

Run the light scale during normal development:

```sh
cd benchmarks/kvbench
./run-docker.sh
```

Select another scale when you need repeated results or complete case coverage:

```sh
./run-docker.sh medium
./run-docker.sh large
```

All scales run all three engines by default. Select a smaller engine set when needed:

```sh
./run-docker.sh light --engines=kvlite,bbolt
./run-docker.sh light --engines=kvlite
./run-docker.sh --engines=kvlite
```

When you omit the scale, the runner uses light. Light uses the focused workload and tmpfs by default.

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
- `all` runs every group available at the selected scale. It is the medium and large default.

The workload selects the operations to measure. The suite loads the initial data with write synchronization enabled. It then opens or configures the engine in the selected mode. Options can appear in any order.

Select where the runner stores benchmark data:

```sh
./run-docker.sh light --workloads=focused --storage=tmpfs
./run-docker.sh light --workloads=focused --storage=volume
./run-docker.sh medium --workloads=all --storage=tmpfs
```

The `storage` modifier is independent of the scale and workload. `tmpfs` uses container memory. It reduces host storage variation, but it does not measure physical-device sync latency or survive a container restart. The runner therefore skips the Redis restart durability probe when it uses tmpfs. `volume` uses a temporary Docker volume. It includes the Docker host or virtual-machine storage path. It is still specific to that environment.

| Scale | Scope | Warm-up | Measurement | Default workload | Default storage | Use |
| --- | --- | ---: | --- | --- | --- | --- |
| `light` | Small case set; `focused` by default | 1 | 3 rounds with 2 samples each | `focused` | `tmpfs` | Quick repeated comparison |
| `medium` | Selected cases from each group | 1 | 10 fixed-work runs | `all` | `volume` | Repeated engineering comparisons |
| `large` | Complete case set for the selected workload | 1 | 15 fixed-work runs | `all` | `volume` | Release and publication results |

The light scale skips the test runs that precede measurement at medium and large. The default `focused` workload measures point reads, combined writes, and point deletes. Use `--workloads=all` to include the other groups and large entries.

The ordinary light cases use 1,000 stored records. They measure 1,000 point reads, 100 point writes, 1,000 mixed operations, or 320 writes across several clients. Large-entry cases use smaller data sets, as shown below. Light collects six samples per selected case and engine.

The focused workload selects five data paths: random point read, random point update, random point insert, random 10-key batch update, and random point delete of an existing key with one client. Each sample uses at least 100,000 reads, 1,000 fixed write cycles, and 1,000 point deletes at the light scale. The three write paths run in one cycle and report one combined signal. Point deletes report a separate result. The cases run in durable and no-sync modes. KVLite, bbolt, and Redis run the same operations. Focused uses one CPU by default. `KVBENCH_CPUSET` can select a different CPU set. Mixed, scan, lifecycle, and collection work remain in the wider workloads. Use `light --workloads=focused` for quick repeated code-regression comparisons.

The ordinary medium cases use 3,000 stored records. They measure 10,000 point reads, 200 writes with one client, 640 writes across several clients, or 2,000 mixed operations. The core group has 15 ordinary cases and eight large-entry cases. The selected workload determines which cases run.

The large scale uses the full case matrix. It uses 10,000 records, 100,000 point reads, 1,000 single-client point writes, 3,200 concurrent point writes, and 10,000 mixed operations.

All scales run one unrecorded pass before measurement. Light records two samples in each of three rounds. Medium records ten runs. Large records fifteen runs. The runner changes engine order and durability-mode order across the measured rounds. The focused workload runs reads in both modes. Other read workloads do not repeat the no-sync mode because commit sync does not affect reads.

A large run is suitable for environment-specific publication. Keep the raw values and environment record with every published report.

All scales use pinned Go and Redis images. The Go module pins bbolt and go-redis. The runner uses the current KVLite checkout and runs the selected engines on Linux. Light uses `tmpfs` by default for quick development feedback. Medium and large use a Docker volume by default for storage-sensitive results. An explicit `--storage` value overrides these defaults. The runner keeps the Go module and build caches in the `kvlite-kvbench-go-cache` Docker volume. The cache reduces repeat-run setup time. It does not contain benchmark data.

The full warm-up pass prepares the executable, container, and shared operating-system state. It does not reuse a measured database fixture. Read-latency cases also perform unrecorded operations against their own loaded fixture before they start the timer. Medium and large run the correctness tests before warm-up and measurement. Light skips these tests.

The Docker scales use up to four CPUs by default. Advanced runs can set `KVBENCH_CPUSET` to select another shared CPU set. All scales can set `KVBENCH_RESULTS_DIR` to select the output directory.

The Go container gets a memory limit of three quarters of the Docker VM memory. Set `KVBENCH_GO_MEMORY` to a Docker memory value, such as `8g`, to change it. With the limit, an out-of-memory error stops one process in the container, not the whole VM.

## Read results and profile cases

The Docker runner builds the Go report tool in its pinned Go container. After the measured rounds, it writes `report.md`, `report.json`, `summary.csv`, `metrics.csv`, and `comparisons.csv` beside the raw result files. The short report lists up to five clear gains and losses for each matched engine and durability mode. It puts cases with overlapping observed ranges in a separate count. The JSON file keeps every sample. The CSV files show the mean, median, minimum, and maximum for each case and metric.

Compare the same operation, key size, value size, client count, and batch size. For example, a read with a 64 KiB value returns 512 times as many value bytes as a read with a 128-byte value. Their times do not measure a code change. Use the same scale, storage mode, and environment when you compare two versions.

The runner saves `git diff HEAD` and a source hash file. The patch includes staged and unstaged changes. The hashes identify the files used by the run. A run with local changes measures that source state. The commit ID alone does not identify its source.

The gain and loss values compare medians. A median is the middle value after sorting the samples. For an even sample count, it is the mean of the two middle values. Large gaps use a ratio instead of a percentage. A clear gap means the observed ranges do not overlap. It does not prove that a difference is stable. Check the raw samples and environment record before you make a performance claim. Cases without a matching peer have no comparison row. The report compares engines in one run. It does not compare two commits.

To profile every case selected by the scale, workload, durability modes, and engines, add `--profile`. For example:

```sh
./run-docker.sh light --workloads=focused --engines=kvlite --profile
./run-docker.sh medium --engines=kvlite,bbolt --case=durable:BenchmarkLifecycle/scale=medium/close-after-writes --profile
```

To profile only some engines, use `--profile-engines` in place of `--profile`. The measured rounds still run every selected engine, so the report keeps its engine comparisons. For example, `--engines=kvlite,bbolt,redis --profile-engines=kvlite` profiles only the KVLite cases.

The runner finishes its normal unprofiled rounds first. It then runs each selected case again with CPU profiling, allocation profiling, and an execution trace. It repeats each profiled case once per measured round. The profiles cover the repeats together. The report divides sampled CPU time and allocated bytes by the number of repeats. Its short table shows the leading engine function for CPU self time, CPU inclusive time, and inclusive allocation. It hides CPU rankings below 200 ms of sampled CPU time across all repeats. It marks rankings below 1 second as limited. `report.json` lists up to three functions for each measure. Raw profiles, traces, wait files, and full function lists are under `profiles/`. Use them to inspect the complete call path. The report also includes the normal timing values and matched-engine comparisons. Without an explicit workload, a direct `--case` run uses `all`.

The trace wait views (`net`, `sync`, `syscall`, and `sched`) are skipped for a trace larger than 256 MiB, because their conversion can need many times the trace size in memory. The case then has a `wait-views-skipped.txt` file. Set `KVBENCH_TRACE_VIEW_MAX_MIB` to change the limit.

Profiles include fixture setup, final checks, and profiling overhead. CPU samples estimate time spent on the CPU; trace wait profiles estimate blocked time. Neither gives exact time for each function call. The short report omits wait rankings because test and profiler activity can dominate them. Use the unprofiled rounds to measure speed changes. A Redis profile covers its Go client process, not the Redis server process.

## Large-entry cases

Large-entry cases measure keys or values that need more than one KVLite page. A continuation page stores part of the same leaf or branch node. These cases belong to the existing operation workloads.

Run all light cases for KVLite and bbolt with disk-backed storage:

```sh
./run-docker.sh light --workloads=all --engines=kvlite,bbolt --storage=volume
```

Light includes 64 KiB value reads, updates, and deletes when their operation workload is selected. The `focused` workload excludes large entries. Medium and large add the cases in this table.

| Workload | Large-entry cases at medium and large |
| --- | --- |
| `reads` | 64 KiB values; 1 MiB values; 32 KiB keys with 128-byte values |
| `writes` | 64 KiB value updates and inserts; growth from 128 bytes to 64 KiB; shrink from 64 KiB to 128 bytes; large-value deletes |
| `deletes` | Delete half the keys with 64 KiB values |
| `mixed` | 95% reads and 5% updates with 64 KiB values |
| `all` | Every case above |

Each large-entry case uses one client. Each write or delete call changes one key. `BenchmarkAcknowledgedOperations` runs the reads, writes, and mixed cases. `BenchmarkDeleteOperations` runs the delete case. The suite checks the values that remain after deletion.

The 64 KiB value cases and the 32 KiB key case use these work sizes. The read, write, and mixed counts are operations per sample. The delete count is half the initial record count.

| Scale | Initial records | Reads | Writes | Mixed operations | Deletes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `light` | 32 | 200 | 100 | Not selected | 16 |
| `medium` | 128 | 1,000 | 200 | 1,000 | 64 |
| `large` | 512 | 5,000 | 1,000 | 5,000 | 256 |

The write schedule can visit the same key more than once. A growth or shrink case changes a key's value size on its first update. Later updates of that key keep the new size.

The 1 MiB value-read case uses 16 records and 200 reads at medium. It uses 64 records and 1,000 reads at large. Light excludes this case and the 32 KiB key case.

The 32 KiB keys differ only in their final eight bytes. KVLite must compare their common first bytes across continuation pages to select the correct tree child.

The suite loads the data, closes each embedded database, and reopens it before measurement. KVLite reads its main database file. This harness uses the default `DataPageChecksums=false` for new files. It skips data-page checksum calculation and checks. Metadata, allocation pages, and WAL records retain their checksums. Historical results use the policy of their recorded source commit. Both KVLite and the bbolt adapter return a value copy. The caller can change that copy without changing the stored value. Redis receives the same keys and values, but its storage pages differ from those of the embedded engines.

Use `--workloads=reads`, `writes`, `deletes`, or `mixed` to select one operation group. Use `--case` to select one exact case. For example, measure only the light 64 KiB value read:

```sh
./run-docker.sh light --engines=kvlite,bbolt --storage=volume --case=durable:BenchmarkAcknowledgedOperations/scale=light/durable/read/random/value=65536/clients=1
```

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

The medium scale uses the selected cases and work sizes listed above. Large uses every case and the standard work sizes. Light uses the smallest fixed workloads.

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

`BenchmarkScaleAndAccessDistribution` tests 1,000 keys in light, 10,000 keys in medium, and 10,000 and 100,000 keys in large. Each scale tests uniform reads and an 80/20 hot set. Each distribution uses a separate fixture.

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

`BenchmarkDeleteBuckets` removes 100 populated top-level buckets. Each bucket has 10, 30, or 100 keys at the light, medium, or large scale. It times one committed delete call per bucket. Setup and checks run outside the timer. This case compares KVLite and bbolt. Redis has no native buckets, so it does not run this case. The focused workload runs only the random one-client key-delete case.

KVLite can reuse free pages inside its file. Checkpoint can release a free file suffix to the operating system. The cases do not run bbolt compaction or Redis AOF rewrite. `BenchmarkDeleteVisibility` measures the next read in the same client. It does not measure replication.

## Metrics

Time metrics use nanoseconds. Rate metrics count completed work per second. Storage metrics use bytes.

KVLite stores tree pages in its main database file. Its write-ahead log (WAL) records changes for recovery. Redis uses an append-only file (AOF) to record commands for recovery. bbolt has no separate log file. The storage metrics below count these files.

| Metric                                           | Meaning                                                                                                                     |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `ns/op` | Nanoseconds per reported operation. Point cases report one API call. Transaction, scan, and life-cycle cases can report a complete sequence. Check the case name and its rate metric before comparison. |
| `ack-keys/s`                                     | Keys acknowledged each second. A write batch counts each key.                                                               |
| `transactions/s`, `cycle-keys/s`                 | Delete calls or changed keys each second in the named case.                                                                  |
| `delete-ack-ns`, `post-ack-miss-p50-ns`, `post-ack-miss-p95-ns`, `post-ack-miss-p99-ns` | Delete time and time for the next read to return a miss.                            |
| `keys/s`, `reads/s`, `entries/s`, `operations/s` | Completed work for the named suite.                                                                                         |
| `p50-ns`, `p95-ns`, `p99-ns`, `max-ns` | Operation latency. For example, `p99-ns` gives the time at the 99th percentile of the recorded calls. `max-ns` is the longest recorded call. |
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
| `wal-commits/op`, `wal-B/op`                     | KVLite WAL commits and WAL bytes for each measured operation. Setup is not included. These counts do not depend on timing. |
| `write-calls/op`, `storage-written-B/op`         | Write system calls and bytes sent to storage by the Go process for each measured operation, from `/proc/self/io` on Linux. KVLite and bbolt only. |
| `dataset-memory-B`                               | Redis `used_memory_dataset`. It is an extra Redis-only value.                                                               |
| `checksum`                                       | A deterministic value that proves scan output was consumed.                                                                 |

The suite does not report cross-engine Go allocation values. Redis server allocations do not appear in the Go client process.

`wal-written-B` and `checkpoints` explain KVLite work. They are not cross-engine storage metrics. The counts for each operation come only from the acknowledged-operations cases. Use them to compare two KVLite versions when durable timings are noisy: a change in `wal-commits/op` or `wal-B/op` is a real change in work. `write-calls/op` also counts writes to sockets. `storage-written-B/op` is zero on `tmpfs`. Redis writes in its own server process, so the suite does not report process counters for Redis.

The suite does not yet report a fair combined CPU value, peak resident memory value, or sync-call count. In durable mode, each KVLite WAL commit makes one WAL sync, so `wal-commits/op` gives the WAL sync count. A Go-process value would omit the Redis server. A Redis-server value would omit the Go client. Collect these values at the container boundary before you use them for a cross-engine claim.

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
