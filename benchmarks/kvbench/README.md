# KVBench

KVBench compares KVLite, bbolt, LMDB, and Redis through their public APIs. It runs deterministic workloads, checks the stored results, and records timing, storage use, and environment details. See [benchmark results](../../BENCHMARKS.md) for recorded runs.

KVLite, bbolt, and LMDB are embedded databases running inside the Go process. Redis runs as a separate server, so its measurements include the client, server, and network path. The suite uses each engine's native operations and identifies differences in their contracts below.

## Run locally

Start Docker, then run from this directory. The runner builds and tests inside a pinned Go image; a host Go installation is not required.

```sh
cd benchmarks/kvbench
./run-docker.sh
```

The default is `light`, with the `focused` workload and all four engines. For broader coverage or disk-backed measurements:

```sh
./run-docker.sh medium
./run-docker.sh large --storage=volume
```

Select engines and workloads to limit a run:

```sh
./run-docker.sh light --engines=kvlite,lmdb --storage=volume
./run-docker.sh medium --workloads=reads --engines=kvlite,bbolt,lmdb
```

### Scales and workloads

| Scale | Coverage | Measured samples per case | Default workload | Default storage |
| --- | --- | ---: | --- | --- |
| `light` | Small fixtures and case set | 6: 3 rounds of 2 samples | `focused` | `tmpfs` |
| `medium` | Selected cases from each group | 10 | `all` | `volume` |
| `large` | Complete case matrix | 15 | `all` | `volume` |

Every scale runs one unrecorded warm-up round. Engine order and durability-mode order rotate across measured rounds. Each sample performs a fixed amount of work rather than calibrating a different operation count for each engine.

| Workload | Operations selected |
| --- | --- |
| `focused` | Random point reads, a combined point-update/insert/10-key-batch-update cycle, and random point deletes |
| `reads` | Point and batch reads, read transactions, enumeration, ordered access, access distributions, read latency, and collection reads |
| `writes` | Updates, inserts, batch writes, explicit sync, deletes, space reuse/recovery, write latency, and collection writes |
| `deletes` | Key and bucket deletion, delete visibility, space reuse/recovery, and reopen after deletion |
| `mixed` | Read/write mixes, mixed transactions, batch writes, and mixed latency |
| `all` | Every group available at the selected scale |

Focused runs use one CPU by default; wider workloads use up to four. Medium and large run correctness tests before measurement. Light skips these tests, but the Redis contract test still runs whenever Redis is selected.

### Runner options

Options may appear in any order.

| Option | Purpose |
| --- | --- |
| `--engines=kvlite,bbolt,lmdb,redis` | Comma-separated engine selection; defaults to all four |
| `--workloads=focused\|reads\|writes\|deletes\|mixed\|all` | Select workload groups |
| `--storage=tmpfs\|volume` | Select the benchmark data storage |
| `--case=MODE:CASE` | Run one exact case in one durability mode |
| `--profile` | Profile all selected engines and cases after timing |
| `--profile-engines=kvlite,lmdb` | Profile only these engines while timing all selected engines |
| `--skip-tests` | Skip preliminary tests except the Redis contract test |

`tmpfs` stores benchmark files in container memory. It is useful for quick comparisons but does not measure physical-device synchronization latency. The runner skips its Redis restart durability probe on tmpfs. `volume` uses a temporary Docker volume and measures the Docker host or virtual-machine storage path.

To select one case, copy its name from a report, without the final engine name, and prefix it with `durable:` or `no-commit-sync:`. The scale and workload must include that case. When `--case` is supplied without a workload, the runner selects `all`.

```sh
./run-docker.sh light --engines=kvlite,lmdb --storage=volume \
  --case=durable:BenchmarkAcknowledgedOperations/scale=light/durable/read/random/value=65536/clients=1
```

Environment overrides:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KVBENCH_RESULTS_DIR` | `/tmp/kvlite-kvbench-results` | Output directory |
| `KVBENCH_CPUSET` | One CPU for focused; up to four otherwise | CPU affinity shared by the Go container and Redis |
| `KVBENCH_GO_MEMORY` | Three quarters of Docker VM memory | Go-container memory limit; accepts Docker values such as `8g` |
| `KVBENCH_TRACE_VIEW_MAX_MIB` | `256` | Maximum trace size for generating trace wait views |

The runner pins Go and Redis container images. [go.mod](go.mod) pins the database/client dependencies, including the CGO-based LMDB binding, and uses the current KVLite checkout. A persistent Docker volume, `kvlite-kvbench-go-cache`, caches Go modules and builds separately from benchmark data.

## Workload coverage

Case availability and fixture sizes depend on the scale. Case names record the operation, key/value sizes, client count, batch size, and other relevant parameters.

| Group | Coverage | Engines |
| --- | --- | --- |
| Acknowledged operations | Point reads, hit/miss ratios, updates, inserts, value growth/shrink, write batches, and read/write mixes | All four |
| Batch reads | Ordered, shuffled, random, and repeated requests; hits and misses; 32- and 256-key calls | All four |
| Batch writes | Sequential/random updates, repeated keys, changing values, and fragmented large-value inserts | All four |
| Read and mixed transactions | Multiple operations within one transaction; different batch sizes and read/write ratios | All four |
| Explicit sync | Batch writes followed by an explicit sync, starting in `no-commit-sync` mode | KVLite, bbolt, LMDB |
| Enumeration | Prefix scans at 0%, 1%, 10%, and 100% selectivity | All four |
| Ordered operations | Seek, range scans, and forward/reverse iteration | KVLite, bbolt, LMDB |
| Access distribution | Uniform and 80/20 hot-set reads at different record counts | All four |
| Latency | Point reads, updates, and mixed work at 1, 8, and 32 clients | All four |
| Collections | Reads and writes across collection counts and path depths | All four, with different collection models |
| Lifecycle | Create/load/close, clean open, close after writes, and recovery after a killed writer | KVLite, bbolt, LMDB |
| Deletion and space recovery | Existing/missing keys, single/batch deletes, post-delete reads, reuse cycles, tail reclamation, and reopen | All four where supported; bucket deletion uses the embedded engines |

Large-entry cases include 64 KiB values, 1 MiB value reads, and 32 KiB keys. Light includes a subset of the 64 KiB cases outside `focused`; medium and large expand coverage. **LMDB skips the 32 KiB key cases because its key limit is 511 bytes.** Skipped or unsupported cases are not treated as completed measurements.

## Comparison contract

### API boundaries

| Operation | KVLite | bbolt | LMDB | Redis |
| --- | --- | --- | --- | --- |
| Point read | `DB.Get` | One read transaction | One read transaction | `GET` |
| Batch read | `DB.GetBatch` | One read transaction | One read transaction | `MGET` |
| Point write/delete | `DB.Put` / `DB.Delete` | `Update`; concurrent durable calls use `Batch` | One write transaction | `SET` / `DEL` |
| Batch write | `DB.PutBatch` | One write transaction | One write transaction with a cursor | Atomic `MSET` |
| Batch delete | `DB.DeleteBatch` | One write transaction | One write transaction | Variadic `DEL` |
| Mixed transaction | Bucket batch methods within one `Update` | One write transaction | One write transaction | `MULTI` / `EXEC` |
| Prefix enumeration | Ordered prefix scan | Ordered prefix cursor | Ordered prefix cursor | `SCAN` and `MGET` |
| Key count | Cursor scan | Cursor scan | Cursor scan | `DBSIZE` |
| Collection storage | Nested buckets | Nested buckets | Flat named databases | Logical key prefixes |
| Whole collection delete | `DB.DeleteBucket` | `Tx.DeleteBucket` | Drop named database | Not measured |

Point and batch reads return owned values. Batch reads preserve request order and nil slots for missing keys. Ordered scans consume borrowed values within their callback or transaction lifetime. Redis prefix enumeration has no ordering guarantee.

Native group commit remains enabled. KVLite can group concurrent durable updates; Redis can group AOF writes from concurrent clients. For concurrent durable point writes, bbolt's maximum batch size equals the client count and its maximum admission delay is 100 microseconds. Single-client and no-commit-sync point writes use `Update`.

### Durability modes

| Mode | KVLite | bbolt | LMDB | Redis |
| --- | --- | --- | --- | --- |
| `durable` | `SyncFull` | `NoSync=false`, `NoGrowSync=false` | Default synchronized commits | AOF enabled, `appendfsync always` |
| `no-commit-sync` | `SyncNone`, WAL bypassed | `NoSync=true`, `NoGrowSync=true` | `NoSync\|WriteMap` | AOF enabled, `appendfsync no` |

Durable timings include the engine's acknowledgement and storage-sync boundary. No-commit-sync timings omit commit synchronization and do not provide the same crash guarantees. LMDB's mapped-write configuration is used only on disposable no-commit-sync fixtures. bbolt persists its freelist in both modes.

There is no shared relaxed mode: KVLite's checkpoint-based `SyncNormal`, Redis's time-based `everysec`, and caller-controlled explicit sync have different acknowledgement and loss windows.

### Fixtures, checks, and limits

- Engines receive the same keys, values, and deterministic operation schedule. Fixtures are loaded sequentially in durable mode before switching to the measured mode.
- Core timers cover API calls and required acknowledgements. They exclude open, fixture load, final checks, and close. Lifecycle cases measure those operations separately.
- Inserts use new keys; updates change existing values. Core cases verify stored counts and values. Scans consume their output through a checksum.
- The adapters validate runtime settings, including LMDB flags/map size, bbolt synchronization/batching options, and Redis persistence and connection-pool settings.
- KVLite uses the default `DataPageChecksums=false`; metadata, allocation-page, and WAL validation remain enabled.
- LMDB reserves a 1 GiB map before measurement. Map capacity, logical file length, and allocated disk blocks are distinct measurements.
- Collections retain their native models, identified in case names. LMDB named databases and Redis key prefixes do not implement nested buckets.
- Read cases use warm operating-system caches. Closing and reopening an embedded database does not establish a cold-cache comparison.
- Recovery cases kill the writer after an acknowledged durable write. They test process-crash recovery, not power loss. The volume-backed Docker runner also checks Redis restart behavior separately.
- Space-recovery cases do not invoke bbolt compaction or Redis AOF rewrite. KVLite close includes its final checkpoint.

## Results and metrics

Results are written to `/tmp/kvlite-kvbench-results`, or `KVBENCH_RESULTS_DIR`. Keep the complete output when publishing or comparing a run:

| Output | Contents |
| --- | --- |
| `report.md` | Timing summary, matched engine comparisons, and profile summary when requested |
| `report.json` | Structured results including individual samples |
| `summary.csv`, `metrics.csv`, `comparisons.csv` | Timing statistics, additional metrics, and matched comparisons |
| Raw benchmark output | Original measurements for each durability mode |
| `environment.txt` | Runtime versions, Docker configuration, CPU affinity, and run settings |
| Working-tree patch and `source-sha256.txt` | Source identity, including local changes |
| `profiles/` | Profiles and traces when requested |

The report compares engine medians within a run; it does not compare commits. It separates cases whose observed ranges overlap. Non-overlapping ranges are useful evidence of a gap, not proof that it will persist across environments. Cases without a matching peer have no comparison row.

Compare matching case names and units under the same scale, storage, CPU allocation, and environment. A commit ID alone is insufficient when the run includes working-tree changes; retain its patch and source hashes.

### Reading the units

| Metric | Meaning |
| --- | --- |
| `ns/op` | Time per reported operation; this may be one call or a complete transaction, scan, or fixed pass |
| `batches/op`, `ns/batch` | API calls in a fixed pass, and time per complete batch call |
| `B/op`, `allocs/op` | Go bytes allocated and allocation count per reported operation, where collected |
| `ack-keys/s`, `keys/s`, `reads/s`, `entries/s`, `operations/s` | Completed logical work per second for the named case |
| `p50-ns`, `p95-ns`, `p99-ns`, `max-ns` | Recorded operation-latency percentiles and maximum |
| `delete-ack-ns`, `post-ack-miss-*-ns` | Delete acknowledgement time and latency of the following read returning a miss |
| `primary-B`, `log-B`, `persistent-B` | Logical database bytes, recovery-log bytes, and their sum |
| `os-allocated-B` | Allocated filesystem blocks for embedded database/log files; unavailable for Redis through its client |
| Storage metrics prefixed `loaded-`, `post-delete-`, `active-`, or `stable-` | Storage at the named stage; active precedes close/reopen and stable follows it |
| Metrics containing `growth`, `reclaimed`, or `change` | Storage growth, reclaimed space, or signed size change at the named boundary |
| `close-ns`, `reopen-ns` | Embedded database close/open time |
| `wal-commits/op`, `wal-B/op` | KVLite WAL commits and bytes per measured operation; `/batch` variants use the API-call boundary |
| `write-calls/op`, `storage-written-B/op` | Linux process write calls and storage-write bytes; `/batch` variants use the API-call boundary |
| `wal-written-B`, `checkpoints`, page counters | Additional KVLite work and allocation statistics |
| `dataset-memory-B` | Redis dataset memory |
| `checksum` | Consumed scan-output checksum |

For `BenchmarkGetBatch`, `BenchmarkPutBatch`, and `BenchmarkSync`, **one Go benchmark operation is a fixed pass containing several API calls**. `ns/op`, `B/op`, and `allocs/op` describe that pass; use `ns/batch` for per-call timing. Other groups retain their own operation boundaries.

Latency cases call `time.Now` around each operation. Use them for latency distributions and the acknowledged-operations group for throughput. Payload throughput measures logical bytes returned or changed, not physical disk I/O.

Go allocation figures exclude Redis server allocations and LMDB's native C allocations, so they are not whole-engine memory comparisons. Linux `/proc/self/io` counters cover the embedded process, exclude Redis server writes, and do not directly attribute all mapped writeback to the timed calls. Storage-write bytes are zero on tmpfs. The suite does not report a combined client/server CPU or peak-memory comparison.

## Profiling

Profiling runs **after** the normal, unprofiled measurements. Select engines to profile independently of the engines being compared:

```sh
./run-docker.sh light --workloads=focused --engines=kvlite,bbolt,lmdb,redis \
  --profile-engines=kvlite,lmdb
```

Use `--profile` to profile every selected engine, or combine profiling with `--case` to inspect one case. `--profile` and `--profile-engines` are alternatives. Profiling the full large matrix adds substantial runtime and output.

Each selected case is repeated once per measured round with CPU/allocation profiles and an execution trace. The report normalizes sampled CPU time and allocation bytes by the repeat count. It suppresses CPU rankings below 200 ms of total sampled CPU time and marks rankings below 1 second as limited.

Profiles include fixture setup, final checks, and profiling overhead; use unprofiled timings to assess speed. Redis profiles cover the Go client, and LMDB CPU attribution inside native C code may be incomplete. Raw profiles and full function lists are saved under `profiles/`.

Trace wait views (`net`, `sync`, `syscall`, `sched`) are skipped above the configured trace-size limit to bound conversion memory. The case then contains `wait-views-skipped.txt`.

## Run benchmarks on GitHub Actions

The [KVBench workflow](../../.github/workflows/kvbench.yml) uses this Docker runner. Pull requests run `light`/`focused`; pushes to main run `medium`/`all`. Both select all four engines and skip preliminary tests already covered by the [test workflow](../../.github/workflows/ci.yml).

Manual runs default to `medium`/`all` with all engines and retain preliminary tests. They measure committed code at the selected ref, not local working-tree changes:

```sh
gh workflow run kvbench.yml --ref my-benchmark-branch \
  -f scale=medium -f workload=all -f engines=kvlite,bbolt,lmdb,redis
gh run list --workflow kvbench.yml --branch my-benchmark-branch
gh run download RUN_ID
```

Manual inputs also accept an exact `case` and `profiling=true`. Reports, raw samples, environment records, and profiles are saved in the `kvbench-RUN_ID-RUN_ATTEMPT` artifact for 14 days. Check environment records when comparing separate runs: GitHub-hosted machine and storage variation can affect timings.
