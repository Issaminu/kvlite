# KVLite benchmarks

This report compares KVLite with bbolt, LMDB, and Redis at the public call boundary. It contains the median of 15 measured runs at the complete `large --workloads=all --storage=volume` scale. The same run then profiled every KVLite case.

| Item | Value |
| --- | --- |
| Run time | `2026-10-31T04:13:27Z` |
| Timing duration | 4 hours 12 minutes, including the warm-up round and the correctness tests |
| Profile capture | 38 minutes 49 seconds, KVLite cases only |
| KVLite | `158e97b060a3d506112fb2ad2857951c4e61d209`, clean working tree |
| bbolt | `v1.4.3` |
| LMDB | `0.9.35` through `github.com/PowerDNS/lmdb-go v1.9.4` |
| Redis | `8.8.0` |
| Platform | Linux `arm64`, four CPUs in containers |
| Scale | `large --workloads=all --storage=volume --profile-engines=kvlite` |
| Warm-up | One complete unrecorded round |
| Measurement | 15 measured rounds |

## Results at a glance

A higher throughput value is better. A lower latency or life-cycle value is better. This section shows cases where KVLite had the best median.

| Category | Mode and workload | KVLite | bbolt | LMDB | Redis | Highlight |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Point read | Read-only, random, 128-byte value, eight clients | 6,390,380 keys/s | 956,215 keys/s | 3,547,695 keys/s | 240,688 keys/s | **KVLite: 6.68x bbolt; 1.80x LMDB; 26.55x Redis** |
| Point-read p99 | Read-only, eight clients | 0.71 µs | 6.38 µs | 1.50 µs | 101.13 µs | **KVLite: 8.99x advantage over bbolt; 2.11x over LMDB** |
| Point-read p99 | Read-only, 32 clients | 0.71 µs | 159.29 µs | 1.46 µs | 471.29 µs | **KVLite: 224.67x advantage over bbolt; 2.06x over LMDB** |
| Point update | Durable, random, 128-byte value, one client, batch of 100 | 116,043 keys/s | 18,117 keys/s | 15,742 keys/s | 42,932 keys/s | **KVLite: 6.41x bbolt; 7.37x LMDB; 2.70x Redis** |
| Point insert | Durable, random, 128-byte value, one client, batch of 100 | 103,029 keys/s | 17,082 keys/s | 13,036 keys/s | 39,067 keys/s | **KVLite: 6.03x bbolt; 7.90x LMDB; 2.64x Redis** |
| Mixed work | Durable, 95% reads, eight clients | 116,890 operations/s | 63,195 operations/s | 15,942 operations/s | 10,233 operations/s | **KVLite: 1.85x bbolt; 7.33x LMDB; 11.42x Redis** |
| Large-key read | Read-only, random, 32 KiB key, one client | 114,901 keys/s | 67,294 keys/s | Not supported | 35,984 keys/s | **KVLite: 1.71x bbolt; 3.19x Redis** |
| Create and load | Life cycle | 49.00 ms | 65.68 ms | 55.52 ms | Not comparable | **KVLite: 1.34x advantage over bbolt; 1.13x over LMDB** |

These ratios apply only to the named workloads and public call boundaries. KVLite did not lead every case. LMDB led 67 of 84 no-commit-sync cases. bbolt read 64 KiB values 1.30x faster and 1 MiB values 1.31x faster. bbolt opened a clean database 5.65x faster, and LMDB 3.06x faster. The full tables show every case.

## Scope

This run measured every workload group and every case variant at the large scale. It covers point operations, deletion, transactions, enumeration, ordered operations, access distributions, latency, collections, storage size, and embedded life-cycle operations.

The run includes large-entry cases. These cases use 64 KiB values, 1 MiB values, or 32 KiB keys. A large entry needs continuation pages in KVLite. LMDB skips the 32 KiB key cases, because its key limit is 511 bytes.

## Fairness and API boundaries

The runner gave each engine the same fixed operation schedule and data set. It ran the engines serially. It changed the engine order and durability-mode order between measured rounds. Each engine used the same four-CPU set and Docker storage class.

| Logical operation | KVLite | bbolt | LMDB | Redis |
| --- | --- | --- | --- | --- |
| Point read | `DB.Get` | One read transaction | One read transaction | `GET` |
| Batch read | `DB.GetBatch` | One read transaction | One read transaction | `MGET` |
| Point write or delete | `DB.Put` or `DB.Delete` | `DB.Update`; concurrent durable calls use `DB.Batch` | One write transaction | `SET` or `DEL` |
| Batch write | `DB.PutBatch` | One write transaction | One write transaction with a cursor | `MSET` |
| Batch delete | `DB.DeleteBatch` | One write transaction | One write transaction | One variadic `DEL` |
| Mixed transaction | Bucket batch methods in one `DB.Update` | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Prefix enumeration | Ordered prefix scan | Ordered prefix cursor | Ordered prefix cursor | `SCAN` and `MGET` |
| Collection storage | Nested buckets | Nested buckets | Named databases | Key prefixes |

Point and batch reads return owned values in every adapter. Redis includes its client, server, and local network path. These are public call comparisons. They do not compare identical internal operations.

## Durability modes

| Mode | KVLite | bbolt | LMDB | Redis |
| --- | --- | --- | --- | --- |
| Durable | `SyncFull` | `NoSync=false`; `NoGrowSync=false` | Default synchronized commits | AOF with `appendfsync always` |
| No commit sync | `SyncNone`, WAL enabled | `NoSync=true`; `NoGrowSync=true` | `NoSync` and `WriteMap` | AOF with `appendfsync no` |

A successful durable write waits for the durability boundary of its engine. In no-commit-sync mode, no engine asks storage to sync during commit. Durable and no-commit-sync results have different guarantees. This report keeps them separate. In no-commit-sync mode, KVLite still writes each commit to its WAL, and LMDB writes directly into its mapped file.

The harness opens KVLite with the default `DataPageChecksums=false`. KVLite therefore does not calculate or check checksums for leaf, branch, and continuation pages in this run. Metadata pages, allocation pages, and WAL records keep their checksums.

## Test environment

- CPU set: `0-3`
- Go: `go1.26.7 linux/arm64`
- KVLite: `158e97b060a3d506112fb2ad2857951c4e61d209`
- bbolt: `v1.4.3`
- LMDB: `0.9.35`, Go binding `github.com/PowerDNS/lmdb-go v1.9.4`, 1 GiB map
- Redis client: `go-redis v9.22.0`
- Redis server: `8.8.0`, `jemalloc-5.3.0`
- Container system: OrbStack with Linux kernel `7.2.12-orbstack-00394-gbecdd6130068`
- Storage driver: `overlay2`
- Host: Apple M4 Max; 14 CPUs visible to the container system
- CPUs assigned to each measured process and its Redis server: four
- Go container memory limit: 12,613,757,952 bytes
- Benchmark data storage: temporary Docker volume

The Docker volume path includes the OrbStack virtual machine and the host storage path. It does not prove bare-metal device performance. Other containers were active on the host. CPU assignment reduced direct CPU competition. Other host work could still affect the results.

## Method

The runner first completed its correctness tests. It then completed one unrecorded warm-up round. It created a new database fixture for each benchmark process. It did not reuse a measured database from the warm-up round.

The runner recorded 15 fixed-work rounds. It changed the engine order and durability-mode order between rounds. Pure reads ran once per round because commit-sync settings do not affect them.

Setup, fixture loading, final stored-data checks, and close operations were outside the core timers. Before a timed write workload in the acknowledged-operation, focused-write, and key-deletion cases, the harness makes one untimed commit on the new handle. The named life-cycle cases measure open, close, and recovery work separately.

After the 15 timed rounds, the runner profiled every KVLite case again with 15 repeats. It recorded CPU profiles, allocation profiles, and execution traces. The profile pass does not change the timing values in this report.

## Changes since the previous report

The previous committed report measured `2196993` on 2026-10-21. The raw samples of a later full capture on 2026-10-29 were kept, so this section compares with that capture. It measured the work that came before the node-ownership refactor (`af7bd3d`). This run measures that refactor and three later commits: `ef6d32e`, which stops a transaction after a write fails part way, `f7300d0` (documentation), and `158e97b` (CI only).

The table gives the geometric mean of the median time ratio, this run over the 2026-10-29 capture, for each workload group. A ratio below 1 means a lower time. The code of bbolt, LMDB, and Redis did not change, so their columns show host drift.

| Workload group | Cases | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: | ---: |
| Point reads | 14 | 0.903 | 0.986 | 0.970 | 0.976 |
| Batch reads | 14 | 0.941 | 0.998 | 0.999 | 0.962 |
| Read transactions | 4 | 0.895 | 1.017 | 0.962 | 0.979 |
| Enumeration and ordered operations | 14 | 1.063 | 0.999 | 0.990 | 0.990 |
| Access-distribution reads | 4 | 0.924 | 1.007 | 1.005 | 0.973 |
| No-commit-sync point updates | 21 | 0.952 | 1.018 | 0.962 | 0.988 |
| No-commit-sync point inserts | 13 | 0.992 | 0.979 | 0.966 | 1.012 |
| No-commit-sync batch writes | 9 | 1.048 | 0.905 | 0.992 | 0.943 |
| Durable point updates | 21 | 1.124 | 1.076 | 1.220 | 1.204 |
| Durable point inserts | 13 | 1.082 | 1.060 | 1.210 | 1.066 |
| Durable batch writes | 9 | 1.166 | 0.946 | 1.273 | 1.026 |
| Key deletion | 20 | 1.027 | 1.022 | 0.970 | 1.101 |
| Life cycle | 4 | 0.898 | 0.909 | 0.970 | No result |
| All KVLite rows | 227 | 0.999 | 1.028 | 1.012 | 1.040 |

LMDB took 21% to 27% more time and Redis 3% to 20% more time in the durable write groups, but their code and settings did not change. So the durable write groups mix code changes with storage-sync drift.

Paired runs then measured the library of the 2026-10-29 capture and the library of `158e97b` in one container, with a second copy of one build as a same-binary control. These runs show the code effects below.

- **Reads are faster.** Point reads of small values are 8% to 14% faster in the full run. Unbounded scans, batch reads, read transactions, and collection reads are 7% to 20% faster. The refactor reads committed nodes in their encoded form, so a read no longer decodes branch nodes or fills a node cache.
- **Bounded scans were slower.** `Enumeration/selectivity=0` took 1.91 times as long, `selectivity=1` 1.20 times as long, and `range/selectivity=1` 1.21 times as long. A bounded scan checked every separator key of each branch that it entered. The old code checked only the separators that it read. A tested patch restores that rule and adds a test for the end bound. In six paired rounds with all patches, the patched code is equal to the old code in these cases (0.97 to 1.18, not significant).
- **Long cursor walks were slower.** `seek-and-read=100` took 1.13 times as long, and `seek-and-read=1000` 1.20 times as long. Each `Cursor.Next` made two calls instead of one, and `Cursor.Entry` built a full entry value. Two tested patches move the entry checks back into the cursor step and return the cursor fields directly. In 12 paired rounds with both patches, `seek-and-read=100` is 2.9% faster than the old code (p = 0.009), and `seek-and-read=1000` is equal (p = 0.885). In the six-round check of all patches, `seek-and-read=1000` was 2.2% slower (p = 0.005).
- **No-commit-sync batch writes were slower.** `PutBatch replace` with 256 keys took 1.34 times as long, and updates and inserts with batches of 100 or 1,000 up to 1.26 times as long. Each write allocated its tree path on the heap, and `PutEntry` searched the leaf twice. Two tested patches use a stack array for the path again and search the leaf once. In eight paired rounds, the batch update and insert cases are now equal to the old code (0.95 to 1.02, mostly not significant). In the six-round check of all patches, the update batch of 1,000 keys was 8% slower (p = 0.045), and `PutBatch replace` with 256 keys was 13% slower (p = 0.005). The remaining `PutBatch` cost is GC work: the patched code runs 650 GC cycles where the old code runs 588, because the smaller live heap gives a lower GC goal.
- **Durable large-value writes did not get slower.** The full run showed 1.9 to 3.2 times longer durable writes of 64 KiB values, but bbolt was also 1.2 to 2.0 times slower in those cases, and the samples of both engines had two groups of times. KVLite now writes fewer bytes for a 64 KiB insert: 0.64 times the storage bytes and 0.73 times the WAL bytes. In 12 paired rounds, the 64 KiB insert is 2.3 times faster (p = 0.023), and the other large-value, reopen, and update-latency cases show no significant change.
- **Random deletes are slower.** In paired runs, a single random delete takes 1.10 times as long, and a delete batch of 100 keys 1.16 times as long. The patches above do not change these cases. The causes are small: the first edit copies the whole leaf before it removes the entry, each write transaction creates a parent map when it opens a bucket, and commit starts one more map iteration. A patch that makes one allocation for the borrowed bytes of a node, instead of one for each key and each value, removed 45% of the allocations but did not change the time.

The patches are not part of `158e97b`. The full results below show the measured commit without them.

## Interpretation

These statements apply only to this run and its tested workloads.

- KVLite led 98 of 143 durable cases. Redis led 28. bbolt led 11. LMDB led six.
- KVLite led 13 of 84 no-commit-sync cases. LMDB led 67. Redis led four.
- Each count uses one main metric for each case: the median time per operation, or p99 for latency cases. It excludes storage-size metrics.
- KVLite led 38 of 43 read cases. bbolt led 64 KiB value reads, 1 MiB value reads, `Enumeration/selectivity=0`, `seek-and-read=100`, and `seek-and-read=1000`. The tested patches above fix the last three cases.
- KVLite led 11 of 21 durable point-update cases and eight of 13 durable point-insert cases. Redis led most of the other cases.
- LMDB led 17 of 21 no-commit-sync point-update cases and all 13 no-commit-sync point-insert cases. In this mode, LMDB writes into its mapped file, and KVLite still writes and encodes a WAL record for each commit.
- KVLite led six of seven durable mixed-work cases. LMDB led the case with 64 KiB values.
- LMDB led 16 of 32 deletion, reuse, and reopen cases. KVLite led nine. Redis led seven.
- KVLite created and loaded a database 1.34 times faster than bbolt. A clean open took 81.29 µs for KVLite, 14.38 µs for bbolt, and 26.54 µs for LMDB. In a micro benchmark, the creation of the new WAL takes about 40% of a clean KVLite open, and the read of the allocation bitmap about 32%. The patches do not change the open time (paired micro benchmark, not significant). The recovery open after a killed process took 5.06 ms for KVLite and 16.38 µs for bbolt. bbolt closed after writes 250 times faster, because KVLite checkpoints its WAL during close.

## Limits

- This is one run on one container host. It is not a bare-metal Linux result.
- Docker volume storage includes the OrbStack virtual machine and host storage path. It does not isolate physical-device sync latency.
- Durable write times depend on storage sync latency. The durable groups of LMDB and Redis changed by up to 27% between this run and the 2026-10-29 capture with the same code.
- The tested patches were measured in paired runs of selected cases, not in a new full run.
- Each timed run opens a new handle. The acknowledged-operation, focused-write, and key-deletion cases start their timers after one commit. Other cases with timed writes still include the first commit of the handle.
- Read tests use a warm operating-system cache. The suite does not claim cold-cache performance.
- KVLite ran with the default `DataPageChecksums=false`.
- The suite does not rank combined CPU use or peak resident memory across embedded and client-server designs. Go allocation figures exclude LMDB's native C allocations and Redis server allocations.
- Redis has no result for ordered cursor or embedded life-cycle operations. A Redis profile covers its Go client process, not the Redis server.
- KVLite performs a final checkpoint during close. The close-after-writes case does not compare the same work across the embedded engines.
- Persistent byte counts cover different file designs and maintenance rules. LMDB reserves a 1 GiB map. These counts are not a direct storage-efficiency ranking.

## Reproduce

```sh
cd benchmarks/kvbench
./run-docker.sh large --workloads=all --storage=volume --profile-engines=kvlite
```

The runner uses pinned Go and Redis container images. The Go module pins bbolt, the LMDB binding, and the Redis client. Omit `--profile-engines` for a timing-only run.

## Full results

Every value is the median of 15 measured runs.

### Acknowledged operations

#### Durable

| Case | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: |
| read/random/hits=0/misses=above/value=128/clients=1 | 8,720,773 keys/s | 1,668,405 keys/s | 2,526,363 keys/s | 142,535 keys/s |
| read/random/hits=0/misses=below/value=128/clients=1 | 8,221,018 keys/s | 1,792,965 keys/s | 2,544,477 keys/s | 143,885 keys/s |
| read/random/hits=0/misses=between/value=128/clients=1 | 6,967,484 keys/s | 1,672,400 keys/s | 2,363,136 keys/s | 142,833 keys/s |
| read/random/hits=50/value=128/clients=1 | 5,349,380 keys/s | 1,502,331 keys/s | 2,112,389 keys/s | 152,233 keys/s |
| read/random/hits=100/value=128/clients=8 | 6,390,380 keys/s | 956,215 keys/s | 3,547,695 keys/s | 240,688 keys/s |
| read/random/hits=100/value=128/clients=32 | 5,951,327 keys/s | 877,602 keys/s | 3,889,282 keys/s | 318,877 keys/s |
| read/random/key=32768/value=128/clients=1 | 114,901 keys/s | 67,294 keys/s | — | 35,984 keys/s |
| read/random/value=32/clients=1 | 4,625,536 keys/s | 1,294,525 keys/s | 2,071,021 keys/s | 170,552 keys/s |
| read/random/value=128/clients=1 | 4,294,046 keys/s | 1,336,584 keys/s | 1,962,634 keys/s | 159,952 keys/s |
| read/random/value=1024/clients=1 | 2,639,441 keys/s | 1,366,678 keys/s | 1,414,305 keys/s | 154,884 keys/s |
| read/random/value=3072/clients=1 | 1,646,756 keys/s | 1,192,037 keys/s | 1,172,392 keys/s | 133,881 keys/s |
| read/random/value=65536/clients=1 | 166,718 keys/s | 217,272 keys/s | 168,066 keys/s | 48,905 keys/s |
| read/random/value=1048576/clients=1 | 18,108 keys/s | 23,710 keys/s | 22,671 keys/s | 6,401 keys/s |
| read/sequential/hits=100/value=128/clients=1 | 6,021,497 keys/s | 1,551,388 keys/s | 2,314,728 keys/s | 168,675 keys/s |
| update/random/grow=32-1024/clients=1 | 1,447 keys/s | 752 keys/s | 696 keys/s | 1,479 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 23,773 keys/s | 12,239 keys/s | 12,294 keys/s | 33,165 keys/s |
| update/random/grow=128-65536/clients=1 | 367 keys/s | 437 keys/s | 239 keys/s | 387 keys/s |
| update/random/shrink=1024-32/clients=1 | 1,569 keys/s | 885 keys/s | 820 keys/s | 1,971 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 36,528 keys/s | 12,621 keys/s | 13,289 keys/s | 47,131 keys/s |
| update/random/shrink=65536-128/clients=1 | 1,800 keys/s | 730 keys/s | 789 keys/s | 1,651 keys/s |
| update/random/value=32/clients=1 | 1,246 keys/s | 695 keys/s | 812 keys/s | 1,601 keys/s |
| update/random/value=128/clients=1 | 1,485 keys/s | 883 keys/s | 858 keys/s | 1,466 keys/s |
| update/random/value=128/clients=1/batch=10 | 15,995 keys/s | 3,463 keys/s | 3,825 keys/s | 12,363 keys/s |
| update/random/value=128/clients=1/batch=100 | 116,043 keys/s | 18,117 keys/s | 15,742 keys/s | 42,932 keys/s |
| update/random/value=128/clients=1/batch=1000 | 305,413 keys/s | 107,607 keys/s | 164,712 keys/s | 331,677 keys/s |
| update/random/value=128/clients=8 | 10,890 keys/s | 5,188 keys/s | 817 keys/s | 9,802 keys/s |
| update/random/value=128/clients=8/batch=100 | 207,703 keys/s | 20,591 keys/s | 20,275 keys/s | 227,503 keys/s |
| update/random/value=128/clients=32 | 34,448 keys/s | 9,640 keys/s | 750 keys/s | 9,730 keys/s |
| update/random/value=128/clients=32/batch=100 | 384,196 keys/s | 19,672 keys/s | 14,853 keys/s | 424,357 keys/s |
| update/random/value=1024/clients=1 | 1,528 keys/s | 916 keys/s | 807 keys/s | 1,408 keys/s |
| update/random/value=3072/clients=1 | 1,356 keys/s | 836 keys/s | 815 keys/s | 436 keys/s |
| update/random/value=65536/clients=1 | 437 keys/s | 368 keys/s | 192 keys/s | 1,043 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 15,743 keys/s | 6,342 keys/s | 8,158 keys/s | 11,657 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 106,842 keys/s | 28,719 keys/s | 27,041 keys/s | 41,553 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 610,716 keys/s | 253,247 keys/s | 258,829 keys/s | 389,031 keys/s |
| insert/random/value=128/clients=1 | 1,701 keys/s | 844 keys/s | 842 keys/s | 1,860 keys/s |
| insert/random/value=128/clients=1/batch=10 | 14,673 keys/s | 1,998 keys/s | 2,043 keys/s | 11,201 keys/s |
| insert/random/value=128/clients=1/batch=100 | 103,029 keys/s | 17,082 keys/s | 13,036 keys/s | 39,067 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 267,430 keys/s | 190,847 keys/s | 194,401 keys/s | 376,454 keys/s |
| insert/random/value=128/clients=8 | 11,870 keys/s | 4,078 keys/s | 404 keys/s | 7,466 keys/s |
| insert/random/value=128/clients=8/batch=100 | 175,439 keys/s | 16,090 keys/s | 14,906 keys/s | 221,008 keys/s |
| insert/random/value=128/clients=32 | 33,798 keys/s | 7,470 keys/s | 781 keys/s | 10,566 keys/s |
| insert/random/value=128/clients=32/batch=100 | 335,070 keys/s | 15,647 keys/s | 14,106 keys/s | 531,629 keys/s |
| insert/random/value=65536/clients=1 | 375 keys/s | 521 keys/s | 276 keys/s | 1,277 keys/s |
| insert/sequential/value=128/clients=1 | 1,827 keys/s | 934 keys/s | 876 keys/s | 1,787 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 15,647 keys/s | 5,253 keys/s | 7,342 keys/s | 12,660 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 107,812 keys/s | 30,416 keys/s | 20,259 keys/s | 39,016 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 490,103 keys/s | 230,177 keys/s | 228,233 keys/s | 341,434 keys/s |
| mixed/read=50/value=128/clients=1 | 4,365 operations/s | 1,285 operations/s | 689 operations/s | 3,226 operations/s |
| mixed/read=50/value=128/clients=8 | 22,660 operations/s | 7,241 operations/s | 1,477 operations/s | 10,717 operations/s |
| mixed/read=50/value=128/clients=32 | 57,882 operations/s | 11,111 operations/s | 1,676 operations/s | 29,270 operations/s |
| mixed/read=95/value=128/clients=1 | 32,229 operations/s | 14,020 operations/s | 15,988 operations/s | 9,683 operations/s |
| mixed/read=95/value=128/clients=8 | 116,890 operations/s | 63,195 operations/s | 15,942 operations/s | 10,233 operations/s |
| mixed/read=95/value=128/clients=32 | 267,301 operations/s | 70,967 operations/s | 14,804 operations/s | 15,828 operations/s |
| mixed/read=95/value=65536/clients=1 | 9,377 operations/s | 7,777 operations/s | 15,741 operations/s | 9,573 operations/s |

#### No commit sync

| Case | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 172,535 keys/s | 86,527 keys/s | 616,060 keys/s | 105,065 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 251,537 keys/s | 258,562 keys/s | 1,209,406 keys/s | 1,018,728 keys/s |
| update/random/grow=128-65536/clients=1 | 54,285 keys/s | 29,297 keys/s | 232,053 keys/s | 28,905 keys/s |
| update/random/shrink=1024-32/clients=1 | 410,422 keys/s | 87,856 keys/s | 534,613 keys/s | 85,640 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 602,718 keys/s | 293,451 keys/s | 1,085,749 keys/s | 1,805,525 keys/s |
| update/random/shrink=65536-128/clients=1 | 437,371 keys/s | 42,714 keys/s | 208,199 keys/s | 133,217 keys/s |
| update/random/value=32/clients=1 | 161,371 keys/s | 62,953 keys/s | 1,365,231 keys/s | 102,611 keys/s |
| update/random/value=128/clients=1 | 341,843 keys/s | 80,282 keys/s | 1,006,869 keys/s | 89,086 keys/s |
| update/random/value=128/clients=1/batch=10 | 621,096 keys/s | 179,815 keys/s | 2,451,793 keys/s | 793,806 keys/s |
| update/random/value=128/clients=1/batch=100 | 626,841 keys/s | 434,935 keys/s | 2,822,134 keys/s | 2,249,796 keys/s |
| update/random/value=128/clients=1/batch=1000 | 942,164 keys/s | 577,170 keys/s | 2,828,183 keys/s | 2,966,844 keys/s |
| update/random/value=128/clients=8 | 247,738 keys/s | 56,087 keys/s | 1,085,956 keys/s | 178,586 keys/s |
| update/random/value=128/clients=8/batch=100 | 418,899 keys/s | 317,988 keys/s | 2,685,924 keys/s | 1,816,508 keys/s |
| update/random/value=128/clients=32 | 211,942 keys/s | 48,237 keys/s | 1,187,823 keys/s | 195,080 keys/s |
| update/random/value=128/clients=32/batch=100 | 547,146 keys/s | 358,810 keys/s | 2,525,034 keys/s | 2,636,878 keys/s |
| update/random/value=1024/clients=1 | 345,618 keys/s | 88,048 keys/s | 525,616 keys/s | 91,077 keys/s |
| update/random/value=3072/clients=1 | 299,715 keys/s | 82,444 keys/s | 406,313 keys/s | 79,028 keys/s |
| update/random/value=65536/clients=1 | 56,799 keys/s | 35,871 keys/s | 116,692 keys/s | 30,861 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 1,616,769 keys/s | 555,985 keys/s | 5,147,987 keys/s | 704,441 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 2,745,343 keys/s | 2,030,479 keys/s | 7,254,388 keys/s | 2,384,297 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 2,781,509 keys/s | 2,322,113 keys/s | 7,953,412 keys/s | 3,130,484 keys/s |
| insert/random/value=128/clients=1 | 527,118 keys/s | 68,491 keys/s | 1,214,294 keys/s | 92,068 keys/s |
| insert/random/value=128/clients=1/batch=10 | 436,491 keys/s | 173,327 keys/s | 2,294,272 keys/s | 567,260 keys/s |
| insert/random/value=128/clients=1/batch=100 | 525,979 keys/s | 312,059 keys/s | 2,613,719 keys/s | 1,402,900 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 789,287 keys/s | 739,245 keys/s | 2,904,165 keys/s | 1,919,113 keys/s |
| insert/random/value=128/clients=8 | 224,203 keys/s | 40,111 keys/s | 1,195,305 keys/s | 182,290 keys/s |
| insert/random/value=128/clients=8/batch=100 | 370,378 keys/s | 241,768 keys/s | 2,459,358 keys/s | 1,551,661 keys/s |
| insert/random/value=128/clients=32 | 179,950 keys/s | 35,934 keys/s | 1,340,117 keys/s | 216,637 keys/s |
| insert/random/value=128/clients=32/batch=100 | 438,062 keys/s | 264,360 keys/s | 2,531,581 keys/s | 1,804,930 keys/s |
| insert/random/value=65536/clients=1 | 55,658 keys/s | 23,324 keys/s | 150,372 keys/s | 30,132 keys/s |
| insert/sequential/value=128/clients=1 | 527,826 keys/s | 72,706 keys/s | 1,426,548 keys/s | 103,417 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 1,344,542 keys/s | 394,591 keys/s | 5,374,145 keys/s | 591,245 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 2,051,801 keys/s | 1,763,132 keys/s | 7,394,503 keys/s | 1,524,022 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 2,612,239 keys/s | 2,894,931 keys/s | 7,703,383 keys/s | 1,928,864 keys/s |
| mixed/read=50/value=128/clients=1 | 772,690 operations/s | 156,142 operations/s | 1,640,551 operations/s | 137,564 operations/s |
| mixed/read=50/value=128/clients=8 | 545,872 operations/s | 81,896 operations/s | 1,372,847 operations/s | 219,755 operations/s |
| mixed/read=50/value=128/clients=32 | 473,605 operations/s | 69,687 operations/s | 689,032 operations/s | 235,670 operations/s |
| mixed/read=95/value=128/clients=1 | 1,951,360 operations/s | 629,290 operations/s | 1,876,468 operations/s | 155,065 operations/s |
| mixed/read=95/value=128/clients=8 | 1,054,849 operations/s | 381,239 operations/s | 1,736,755 operations/s | 227,218 operations/s |
| mixed/read=95/value=128/clients=32 | 1,267,008 operations/s | 412,327 operations/s | 1,899,484 operations/s | 271,830 operations/s |
| mixed/read=95/value=65536/clients=1 | 189,338 operations/s | 187,098 operations/s | 200,749 operations/s | 47,649 operations/s |

### Key deletion: durable

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 1,477 ack-keys/s | 561 ack-keys/s | 694 ack-keys/s | 1,412 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,724,480 B | 1,950,625 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 102,646 ack-keys/s | 38,350 ack-keys/s | 38,060 ack-keys/s | 123,663 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 18,526,208 B | 4,194,304 B | 2,588,736 B | 1,822,125 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 596,143 ack-keys/s | 206,756 ack-keys/s | 343,598 ack-keys/s | 411,199 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 18,526,208 B | 8,388,608 B | 4,841,536 B | 1,820,785 B |
| `existing/random/clients=8` | `ack-keys/s` | 10,381 ack-keys/s | 3,242 ack-keys/s | 417 ack-keys/s | 9,635 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,728,576 B | 1,950,625 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 1,356 ack-keys/s | 639 ack-keys/s | 830 ack-keys/s | 1,514 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 54,640,640 B | 51,425,280 B | 35,872,832 B | 33,711,836 B |
| `existing/random/value=65536/clients=1/batch=32` | `ack-keys/s` | 18,155 ack-keys/s | 5,996 ack-keys/s | 20,537 ack-keys/s | 22,998 ack-keys/s |
| `existing/random/value=65536/clients=1/batch=32` | `persistent-B` | 54,640,640 B | 51,425,280 B | 35,872,832 B | 33,714,620 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 1,573 ack-keys/s | 358 ack-keys/s | 611 ack-keys/s | 1,082 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,728,576 B | 1,950,625 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 129,427 ack-keys/s | 87,147 ack-keys/s | 84,549 ack-keys/s | 49,390 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,728,576 B | 1,822,125 B |
| `existing/sequential/value=65536/clients=1/batch=32` | `ack-keys/s` | 18,700 ack-keys/s | 12,044 ack-keys/s | 13,610 ack-keys/s | 19,579 ack-keys/s |
| `existing/sequential/value=65536/clients=1/batch=32` | `persistent-B` | 54,640,640 B | 51,425,280 B | 35,872,832 B | 33,714,620 B |
| `missing/random/clients=1` | `ack-keys/s` | 496,779 ack-keys/s | 839 ack-keys/s | 3,785,258 ack-keys/s | 157,135 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,720,384 B | 1,590,625 B |

### Key deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 477,916 ack-keys/s | 79,865 ack-keys/s | 1,231,047 ack-keys/s | 108,267 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,950,625 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 750,925 ack-keys/s | 485,684 ack-keys/s | 2,886,035 ack-keys/s | 2,642,234 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,822,125 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 1,622,264 ack-keys/s | 849,565 ack-keys/s | 2,747,626 ack-keys/s | 3,079,520 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 18,526,208 B | 6,045,696 B | 1,073,750,080 B | 1,820,785 B |
| `existing/random/clients=8` | `ack-keys/s` | 321,564 ack-keys/s | 56,546 ack-keys/s | 1,030,109 ack-keys/s | 219,574 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,950,625 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 171,853 ack-keys/s | 26,030 ack-keys/s | 190,106 ack-keys/s | 133,086 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 54,640,640 B | 51,425,280 B | 1,073,750,080 B | 33,711,836 B |
| `existing/random/value=65536/clients=1/batch=32` | `ack-keys/s` | 409,154 ack-keys/s | 45,926 ack-keys/s | 1,052,322 ack-keys/s | 984,771 ack-keys/s |
| `existing/random/value=65536/clients=1/batch=32` | `persistent-B` | 54,640,640 B | 51,425,280 B | 1,073,750,080 B | 33,714,620 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 426,761 ack-keys/s | 83,615 ack-keys/s | 1,187,014 ack-keys/s | 116,549 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,950,625 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 8,041,352 ack-keys/s | 2,374,223 ack-keys/s | 4,971,256 ack-keys/s | 2,403,397 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,822,125 B |
| `existing/sequential/value=65536/clients=1/batch=32` | `ack-keys/s` | 474,875 ack-keys/s | 333,094 ack-keys/s | 1,223,627 ack-keys/s | 956,857 ack-keys/s |
| `existing/sequential/value=65536/clients=1/batch=32` | `persistent-B` | 54,640,640 B | 51,425,280 B | 1,073,750,080 B | 33,714,620 B |
| `missing/random/clients=1` | `ack-keys/s` | 2,190,786 ack-keys/s | 251,395 ack-keys/s | 4,148,061 ack-keys/s | 168,090 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,590,625 B |

### Bucket deletion: durable

| Case | Metric | KVLite | bbolt | LMDB |
| --- | --- | ---: | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 1,618 buckets/s | 892 buckets/s | 765 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 18,919,424 B | 4,194,304 B | 2,113,600 B |

### Bucket deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | LMDB |
| --- | --- | ---: | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 111,311 buckets/s | 65,020 buckets/s | 259,039 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 18,919,424 B | 4,194,304 B | 1,073,750,080 B |

### Read after delete: durable

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 671.94 µs | 1.78 ms | 3.76 ms | 667.41 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 917 ns | 1.46 µs | 2.12 µs | 13.67 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 1.25 µs | 14.46 µs | 27.95 µs | 23.43 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 1.54 µs | 21.29 µs | 57.47 µs | 34.92 µs |

### Read after delete: no-commit-sync

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 2.42 µs | 21.18 µs | 1.19 µs | 7.24 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 250 ns | 583 ns | 500 ns | 6.67 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 292 ns | 1.96 µs | 584 ns | 8.50 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 334 ns | 3.08 µs | 708 ns | 13.92 µs |

### Page reuse: durable

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 70,407 cycle-keys/s | 18,818 cycle-keys/s | 14,072 cycle-keys/s | 98,671 cycle-keys/s |
| `random-half/batch=100` | `loaded-persistent-B` | 6,029,312 B | 16,777,216 B | 6,942,784 B | 5,430,281 B |
| `random-half/batch=100` | `post-delete-persistent-B` | 9,252,425 B | 16,777,216 B | 7,962,688 B | 5,546,031 B |
| `random-half/batch=100` | `active-post-reinsert-persistent-B` | 11,111,272 B | 16,777,216 B | 12,140,608 B | 8,261,831 B |
| `random-half/batch=100` | `stable-post-reinsert-persistent-B` | 9,986,048 B | 16,777,216 B | 12,140,608 B | — |
| `random-half/batch=100` | `reuse-active-growth-B` | 5,081,960 B | 0 B | 5,197,824 B | 2,831,550 B |
| `random-half/batch=100` | `reuse-stable-growth-B` | 3,956,736 B | 0 B | 5,197,824 B | — |
| `random-half/batch=100` | `reinsert-pages-reused` | 82 | — | — | — |

### Page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 582,225 cycle-keys/s | 344,464 cycle-keys/s | 1,640,616 cycle-keys/s | 1,369,924 cycle-keys/s |
| `random-half/batch=100` | `loaded-persistent-B` | 6,029,312 B | 16,777,216 B | 1,073,750,080 B | 5,430,281 B |
| `random-half/batch=100` | `post-delete-persistent-B` | 9,252,425 B | 16,777,216 B | 1,073,750,080 B | 5,546,031 B |
| `random-half/batch=100` | `active-post-reinsert-persistent-B` | 11,111,272 B | 16,777,216 B | 1,073,750,080 B | 8,261,831 B |
| `random-half/batch=100` | `stable-post-reinsert-persistent-B` | 9,986,048 B | 16,777,216 B | 1,073,750,080 B | — |
| `random-half/batch=100` | `reuse-active-growth-B` | 5,081,960 B | 0 B | 0 B | 2,831,550 B |
| `random-half/batch=100` | `reuse-stable-growth-B` | 3,956,736 B | 0 B | 0 B | — |
| `random-half/batch=100` | `reinsert-pages-reused` | 82 | — | — | — |

### Repeated page reuse: durable

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 29,334 cycle-keys/s | 19,784 cycle-keys/s | 12,189 cycle-keys/s | 43,305 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,847,360 B | 16,777,216 B | 14,983,232 B | 5,430,281 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 13,729,122 B | 16,777,216 B | 14,983,232 B | 33,745,781 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 11,231,409 B | 16,777,216 B | 14,983,232 B | 33,745,781 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,744,960 B | 16,777,216 B | 14,983,232 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -102,400 B | 0 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 10,083 | — | — | — |

### Repeated page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 656,887 cycle-keys/s | 500,929 cycle-keys/s | 2,394,211 cycle-keys/s | 2,346,201 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,847,360 B | 16,777,216 B | 1,073,750,080 B | 5,430,281 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 13,729,122 B | 16,777,216 B | 1,073,750,080 B | 33,745,781 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 11,231,409 B | 16,777,216 B | 1,073,750,080 B | 33,745,781 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,744,960 B | 16,777,216 B | 1,073,750,080 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -102,400 B | 0 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 10,083 | — | — | — |

### Tail reclamation: durable

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 38,383 ack-keys/s | 47,033 ack-keys/s | 66,269 ack-keys/s | 50,182 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 6,029,312 B | 16,777,216 B | 6,942,784 B | 5,430,281 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 22,806,528 B | 16,777,216 B | 6,946,880 B | 5,546,031 B |
| `upper-half/batch=100` | `final-persistent-B` | 3,059,712 B | 16,777,216 B | 6,946,880 B | 5,546,031 B |
| `upper-half/batch=100` | `close-ns` | 3.29 ms | 65.04 µs | 29.00 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 191.85 µs | 37.84 µs | 41.08 µs | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 725 | — | — | — |

### Tail reclamation: no-commit-sync

| Case | Metric | KVLite | bbolt | LMDB | Redis |
| --- | --- | ---: | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 3,899,517 ack-keys/s | 1,887,305 ack-keys/s | 3,689,574 ack-keys/s | 2,275,629 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 6,029,312 B | 16,777,216 B | 1,073,750,080 B | 5,430,281 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 22,806,528 B | 16,777,216 B | 1,073,750,080 B | 5,546,031 B |
| `upper-half/batch=100` | `final-persistent-B` | 3,059,712 B | 16,777,216 B | 1,073,750,080 B | 5,546,031 B |
| `upper-half/batch=100` | `close-ns` | 279.09 µs | 63.00 µs | 22.46 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 77.63 µs | 34.75 µs | 30.29 µs | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 725 | — | — | — |

### Reopen after deletion: durable

| Case | Metric | KVLite | bbolt | LMDB |
| --- | --- | ---: | ---: | ---: |
| `alternating-half` | `reopen-ns` | 164.83 µs | 32.08 µs | 30.00 µs |

### Reopen after deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | LMDB |
| --- | --- | ---: | ---: | ---: |
| `alternating-half` | `reopen-ns` | 78.46 µs | 33.58 µs | 30.79 µs |

### Transactions

#### Read-only

| Keys per transaction | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1,904,856 keys/s | 1,019,643 keys/s | 1,552,848 keys/s | 149,220 keys/s |
| 10 | 3,021,739 keys/s | 1,790,525 keys/s | 2,976,231 keys/s | 929,281 keys/s |
| 100 | 3,426,969 keys/s | 2,058,985 keys/s | 3,332,356 keys/s | 2,169,098 keys/s |
| 1,000 | 3,770,502 keys/s | 1,989,835 keys/s | 3,422,592 keys/s | 2,263,618 keys/s |

#### Durable mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | LMDB | Redis |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 9,531 operations/s | 8,459 operations/s | 7,068 operations/s | 14,113 operations/s |
| 95% | 100 | 96,227 operations/s | 63,083 operations/s | 71,169 operations/s | 147,014 operations/s |
| 95% | 1,000 | 454,188 operations/s | 312,506 operations/s | 425,033 operations/s | 563,732 operations/s |
| 50% | 10 | 11,642 operations/s | 3,222 operations/s | 7,070 operations/s | 14,879 operations/s |
| 50% | 100 | 93,163 operations/s | 25,302 operations/s | 36,427 operations/s | 74,002 operations/s |
| 50% | 1,000 | 439,221 operations/s | 126,897 operations/s | 194,769 operations/s | 381,336 operations/s |

#### No-commit-sync mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | LMDB | Redis |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 1,496,026 operations/s | 642,707 operations/s | 2,625,669 operations/s | 457,253 operations/s |
| 95% | 100 | 1,914,402 operations/s | 1,381,499 operations/s | 2,927,679 operations/s | 1,113,137 operations/s |
| 95% | 1,000 | 2,523,856 operations/s | 1,628,353 operations/s | 2,935,100 operations/s | 1,052,284 operations/s |
| 50% | 10 | 1,034,893 operations/s | 326,913 operations/s | 2,372,359 operations/s | 403,173 operations/s |
| 50% | 100 | 1,061,155 operations/s | 610,028 operations/s | 2,769,002 operations/s | 890,729 operations/s |
| 50% | 1,000 | 1,268,583 operations/s | 837,501 operations/s | 2,558,605 operations/s | 1,408,723 operations/s |

### Enumeration

The 0% case returns no entries. This report omits its entries-per-second value because that rate does not apply.

| Case | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: |
| selectivity=1 | 84,359,809 entries/s | 63,249,755 entries/s | 23,272,587 entries/s | 159,890 entries/s |
| selectivity=10 | 129,917,119 entries/s | 108,825,569 entries/s | 26,129,972 entries/s | 917,339 entries/s |
| selectivity=100 | 220,547,682 entries/s | 99,070,353 entries/s | 26,506,647 entries/s | 1,691,745 entries/s |

### Ordered operations

Redis does not provide the required ordered-cursor API. The 0% range case returns no entries. This report omits its entries-per-second value.

| Case | KVLite | bbolt | LMDB |
| --- | ---: | ---: | ---: |
| full-forward | 232,653,884 entries/s | 103,965,012 entries/s | 26,632,385 entries/s |
| full-reverse | 253,432,084 entries/s | 109,938,729 entries/s | 27,333,369 entries/s |
| range/selectivity=1 | 83,492,479 entries/s | 66,625,699 entries/s | 23,072,102 entries/s |
| range/selectivity=10 | 129,677,698 entries/s | 93,105,481 entries/s | 25,776,086 entries/s |
| range/selectivity=100 | 227,267,479 entries/s | 96,981,459 entries/s | 26,692,476 entries/s |
| seek-and-read=1 | 1,617,597 entries/s | 931,228 entries/s | 1,522,865 entries/s |
| seek-and-read=10 | 13,796,231 entries/s | 9,242,973 entries/s | 9,897,703 entries/s |
| seek-and-read=100 | 56,663,336 entries/s | 58,297,147 entries/s | 22,909,198 entries/s |
| seek-and-read=1000 | 72,789,374 entries/s | 94,569,042 entries/s | 26,506,022 entries/s |

### Access distribution

| Case | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: |
| records=10000/hot-80-20 | 3,979,838 reads/s | 994,063 reads/s | 1,983,623 reads/s | 165,669 reads/s |
| records=10000/uniform | 4,103,135 reads/s | 934,120 reads/s | 1,903,572 reads/s | 163,802 reads/s |
| records=100000/hot-80-20 | 2,860,792 reads/s | 1,285,698 reads/s | 1,557,555 reads/s | 156,821 reads/s |
| records=100000/uniform | 2,321,121 reads/s | 1,151,886 reads/s | 1,317,083 reads/s | 155,517 reads/s |

### Collections

KVLite and bbolt use native buckets. Redis uses logical key prefixes.

#### Durable

| Case | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: |
| write/collections=1/depth=1 | 41,385 keys/s | 34,705 keys/s | — | 39,804 keys/s |
| write/collections=1/depth=3 | 50,566 keys/s | 31,646 keys/s | — | 40,197 keys/s |
| write/collections=100/depth=1 | 45,716 keys/s | 32,814 keys/s | — | 41,129 keys/s |
| write/collections=100/depth=3 | 44,381 keys/s | 30,923 keys/s | — | 40,573 keys/s |
| read/collections=1/depth=1 | 1,884,970 reads/s | 777,882 reads/s | — | 151,519 reads/s |
| read/collections=1/depth=3 | 1,566,283 reads/s | 590,653 reads/s | — | 155,245 reads/s |
| read/collections=100/depth=1 | 1,447,125 reads/s | 758,338 reads/s | — | 155,914 reads/s |
| read/collections=100/depth=3 | 1,203,803 reads/s | 626,001 reads/s | — | 154,129 reads/s |

#### No commit sync

| Case | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: |
| write/collections=1/depth=1 | 1,715,628 keys/s | 1,153,440 keys/s | — | 952,715 keys/s |
| write/collections=1/depth=3 | 1,469,897 keys/s | 1,289,379 keys/s | — | 748,611 keys/s |
| write/collections=100/depth=1 | 2,749,799 keys/s | 1,689,761 keys/s | — | 649,718 keys/s |
| write/collections=100/depth=3 | 2,750,661 keys/s | 2,269,557 keys/s | — | 482,882 keys/s |

### Latency

The large scale reports one, eight, and 32 clients.

#### Durable

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Read | 1 | KVLite | 250 ns | 292 ns | 458 ns | 168.42 µs |
| Read | 1 | bbolt | 417 ns | 709 ns | 1.46 µs | 2.16 ms |
| Read | 1 | LMDB | 500 ns | 583 ns | 750 ns | 417.21 µs |
| Read | 1 | Redis | 5.62 µs | 6.92 µs | 11.79 µs | 4.37 ms |
| Read | 8 | KVLite | 375 ns | 500 ns | 709 ns | 20.99 ms |
| Read | 8 | bbolt | 500 ns | 1.58 µs | 6.38 µs | 21.21 ms |
| Read | 8 | LMDB | 625 ns | 1.12 µs | 1.50 µs | 32.20 ms |
| Read | 8 | Redis | 28.83 µs | 65.17 µs | 101.13 µs | 9.08 ms |
| Read | 32 | KVLite | 375 ns | 541 ns | 709 ns | 59.21 ms |
| Read | 32 | bbolt | 625 ns | 34.67 µs | 159.29 µs | 29.41 ms |
| Read | 32 | LMDB | 625 ns | 1.12 µs | 1.46 µs | 95.01 ms |
| Read | 32 | Redis | 59.75 µs | 240.42 µs | 471.29 µs | 13.11 ms |
| Update | 1 | KVLite | 595.45 µs | 1.63 ms | 3.67 ms | 215.88 ms |
| Update | 1 | bbolt | 1.22 ms | 1.84 ms | 3.24 ms | 67.79 ms |
| Update | 1 | LMDB | 1.17 ms | 1.81 ms | 2.27 ms | 47.96 ms |
| Update | 1 | Redis | 604.10 µs | 2.57 ms | 3.43 ms | 73.52 ms |
| Update | 8 | KVLite | 649.95 µs | 1.12 ms | 2.16 ms | 6.09 ms |
| Update | 8 | bbolt | 1.83 ms | 3.39 ms | 4.74 ms | 9.73 ms |
| Update | 8 | LMDB | 1.21 ms | 8.55 ms | 17.16 ms | 4.40 s |
| Update | 8 | Redis | 659.24 µs | 1.56 ms | 2.26 ms | 5.51 ms |
| Update | 32 | KVLite | 763.50 µs | 1.61 ms | 3.13 ms | 6.71 ms |
| Update | 32 | bbolt | 3.57 ms | 4.30 ms | 5.93 ms | 11.27 ms |
| Update | 32 | LMDB | 1.20 ms | 34.64 ms | 684.46 ms | 9.28 s |
| Update | 32 | Redis | 1.28 ms | 2.90 ms | 4.41 ms | 7.81 ms |
| Mixed | 1 | KVLite | 292 ns | 2.25 µs | 5.33 µs | 5.34 ms |
| Mixed | 1 | bbolt | 708 ns | 562.62 µs | 1.55 ms | 17.78 ms |
| Mixed | 1 | LMDB | 709 ns | 77.42 µs | 1.36 ms | 16.03 ms |
| Mixed | 1 | Redis | 16.71 µs | 722.08 µs | 2.36 ms | 21.66 ms |
| Mixed | 8 | KVLite | 292 ns | 27.42 µs | 626.21 µs | 5.22 ms |
| Mixed | 8 | bbolt | 667 ns | 1.27 ms | 4.88 ms | 17.86 ms |
| Mixed | 8 | LMDB | 792 ns | 878.77 µs | 25.42 ms | 483.70 ms |
| Mixed | 8 | Redis | 124.34 µs | 2.63 ms | 4.17 ms | 29.44 ms |
| Mixed | 32 | KVLite | 292 ns | 207.64 µs | 1.63 ms | 10.95 ms |
| Mixed | 32 | bbolt | 916 ns | 3.13 ms | 32.51 ms | 83.70 ms |
| Mixed | 32 | LMDB | 750 ns | 899.13 µs | 76.60 ms | 991.71 ms |
| Mixed | 32 | Redis | 1.74 ms | 4.10 ms | 5.83 ms | 22.67 ms |

#### No commit sync

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Update | 1 | KVLite | 1.29 µs | 3.38 µs | 5.71 µs | 287.75 µs |
| Update | 1 | bbolt | 8.46 µs | 14.25 µs | 20.83 µs | 4.06 ms |
| Update | 1 | LMDB | 625 ns | 833 ns | 1.67 µs | 12.96 µs |
| Update | 1 | Redis | 7.33 µs | 9.67 µs | 14.79 µs | 66.58 µs |
| Update | 8 | KVLite | 1.46 µs | 3.58 µs | 7.83 µs | 7.52 ms |
| Update | 8 | bbolt | 8.58 µs | 15.79 µs | 3.55 ms | 9.76 ms |
| Update | 8 | LMDB | 584 ns | 1.33 µs | 3.79 µs | 4.40 ms |
| Update | 8 | Redis | 33.38 µs | 68.54 µs | 97.67 µs | 3.23 ms |
| Update | 32 | KVLite | 1.42 µs | 3.88 µs | 2.51 ms | 9.18 ms |
| Update | 32 | bbolt | 8.67 µs | 2.94 ms | 6.79 ms | 10.68 ms |
| Update | 32 | LMDB | 625 ns | 1.46 µs | 9.17 µs | 6.40 ms |
| Update | 32 | Redis | 90.79 µs | 244.34 µs | 409.51 µs | 3.53 ms |
| Mixed | 1 | KVLite | 291 ns | 458 ns | 1.08 µs | 295.98 µs |
| Mixed | 1 | bbolt | 500 ns | 7.54 µs | 10.50 µs | 4.06 ms |
| Mixed | 1 | LMDB | 500 ns | 667 ns | 1.29 µs | 173.42 µs |
| Mixed | 1 | Redis | 5.58 µs | 7.50 µs | 11.79 µs | 3.59 ms |
| Mixed | 8 | KVLite | 291 ns | 459 ns | 1.92 µs | 10.23 ms |
| Mixed | 8 | bbolt | 542 ns | 9.21 µs | 26.25 µs | 15.90 ms |
| Mixed | 8 | LMDB | 583 ns | 1.38 µs | 4.04 µs | 15.30 ms |
| Mixed | 8 | Redis | 30.46 µs | 64.29 µs | 97.17 µs | 6.37 ms |
| Mixed | 32 | KVLite | 291 ns | 459 ns | 2.88 µs | 13.36 ms |
| Mixed | 32 | bbolt | 542 ns | 11.58 µs | 947.97 µs | 20.85 ms |
| Mixed | 32 | LMDB | 583 ns | 1.38 µs | 2.71 µs | 50.61 ms |
| Mixed | 32 | Redis | 64.25 µs | 245.17 µs | 426.34 µs | 7.37 ms |

### Life cycle

Redis does not have the same embedded life-cycle boundary.

| Case | KVLite | bbolt | LMDB |
| --- | ---: | ---: | ---: |
| close-after-writes | 2.38 ms | 9.50 µs | 14.21 µs |
| create-load-close | 49.00 ms | 65.68 ms | 55.52 ms |
| open-clean | 81.29 µs | 14.38 µs | 26.54 µs |
| recover-after-process-kill | 5.06 ms | 16.38 µs | 28.16 µs |

### Persistent bytes

`persistent-B` is the KVLite database plus WAL, the bbolt database, or the Redis AOF. These files have different maintenance and compaction rules.

#### Durable

| Case | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: |
| read/random/hits=0/misses=above/value=128/clients=1 | 1,748,992 B | 4,194,304 B | 1,720,384 B | 1,590,281 B |
| read/random/hits=0/misses=below/value=128/clients=1 | 1,748,992 B | 4,194,304 B | 1,720,384 B | 1,590,281 B |
| read/random/hits=0/misses=between/value=128/clients=1 | 1,748,992 B | 4,194,304 B | 1,720,384 B | 1,590,281 B |
| read/random/hits=50/value=128/clients=1 | 1,748,992 B | 4,194,304 B | 1,720,384 B | 1,590,281 B |
| read/random/hits=100/value=128/clients=8 | 1,748,992 B | 4,194,304 B | 1,720,384 B | 1,590,281 B |
| read/random/hits=100/value=128/clients=32 | 1,748,992 B | 4,194,304 B | 1,720,384 B | 1,590,281 B |
| read/random/key=32768/value=128/clients=1 | 41,984,000 B | 52,056,064 B | — | 16,852,096 B |
| read/random/value=32/clients=1 | 745,472 B | 2,097,152 B | 655,424 B | 620,281 B |
| read/random/value=128/clients=1 | 1,748,992 B | 4,194,304 B | 1,720,384 B | 1,590,281 B |
| read/random/value=1024/clients=1 | 13,950,976 B | 35,549,184 B | 20,684,864 B | 10,560,281 B |
| read/random/value=3072/clients=1 | 41,689,088 B | 58,118,144 B | 41,369,664 B | 31,040,281 B |
| read/random/value=65536/clients=1 | 37,863,424 B | 51,425,280 B | 35,704,896 B | 33,571,456 B |
| read/random/value=1048576/clients=1 | 67,719,168 B | 84,045,824 B | 67,403,840 B | 67,111,231 B |
| read/sequential/hits=100/value=128/clients=1 | 1,748,992 B | 4,194,304 B | 1,720,384 B | 1,590,281 B |
| update/random/grow=32-1024/clients=1 | 17,522,688 B | 4,194,304 B | 2,302,016 B | 1,689,431 B |
| update/random/grow=32-1024/clients=1/batch=100 | 23,246,959 B | 33,640,448 B | 19,202,112 B | 11,182,031 B |
| update/random/grow=128-65536/clients=1 | 36,368,508 B | 50,573,312 B | 35,938,368 B | 65,663,880 B |
| update/random/shrink=1024-32/clients=1 | 30,728,192 B | 35,549,184 B | 20,684,864 B | 10,637,419 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 30,728,192 B | 35,549,184 B | 21,753,920 B | 11,184,019 B |
| update/random/shrink=65536-128/clients=1 | 54,640,640 B | 51,425,280 B | 35,872,832 B | 33,874,620 B |
| update/random/value=32/clients=1 | 17,522,688 B | 2,097,152 B | 655,424 B | 695,431 B |
| update/random/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,720,384 B | 1,762,625 B |
| update/random/value=128/clients=1/batch=10 | 18,526,208 B | 4,194,304 B | 1,810,496 B | 3,195,625 B |
| update/random/value=128/clients=1/batch=100 | 18,526,208 B | 4,194,304 B | 2,486,336 B | 3,182,225 B |
| update/random/value=128/clients=1/batch=1000 | 18,526,208 B | 8,388,608 B | 4,841,536 B | 3,180,795 B |
| update/random/value=128/clients=8 | 18,526,208 B | 4,194,304 B | 1,720,384 B | 2,141,025 B |
| update/random/value=128/clients=8/batch=100 | 18,526,208 B | 4,194,304 B | 2,486,336 B | 3,182,225 B |
| update/random/value=128/clients=32 | 18,526,208 B | 4,194,304 B | 1,720,384 B | 2,141,025 B |
| update/random/value=128/clients=32/batch=100 | 18,526,208 B | 4,194,304 B | 2,490,432 B | 3,182,225 B |
| update/random/value=1024/clients=1 | 30,728,192 B | 35,549,184 B | 20,684,864 B | 11,631,419 B |
| update/random/value=3072/clients=1 | 58,466,304 B | 58,118,144 B | 41,377,856 B | 34,163,515 B |
| update/random/value=65536/clients=1 | 41,539,284 B | 51,425,280 B | 35,942,464 B | 99,284,620 B |
| update/sequential/value=128/clients=1/batch=10 | 18,526,208 B | 4,194,304 B | 1,724,480 B | 3,195,625 B |
| update/sequential/value=128/clients=1/batch=100 | 18,526,208 B | 4,194,304 B | 1,744,960 B | 3,182,225 B |
| update/sequential/value=128/clients=1/batch=1000 | 18,526,208 B | 4,194,304 B | 2,043,968 B | 3,180,795 B |
| insert/random/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,953,856 B | 1,762,625 B |
| insert/random/value=128/clients=1/batch=10 | 18,526,208 B | 8,388,608 B | 4,128,832 B | 3,195,625 B |
| insert/random/value=128/clients=1/batch=100 | 18,526,208 B | 8,388,608 B | 4,767,808 B | 3,182,225 B |
| insert/random/value=128/clients=1/batch=1000 | 18,526,208 B | 8,388,608 B | 7,360,576 B | 3,180,795 B |
| insert/random/value=128/clients=8 | 18,526,208 B | 8,388,608 B | 2,469,952 B | 2,141,025 B |
| insert/random/value=128/clients=8/batch=100 | 18,526,208 B | 8,388,608 B | 4,784,192 B | 3,182,225 B |
| insert/random/value=128/clients=32 | 18,526,208 B | 8,388,608 B | 2,465,856 B | 2,141,025 B |
| insert/random/value=128/clients=32/batch=100 | 18,526,208 B | 8,388,608 B | 4,796,480 B | 3,182,225 B |
| insert/random/value=65536/clients=1 | 71,774,208 B | 85,176,320 B | 71,594,048 B | 99,284,620 B |
| insert/sequential/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,884,224 B | 1,762,625 B |
| insert/sequential/value=128/clients=1/batch=10 | 18,526,208 B | 8,388,608 B | 3,371,072 B | 3,195,625 B |
| insert/sequential/value=128/clients=1/batch=100 | 18,526,208 B | 8,388,608 B | 3,371,072 B | 3,182,225 B |
| insert/sequential/value=128/clients=1/batch=1000 | 18,526,208 B | 8,388,608 B | 3,371,072 B | 3,180,795 B |
| mixed/read=50/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,720,384 B | 2,450,625 B |
| mixed/read=50/value=128/clients=8 | 18,526,208 B | 4,194,304 B | 1,720,384 B | 2,450,625 B |
| mixed/read=50/value=128/clients=32 | 18,526,208 B | 4,194,304 B | 1,720,384 B | 2,450,625 B |
| mixed/read=95/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,720,384 B | 1,676,625 B |
| mixed/read=95/value=128/clients=8 | 18,526,208 B | 4,194,304 B | 1,720,384 B | 1,676,625 B |
| mixed/read=95/value=128/clients=32 | 18,526,208 B | 4,194,304 B | 1,720,384 B | 1,676,625 B |
| mixed/read=95/value=65536/clients=1 | 41,342,364 B | 51,425,280 B | 35,942,464 B | 50,098,120 B |

#### No commit sync

| Case | KVLite | bbolt | LMDB | Redis |
| --- | ---: | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 17,522,688 B | 2,469,888 B | 1,073,750,080 B | 1,689,431 B |
| update/random/grow=32-1024/clients=1/batch=100 | 23,246,959 B | 19,435,520 B | 1,073,750,080 B | 11,182,031 B |
| update/random/grow=128-65536/clients=1 | 36,328,995 B | 38,326,272 B | 1,073,750,080 B | 65,663,880 B |
| update/random/shrink=1024-32/clients=1 | 30,728,192 B | 35,549,184 B | 1,073,750,080 B | 10,637,419 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 30,728,192 B | 35,549,184 B | 1,073,750,080 B | 11,184,019 B |
| update/random/shrink=65536-128/clients=1 | 54,640,640 B | 51,425,280 B | 1,073,750,080 B | 33,874,620 B |
| update/random/value=32/clients=1 | 17,522,688 B | 2,097,152 B | 1,073,750,080 B | 695,431 B |
| update/random/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,762,625 B |
| update/random/value=128/clients=1/batch=10 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 3,195,625 B |
| update/random/value=128/clients=1/batch=100 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 3,182,225 B |
| update/random/value=128/clients=1/batch=1000 | 18,526,208 B | 6,082,560 B | 1,073,750,080 B | 3,180,795 B |
| update/random/value=128/clients=8 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 2,141,025 B |
| update/random/value=128/clients=8/batch=100 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 3,182,225 B |
| update/random/value=128/clients=32 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 2,141,025 B |
| update/random/value=128/clients=32/batch=100 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 3,182,225 B |
| update/random/value=1024/clients=1 | 30,728,192 B | 35,549,184 B | 1,073,750,080 B | 11,631,419 B |
| update/random/value=3072/clients=1 | 58,466,304 B | 58,118,144 B | 1,073,750,080 B | 34,163,515 B |
| update/random/value=65536/clients=1 | 38,454,204 B | 51,425,280 B | 1,073,750,080 B | 99,284,620 B |
| update/sequential/value=128/clients=1/batch=10 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 3,195,625 B |
| update/sequential/value=128/clients=1/batch=100 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 3,182,225 B |
| update/sequential/value=128/clients=1/batch=1000 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 3,180,795 B |
| insert/random/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,762,625 B |
| insert/random/value=128/clients=1/batch=10 | 18,526,208 B | 5,910,528 B | 1,073,750,080 B | 3,195,625 B |
| insert/random/value=128/clients=1/batch=100 | 18,526,208 B | 6,303,744 B | 1,073,750,080 B | 3,182,225 B |
| insert/random/value=128/clients=1/batch=1000 | 18,526,208 B | 7,663,616 B | 1,073,750,080 B | 3,180,795 B |
| insert/random/value=128/clients=8 | 18,526,208 B | 4,268,032 B | 1,073,750,080 B | 2,141,025 B |
| insert/random/value=128/clients=8/batch=100 | 18,526,208 B | 6,283,264 B | 1,073,750,080 B | 3,182,225 B |
| insert/random/value=128/clients=32 | 18,526,208 B | 4,268,032 B | 1,073,750,080 B | 2,141,025 B |
| insert/random/value=128/clients=32/batch=100 | 18,526,208 B | 6,270,976 B | 1,073,750,080 B | 3,182,225 B |
| insert/random/value=65536/clients=1 | 71,734,271 B | 73,461,760 B | 1,073,750,080 B | 99,284,620 B |
| insert/sequential/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,762,625 B |
| insert/sequential/value=128/clients=1/batch=10 | 18,526,208 B | 6,971,392 B | 1,073,750,080 B | 3,195,625 B |
| insert/sequential/value=128/clients=1/batch=100 | 18,526,208 B | 6,971,392 B | 1,073,750,080 B | 3,182,225 B |
| insert/sequential/value=128/clients=1/batch=1000 | 18,526,208 B | 6,971,392 B | 1,073,750,080 B | 3,180,795 B |
| mixed/read=50/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 2,450,625 B |
| mixed/read=50/value=128/clients=8 | 18,526,208 B | 4,378,624 B | 1,073,750,080 B | 2,450,625 B |
| mixed/read=50/value=128/clients=32 | 18,526,208 B | 4,853,760 B | 1,073,750,080 B | 2,450,625 B |
| mixed/read=95/value=128/clients=1 | 18,526,208 B | 4,194,304 B | 1,073,750,080 B | 1,676,625 B |
| mixed/read=95/value=128/clients=8 | 18,526,208 B | 4,390,912 B | 1,073,750,080 B | 1,676,625 B |
| mixed/read=95/value=128/clients=32 | 18,526,208 B | 4,780,032 B | 1,073,750,080 B | 1,676,625 B |
| mixed/read=95/value=65536/clients=1 | 39,110,604 B | 51,425,280 B | 1,073,750,080 B | 50,098,120 B |
