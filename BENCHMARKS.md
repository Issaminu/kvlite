# KVLite benchmarks

This report compares KVLite with bbolt and Redis at the public call boundary. It contains the median of 15 measured runs at the complete `large --workloads=all --storage=volume` scale. The same run then profiled every KVLite case.

| Item | Value |
| --- | --- |
| Run time | `2026-10-21T01:10:07Z` |
| Timing duration | 2 hours 25 minutes 29 seconds |
| Profile capture | 1 hour 4 minutes 40 seconds, KVLite cases only |
| KVLite | `21969938edfef3fa74d1e1920cc4e9d297da415c`; the only untracked file was a Markdown note outside the build |
| bbolt | `v1.4.3` |
| Redis | `8.8.0` |
| Platform | Linux `arm64`, four CPUs in containers |
| Scale | `large --workloads=all --storage=volume --profile-engines=kvlite` |
| Warm-up | One complete unrecorded round |
| Measurement | 15 measured rounds |

## Results at a glance

A higher throughput value is better. A lower latency or life-cycle value is better. This section shows cases where KVLite had the best median.

| Category | Mode and workload | KVLite | bbolt | Redis | Highlight |
| --- | --- | ---: | ---: | ---: | --- |
| Point read | Read-only, random, 128-byte value, eight clients | 5,892,288 keys/s | 942,708 keys/s | 239,126 keys/s | **KVLite: 6.25x bbolt; 24.64x Redis** |
| Point-read p99 | Read-only, eight clients | 1.00 µs | 5.71 µs | 100.42 µs | **KVLite: 5.71x advantage over bbolt; 100.42x over Redis** |
| Point update | Durable, random, 128-byte value, one client, batch of 100 | 107,962 keys/s | 22,566 keys/s | 51,327 keys/s | **KVLite: 4.78x bbolt; 2.10x Redis** |
| Point insert | Durable, random, 128-byte value, one client, batch of 100 | 95,009 keys/s | 18,065 keys/s | 47,223 keys/s | **KVLite: 5.26x bbolt; 2.01x Redis** |
| Mixed work | Durable, 95% reads, eight clients | 96,657 operations/s | 8,584 operations/s | 13,956 operations/s | **KVLite: 11.26x bbolt; 6.93x Redis** |
| Mixed work | No commit sync, 95% reads, eight clients | 1,835,915 operations/s | 363,121 operations/s | 226,411 operations/s | **KVLite: 5.06x bbolt; 8.11x Redis** |
| Key delete | No commit sync, random, one client | 385,900 keys/s | 77,531 keys/s | 107,542 keys/s | **KVLite: 4.98x bbolt; 3.59x Redis** |
| Large-key read | Read-only, random, 32 KiB key, one client | 75,805 keys/s | 64,127 keys/s | 37,584 keys/s | **KVLite: 1.18x bbolt; 2.02x Redis** |
| Clean open | Life cycle | 91.55 µs | 2.54 ms | Not comparable | **KVLite: 27.69x advantage over bbolt** |

These ratios apply only to the named workloads and public call boundaries. KVLite did not lead every case. bbolt read 64 KiB values 1.31x faster and 1 MiB values 1.65x faster. Redis led seven of 13 no-commit-sync insert cases and seven of 13 durable insert cases. The full tables show every case.

## Scope

This run measured every workload group and every case variant at the large scale. It covers point operations, deletion, transactions, enumeration, ordered operations, access distributions, latency, collections, storage size, and embedded life-cycle operations.

The run includes large-entry cases. These cases use 64 KiB values, 1 MiB values, or 32 KiB keys. A large entry needs continuation pages in KVLite.

## Fairness and API boundaries

The runner gave each engine the same fixed operation schedule and data set. It ran the engines serially. It changed the engine order and durability-mode order between measured rounds. Each engine used the same four-CPU set and Docker storage class.

| Logical operation | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| Point read | `DB.Get` | One `DB.View` transaction | `GET` |
| Grouped read | One read transaction | One read transaction | `MGET` |
| Point write, one client | `DB.Put` | `DB.Update` | `SET` |
| Point write, many clients | `DB.Put` | Durable: `DB.Batch`; no commit sync: `DB.Update` | `SET` |
| Batch write | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Mixed transaction | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Key delete | `DB.Delete` | Durable, many clients: `DB.Batch`; other cases: `DB.Update` | `DEL` |
| Batch delete | `Bucket.DeleteBatch` in one write transaction | One write transaction | One variadic `DEL` |
| Prefix enumeration | Ordered prefix scan | Ordered prefix cursor | `SCAN` and `MGET` |
| Full reverse scan | `Bucket.ScanReverse` | Cursor `Last` and `Prev` | No result |

bbolt does not provide a direct `DB.Get` call. Use the grouped read results when one transaction must contain several reads. Both KVLite and the bbolt adapter return a value copy. Redis includes its client, server, and local network path. These are public call comparisons. They do not compare identical internal operations.

## Durability modes

| Mode | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| Durable | `SyncFull` | `NoSync=false`; `NoGrowSync=false`; `NoFreelistSync=false` | AOF with `appendfsync always` |
| No commit sync | `SyncNone` | `NoSync=true`; `NoGrowSync=true`; `NoFreelistSync=false` | AOF with `appendfsync no` |

A successful durable write waits for the durability boundary of its engine. In no-commit-sync mode, no engine asks storage to sync during commit. Durable and no-commit-sync results have different guarantees. This report keeps them separate.

The harness opens KVLite with the default `DataPageChecksums=false`. KVLite therefore does not calculate or check checksums for leaf, branch, and continuation pages in this run. Metadata pages, allocation pages, and WAL records keep their checksums. With data-page checksums on, KVLite point reads and scans are 2.5 to 4.9 times slower than with the default.

## Test environment

- CPU set: `0-3`
- Go: `go1.26.7 linux/arm64`
- KVLite: `21969938edfef3fa74d1e1920cc4e9d297da415c`
- bbolt: `v1.4.3`
- Redis client: `go-redis v9.22.0`
- Redis server: `8.8.0`, `jemalloc-5.3.0`
- Container system: OrbStack with Linux kernel `7.2.9-orbstack-00391-g2b298d05fbd4`
- Storage driver: `overlay2`
- Host: Apple M4 Max; 14 CPUs visible to the container system
- CPUs assigned to each measured process and its Redis server: four
- Go container memory limit: 12,613,708,800 bytes
- Benchmark data storage: temporary Docker volume

The Docker volume path includes the OrbStack virtual machine and the host storage path. It does not prove bare-metal device performance. Other containers were active on the host. CPU assignment reduced direct CPU competition. Other host work could still affect the results.

## Method

The runner first completed its correctness tests. It then completed one unrecorded warm-up round. It created a new database fixture for each benchmark process. It did not reuse a measured database from the warm-up round.

The runner recorded 15 fixed-work rounds. It changed the engine order and durability-mode order between rounds. Pure reads ran once per round because commit-sync settings do not affect them.

The large scale uses 10,000 records for its main cases. It uses 100,000 point reads, 1,000 single-client point writes, 3,200 concurrent point writes, and 10,000 mixed operations. Large-entry cases use 512 records, 5,000 reads, and 1,000 writes. The 1 MiB value case uses 64 records and 1,000 reads. Read-latency cases use 1,000,000 recorded operations and 10,000 unrecorded fixture warm-up reads. Update-latency cases use 10,000 operations. Mixed-latency cases use 100,000 operations.

Setup, fixture loading, final stored-data checks, and close operations were outside the core timers. Before a timed write workload in the acknowledged-operation, focused-write, and key-deletion cases, the harness makes one untimed commit on the new handle. The named life-cycle cases measure open, close, and recovery work separately. The runner ran each life-cycle case in its own process. Before the timed recovery open, the harness waits until the killed helper process releases its file lock.

After the 15 timed rounds, the runner profiled every KVLite case again with 15 repeats. It recorded CPU profiles, allocation profiles, and execution traces. The profile pass does not change the timing values in this report. KVLite rows also report WAL commits, WAL bytes, write calls, and storage bytes written per operation.

## Changes since the previous report

The previous report measured `689a4dc` on 2026-10-17. This run measures 20 library commits after it. Three commits make reads and WAL encoding faster (`7a2fb49`, `4d293f4`, `b57c899`). One commit adds a file-system interface for simulated storage (`568ef83`). Most other commits fix crash, recovery, and storage-failure behavior that the Blackout simulation found, or support that simulation.

The harness also changed. Commit `d1d7f67` adds the untimed warm-up commit that the Method section describes. Before this change, a timed write workload with few commits also measured the first-commit work of a new handle. The kernel and the container system did not change.

The raw samples of the previous run were not kept. The table below therefore compares medians, without significance tests. A ratio below 1 means a lower time. Each ratio is the geometric mean of the main metric of each case in the group.

| Workload group | KVLite time ratio | bbolt time ratio | Redis time ratio |
| --- | ---: | ---: | ---: |
| Point reads | 0.886 | 1.016 | 1.045 |
| Read transactions | 0.973 | 1.000 | 0.989 |
| Access-distribution reads | 0.974 | 1.013 | 1.020 |
| Ordered operations | 1.021 | 1.005 | No result |
| No-commit-sync point updates | 1.022 | 1.023 | 0.987 |
| No-commit-sync point inserts | 1.090 | 0.994 | 1.008 |
| No-commit-sync mixed work | 1.140 | 0.950 | 1.031 |
| Durable point updates | 1.074 | 1.046 | 0.808 |
| Durable point inserts | 1.108 | 1.180 | 0.777 |
| Durable mixed work | 1.291 | 0.838 | 0.917 |
| Durable mixed transactions | 1.090 | 1.219 | 0.741 |
| Life cycle | 1.209 | 0.925 | No result |
| All 197 time and rate rows | 1.060 | 0.999 | 0.937 |

Redis times went down by 19% to 26% in the durable point-update, point-insert, and mixed-transaction groups, but the Redis server and its settings did not change. So the host was not equal between the two runs, and the median ratios mix code changes with host changes. KVLite led 17 fewer durable cases than in the previous report, and Redis led 18 more.

To separate code from host, paired runs measured the library of `689a4dc` and the library of `fafae4c` in one container. `fafae4c` has the same library code as the measured commit. Both builds used the current harness. Ten interleaved rounds covered every KVLite case. A second copy of the `fafae4c` build ran as a control, so the noise of each case is known. Focused runs with 14 to 30 rounds then checked each case that looked different, and bisected over the commits.

- **Reads are faster.** In the paired runs, single-client point reads of small values are 8% to 10% faster (p ≤ 0.008 in three of four cases), and absent-key reads 11% to 13% faster (p ≤ 0.0005). In paired micro benchmarks, a point read is 7% to 10% faster, an absent-key read 9% to 10% faster, and 100 reads in one transaction 6% to 8% faster. Commit `7a2fb49` passes the 56-byte result entry by pointer in the lookup functions, so the compiler no longer copies it through the stack at each return. `lookupNode` goes from 392 to 264 machine instructions.
- **The WAL format did not change.** WAL bytes per operation are within 1% in every case. Multi-client cases show small differences in commits per operation, because group-commit batches depend on timing.
- **The recovery open after a killed process is 1.89 times slower.** It went from 3.53 ms to 6.68 ms in the paired runs (p < 0.001) and from 2.91 ms to 4.99 ms in the full runs. Two durability fixes cause it. Commit `3358ef6` writes the recovered WAL records again during open, so a later failed sync cannot hide lost data. It added 39% (p = 0.017). Commit `fafae4c` syncs the cleared WAL before open returns, so a power failure cannot bring back records that recovery did not apply. It added 34% in a focused test with 24 rounds (p < 0.001). A test that counts file operations shows one more WAL write and one more WAL sync per recovery open, and the rewrite makes the existing WAL sync flush real data. Both commits add these flushes to a rare path.
- **Creating, loading, and closing a database is 16% slower** (p = 0.015). The bisection shows small steps at commits `0c09824`, `f028de9`, and `73a88c5`, and each step alone is within the noise. A new database now starts with a WAL commit and a checkpoint, and each new WAL generation syncs its header before its first record. The counting test shows that creating a database and making its first commit now take six syncs instead of three, including the directory sync. Since `0c09824`, the first commit of every new handle also takes one more WAL sync. The full runs show 45.19 ms before and 48.54 ms now.
- **Open and close are 3.2% slower in a micro benchmark** (20.45 µs before, p = 0.015). Commit `fafae4c` first tries to open an existing WAL, and then creates it. A clean open therefore makes one more failed open call. The KVBench open cases cannot resolve this difference. On the benchmark volume, the failed open call costs about 0.28 µs, and its error adds two allocations.
- **The forward full scan is 7.3% slower, but the scan code does not cause it.** The only scan change, the flag check of `0f883ba`, made this case 1.6% faster. The time went up at commits that do not touch the scan path: by 3.9% at the harness commit `d1d7f67` and by 9.9% at the open commit `fafae4c`. Between `9a12749` and `fafae4c`, the machine code of the scan functions is the same after address masking. A range scan over all entries calls the same functions and moved by at most 3%. This is a code-placement effect.

The paired runs did not confirm the other cases that looked slower in the median table. Durable multi-client inserts, updates, and mixed work had paired ratios from 0.68 to 1.30, and none was significant (p > 0.05). The durable update latency maximum went up because some rounds had sync stalls of 240 ms to 527 ms. bbolt had stalls of the same size in other rounds. Reopen after deletion (24 paired rounds), key deletion (20 rounds), and 64 KiB and 1 MiB value reads (20 rounds) did not change.

## Interpretation

These statements apply only to this run and its tested workloads.

- KVLite led 75 of 118 durable cases. bbolt led eight. Redis led 35.
- KVLite led 44 of 71 no-commit-sync cases. bbolt led six. Redis led 21.
- Each count uses one main metric for each case: the median time per operation, or p99 for latency cases. It excludes storage-size metrics.
- KVLite led 40 of 43 read cases, including point reads, read transactions, scans, ordered reads, access distributions, collection reads, and read latency. bbolt led 64 KiB value reads, 1 MiB value reads, and `seek-and-read=1000`.
- KVLite led 10 of 21 durable point-update cases and six of 13 durable point-insert cases. Redis led the other cases. In 14 of these 18 Redis cases, KVLite was within 17% of Redis.
- KVLite led 13 of 21 no-commit-sync point-update cases and five of 13 no-commit-sync point-insert cases. Redis led most of the other cases.
- KVLite led acknowledged mixed work in five of seven durable cases and six of seven no-commit-sync cases. bbolt led the cases with 64 KiB values. Redis led the durable case with 50% reads and one client.
- KVLite led all six no-commit-sync mixed-transaction cases. Redis led five of six durable mixed-transaction cases.
- KVLite led 13 of 28 deletion, reuse, and reopen cases across both modes. Redis led 14. bbolt led one.
- KVLite opened a clean database 27.69x faster than bbolt. The recovery open after a killed process took 4.99 ms for KVLite and 2.92 ms for bbolt. bbolt closed after writes 200x faster, because KVLite checkpoints its WAL during close.

## Limits

- This is one run on one container host. It is not a bare-metal Linux result.
- Docker volume storage includes the OrbStack virtual machine and host storage path. It does not isolate physical-device sync latency.
- Durable write times depend on storage sync latency. That latency changed by more than four times between rounds of the same binary.
- The raw samples of the previous run were not kept. The comparison with that run uses medians only. The paired runs are the evidence for code changes.
- Each timed run opens a new handle. The acknowledged-operation, focused-write, and key-deletion cases start their timers after one commit. Other cases with timed writes, such as mixed transactions, latency, collections, page reuse, and tail reclamation, still include the first commit of the handle. That commit syncs the WAL header and the directory.
- Read tests use a warm operating-system cache. The suite does not claim cold-cache performance.
- KVLite ran with the default `DataPageChecksums=false`. With data-page checksums on, KVLite point reads and scans are much slower.
- The suite does not rank combined CPU use or peak resident memory across embedded and client-server designs.
- Redis has no result for ordered cursor or embedded life-cycle operations. A Redis profile covers its Go client process, not the Redis server.
- KVLite performs a final checkpoint during close. The close-after-writes case does not compare the same work across the embedded engines.
- Persistent byte counts cover different file designs and maintenance rules. They are not a direct storage-efficiency ranking.

## Reproduce

```sh
cd benchmarks/kvbench
./run-docker.sh large --workloads=all --storage=volume --profile-engines=kvlite
```

The runner uses pinned Go and Redis container images. The Go module pins bbolt and the Redis client. Omit `--profile-engines` for a timing-only run.

## Full results

Every value is the median of 15 measured runs.

### Acknowledged operations

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| read/random/hits=0/misses=above/value=128/clients=1 | 8,257,236 keys/s | 1,677,808 keys/s | 142,091 keys/s |
| read/random/hits=0/misses=below/value=128/clients=1 | 8,192,203 keys/s | 1,853,854 keys/s | 143,241 keys/s |
| read/random/hits=0/misses=between/value=128/clients=1 | 6,753,285 keys/s | 1,760,228 keys/s | 142,004 keys/s |
| read/random/hits=50/value=128/clients=1 | 4,826,508 keys/s | 1,522,881 keys/s | 150,691 keys/s |
| read/random/hits=100/value=128/clients=8 | 5,892,288 keys/s | 942,708 keys/s | 239,126 keys/s |
| read/random/hits=100/value=128/clients=32 | 4,322,788 keys/s | 892,717 keys/s | 308,659 keys/s |
| read/random/key=32768/value=128/clients=1 | 75,805 keys/s | 64,127 keys/s | 37,584 keys/s |
| read/random/value=32/clients=1 | 4,284,665 keys/s | 1,369,643 keys/s | 169,917 keys/s |
| read/random/value=128/clients=1 | 3,904,999 keys/s | 1,341,297 keys/s | 164,750 keys/s |
| read/random/value=1024/clients=1 | 1,940,663 keys/s | 1,349,848 keys/s | 154,812 keys/s |
| read/random/value=3072/clients=1 | 1,580,916 keys/s | 1,082,009 keys/s | 127,709 keys/s |
| read/random/value=65536/clients=1 | 178,123 keys/s | 232,680 keys/s | 48,176 keys/s |
| read/random/value=1048576/clients=1 | 16,601 keys/s | 27,334 keys/s | 6,200 keys/s |
| read/sequential/hits=100/value=128/clients=1 | 5,468,777 keys/s | 1,437,837 keys/s | 160,055 keys/s |
| update/random/grow=32-1024/clients=1 | 1,551 keys/s | 705 keys/s | 1,651 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 30,658 keys/s | 13,722 keys/s | 38,005 keys/s |
| update/random/grow=128-65536/clients=1 | 1,211 keys/s | 806 keys/s | 1,306 keys/s |
| update/random/shrink=1024-32/clients=1 | 1,889 keys/s | 931 keys/s | 2,075 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 45,007 keys/s | 13,025 keys/s | 50,228 keys/s |
| update/random/shrink=65536-128/clients=1 | 1,866 keys/s | 912 keys/s | 1,902 keys/s |
| update/random/value=32/clients=1 | 1,734 keys/s | 843 keys/s | 1,834 keys/s |
| update/random/value=128/clients=1 | 1,806 keys/s | 941 keys/s | 1,748 keys/s |
| update/random/value=128/clients=1/batch=10 | 17,080 keys/s | 5,433 keys/s | 16,718 keys/s |
| update/random/value=128/clients=1/batch=100 | 107,962 keys/s | 22,566 keys/s | 51,327 keys/s |
| update/random/value=128/clients=1/batch=1000 | 397,354 keys/s | 110,563 keys/s | 312,346 keys/s |
| update/random/value=128/clients=8 | 10,587 keys/s | 395 keys/s | 11,546 keys/s |
| update/random/value=128/clients=8/batch=100 | 249,774 keys/s | 20,965 keys/s | 196,830 keys/s |
| update/random/value=128/clients=32 | 33,161 keys/s | 1,381 keys/s | 31,120 keys/s |
| update/random/value=128/clients=32/batch=100 | 460,044 keys/s | 19,386 keys/s | 433,735 keys/s |
| update/random/value=1024/clients=1 | 1,785 keys/s | 910 keys/s | 1,739 keys/s |
| update/random/value=3072/clients=1 | 1,282 keys/s | 860 keys/s | 1,627 keys/s |
| update/random/value=65536/clients=1 | 796 keys/s | 874 keys/s | 1,218 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 16,121 keys/s | 8,195 keys/s | 16,271 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 115,231 keys/s | 58,939 keys/s | 60,356 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 647,310 keys/s | 323,479 keys/s | 369,874 keys/s |
| insert/random/value=128/clients=1 | 1,937 keys/s | 942 keys/s | 2,033 keys/s |
| insert/random/value=128/clients=1/batch=10 | 15,009 keys/s | 3,532 keys/s | 17,463 keys/s |
| insert/random/value=128/clients=1/batch=100 | 95,009 keys/s | 18,065 keys/s | 47,223 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 399,567 keys/s | 216,468 keys/s | 386,721 keys/s |
| insert/random/value=128/clients=8 | 11,655 keys/s | 406 keys/s | 9,099 keys/s |
| insert/random/value=128/clients=8/batch=100 | 231,516 keys/s | 16,949 keys/s | 227,163 keys/s |
| insert/random/value=128/clients=32 | 27,561 keys/s | 1,452 keys/s | 31,194 keys/s |
| insert/random/value=128/clients=32/batch=100 | 428,275 keys/s | 19,087 keys/s | 478,862 keys/s |
| insert/random/value=65536/clients=1 | 865 keys/s | 833 keys/s | 1,354 keys/s |
| insert/sequential/value=128/clients=1 | 1,912 keys/s | 943 keys/s | 1,962 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 14,991 keys/s | 7,736 keys/s | 17,553 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 114,371 keys/s | 67,259 keys/s | 50,318 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 620,252 keys/s | 293,659 keys/s | 355,644 keys/s |
| mixed/read=50/value=128/clients=1 | 3,256 operations/s | 1,750 operations/s | 3,466 operations/s |
| mixed/read=50/value=128/clients=8 | 18,991 operations/s | 791 operations/s | 12,271 operations/s |
| mixed/read=50/value=128/clients=32 | 47,953 operations/s | 2,796 operations/s | 33,187 operations/s |
| mixed/read=95/value=128/clients=1 | 31,719 operations/s | 14,148 operations/s | 10,278 operations/s |
| mixed/read=95/value=128/clients=8 | 96,657 operations/s | 8,584 operations/s | 13,956 operations/s |
| mixed/read=95/value=128/clients=32 | 246,991 operations/s | 27,161 operations/s | 20,319 operations/s |
| mixed/read=95/value=65536/clients=1 | 11,637 operations/s | 14,735 operations/s | 7,642 operations/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 199,634 keys/s | 88,097 keys/s | 106,633 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 268,251 keys/s | 245,391 keys/s | 679,034 keys/s |
| update/random/grow=128-65536/clients=1 | 28,359 keys/s | 28,413 keys/s | 28,883 keys/s |
| update/random/shrink=1024-32/clients=1 | 378,080 keys/s | 87,382 keys/s | 93,263 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 603,841 keys/s | 288,593 keys/s | 1,078,828 keys/s |
| update/random/shrink=65536-128/clients=1 | 143,767 keys/s | 48,098 keys/s | 126,341 keys/s |
| update/random/value=32/clients=1 | 402,877 keys/s | 61,613 keys/s | 113,204 keys/s |
| update/random/value=128/clients=1 | 315,667 keys/s | 84,896 keys/s | 94,887 keys/s |
| update/random/value=128/clients=1/batch=10 | 829,210 keys/s | 194,482 keys/s | 534,930 keys/s |
| update/random/value=128/clients=1/batch=100 | 983,485 keys/s | 429,232 keys/s | 1,255,235 keys/s |
| update/random/value=128/clients=1/batch=1000 | 1,228,717 keys/s | 555,735 keys/s | 1,378,480 keys/s |
| update/random/value=128/clients=8 | 283,724 keys/s | 56,203 keys/s | 176,170 keys/s |
| update/random/value=128/clients=8/batch=100 | 698,736 keys/s | 312,685 keys/s | 1,141,665 keys/s |
| update/random/value=128/clients=32 | 275,872 keys/s | 48,577 keys/s | 197,714 keys/s |
| update/random/value=128/clients=32/batch=100 | 707,746 keys/s | 385,617 keys/s | 1,279,234 keys/s |
| update/random/value=1024/clients=1 | 276,446 keys/s | 89,394 keys/s | 92,777 keys/s |
| update/random/value=3072/clients=1 | 223,637 keys/s | 82,477 keys/s | 84,148 keys/s |
| update/random/value=65536/clients=1 | 25,669 keys/s | 36,445 keys/s | 33,281 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 1,975,042 keys/s | 609,675 keys/s | 523,100 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 3,218,735 keys/s | 1,897,665 keys/s | 1,299,588 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 3,536,699 keys/s | 2,706,668 keys/s | 1,359,751 keys/s |
| insert/random/value=128/clients=1 | 472,928 keys/s | 66,028 keys/s | 97,163 keys/s |
| insert/random/value=128/clients=1/batch=10 | 403,826 keys/s | 164,999 keys/s | 463,394 keys/s |
| insert/random/value=128/clients=1/batch=100 | 579,313 keys/s | 314,425 keys/s | 900,164 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 1,051,181 keys/s | 819,185 keys/s | 955,581 keys/s |
| insert/random/value=128/clients=8 | 157,029 keys/s | 41,363 keys/s | 186,833 keys/s |
| insert/random/value=128/clients=8/batch=100 | 333,282 keys/s | 224,439 keys/s | 996,054 keys/s |
| insert/random/value=128/clients=32 | 176,765 keys/s | 36,297 keys/s | 216,590 keys/s |
| insert/random/value=128/clients=32/batch=100 | 406,911 keys/s | 300,152 keys/s | 1,104,118 keys/s |
| insert/random/value=65536/clients=1 | 23,705 keys/s | 24,305 keys/s | 31,502 keys/s |
| insert/sequential/value=128/clients=1 | 453,698 keys/s | 71,852 keys/s | 108,348 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 794,574 keys/s | 420,271 keys/s | 458,782 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 1,767,280 keys/s | 1,849,334 keys/s | 854,554 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 3,666,250 keys/s | 2,594,023 keys/s | 993,050 keys/s |
| mixed/read=50/value=128/clients=1 | 739,015 operations/s | 163,248 operations/s | 137,322 operations/s |
| mixed/read=50/value=128/clients=8 | 512,203 operations/s | 85,305 operations/s | 218,433 operations/s |
| mixed/read=50/value=128/clients=32 | 459,866 operations/s | 68,051 operations/s | 239,468 operations/s |
| mixed/read=95/value=128/clients=1 | 2,200,984 operations/s | 646,899 operations/s | 155,451 operations/s |
| mixed/read=95/value=128/clients=8 | 1,835,915 operations/s | 363,121 operations/s | 226,411 operations/s |
| mixed/read=95/value=128/clients=32 | 1,126,682 operations/s | 376,979 operations/s | 248,503 operations/s |
| mixed/read=95/value=65536/clients=1 | 162,413 operations/s | 202,205 operations/s | 46,958 operations/s |

### Key deletion: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 1,615 ack-keys/s | 666 ack-keys/s | 1,615 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 6,574,889 B | 4,194,304 B | 2,080,573 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 95,383 ack-keys/s | 38,968 ack-keys/s | 133,092 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 6,199,461 B | 4,194,304 B | 1,952,073 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 560,027 ack-keys/s | 239,392 ack-keys/s | 766,084 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,779,016 B | 8,388,608 B | 1,950,733 B |
| `existing/random/clients=8` | `ack-keys/s` | 10,362 ack-keys/s | 403 ack-keys/s | 10,236 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 6,340,705 B | 4,194,304 B | 2,080,573 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 956 ack-keys/s | 816 ack-keys/s | 1,564 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 39,244,474 B | 51,425,280 B | 33,652,922 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 1,659 ack-keys/s | 650 ack-keys/s | 1,299 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 5,835,030 B | 4,194,304 B | 2,080,573 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 128,351 ack-keys/s | 87,212 ack-keys/s | 88,347 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 3,741,426 B | 4,194,304 B | 1,952,073 B |
| `missing/random/clients=1` | `ack-keys/s` | 376,709 ack-keys/s | 812 ack-keys/s | 165,335 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,487,704 B | 4,194,304 B | 1,720,573 B |

### Key deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 385,900 ack-keys/s | 77,531 ack-keys/s | 107,542 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 6,574,889 B | 4,194,304 B | 2,080,573 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 766,209 ack-keys/s | 481,387 ack-keys/s | 2,778,163 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 6,199,461 B | 4,194,304 B | 1,952,073 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 1,650,303 ack-keys/s | 889,156 ack-keys/s | 3,170,975 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,779,016 B | 6,045,696 B | 1,950,733 B |
| `existing/random/clients=8` | `ack-keys/s` | 274,525 ack-keys/s | 56,396 ack-keys/s | 231,807 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 6,575,577 B | 4,194,304 B | 2,080,573 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 22,513 ack-keys/s | 25,252 ack-keys/s | 121,630 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 39,244,474 B | 51,425,280 B | 33,652,922 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 454,405 ack-keys/s | 79,821 ack-keys/s | 117,287 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 5,835,030 B | 4,194,304 B | 2,080,573 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 3,856,503 ack-keys/s | 2,778,807 ack-keys/s | 2,568,542 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 3,741,426 B | 4,194,304 B | 1,952,073 B |
| `missing/random/clients=1` | `ack-keys/s` | 3,080,991 ack-keys/s | 245,147 ack-keys/s | 170,769 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,487,704 B | 4,194,304 B | 1,720,573 B |

### Bucket deletion: durable

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 1,535 buckets/s | 949 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 3,734,992 B | 4,194,304 B |

### Bucket deletion: no-commit-sync

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 82,472 buckets/s | 56,626 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 3,734,992 B | 4,194,304 B |

### Read after delete: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 663.57 µs | 1.18 ms | 598.54 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 1.12 µs | 1.46 µs | 14.00 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 1.54 µs | 2.58 µs | 26.50 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 2.00 µs | 7.12 µs | 34.11 µs |

### Read after delete: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 2.62 µs | 20.79 µs | 7.21 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 291 ns | 584 ns | 6.75 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 375 ns | 2.04 µs | 8.71 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 542 ns | 3.17 µs | 13.96 µs |

### Page reuse: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 60,603 cycle-keys/s | 22,991 cycle-keys/s | 114,953 cycle-keys/s |
| `random-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `random-half/batch=100` | `post-delete-persistent-B` | 15,461,506 B | 16,777,216 B | 5,676,151 B |
| `random-half/batch=100` | `active-post-reinsert-persistent-B` | 17,670,623 B | 16,777,216 B | 8,457,601 B |
| `random-half/batch=100` | `stable-post-reinsert-persistent-B` | 13,881,344 B | 16,777,216 B | — |
| `random-half/batch=100` | `reuse-active-growth-B` | 3,789,279 B | 0 B | 2,897,200 B |
| `random-half/batch=100` | `reuse-stable-growth-B` | 0 B | 0 B | — |
| `random-half/batch=100` | `reinsert-pages-reused` | 1,035 | — | — |

### Page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 471,107 cycle-keys/s | 345,682 cycle-keys/s | 1,043,144 cycle-keys/s |
| `random-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `random-half/batch=100` | `post-delete-persistent-B` | 15,461,506 B | 16,777,216 B | 5,676,151 B |
| `random-half/batch=100` | `active-post-reinsert-persistent-B` | 17,670,623 B | 16,777,216 B | 8,457,601 B |
| `random-half/batch=100` | `stable-post-reinsert-persistent-B` | 13,881,344 B | 16,777,216 B | — |
| `random-half/batch=100` | `reuse-active-growth-B` | 3,789,279 B | 0 B | 2,897,200 B |
| `random-half/batch=100` | `reuse-stable-growth-B` | 0 B | 0 B | — |
| `random-half/batch=100` | `reinsert-pages-reused` | 1,035 | — | — |

### Repeated page reuse: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 41,427 cycle-keys/s | 22,240 cycle-keys/s | 102,547 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 12,638,038 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 10,360,028 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,679 | — | — |

### Repeated page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 699,857 cycle-keys/s | 489,325 cycle-keys/s | 1,550,842 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 12,322,505 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 10,360,028 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,679 | — | — |

### Tail reclamation: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 44,841 ack-keys/s | 62,541 ack-keys/s | 70,049 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,013,230 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 2.39 ms | 75.29 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 171.17 µs | 1.28 ms | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Tail reclamation: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 1,665,338 ack-keys/s | 1,738,714 ack-keys/s | 2,629,735 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,013,230 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 123.79 µs | 61.58 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 84.79 µs | 43.62 µs | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Reopen after deletion: durable

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `alternating-half` | `reopen-ns` | 147.12 µs | 1.39 ms |

### Reopen after deletion: no-commit-sync

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `alternating-half` | `reopen-ns` | 87.75 µs | 37.87 µs |

### Transactions

#### Read-only

| Keys per transaction | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| 1 | 1,761,135 keys/s | 1,081,721 keys/s | 150,267 keys/s |
| 10 | 2,961,278 keys/s | 1,903,251 keys/s | 970,624 keys/s |
| 100 | 3,151,661 keys/s | 2,004,847 keys/s | 1,997,351 keys/s |
| 1,000 | 3,141,960 keys/s | 1,991,282 keys/s | 2,492,511 keys/s |

#### Durable mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 12,585 operations/s | 7,055 operations/s | 15,181 operations/s |
| 95% | 100 | 87,531 operations/s | 61,209 operations/s | 143,021 operations/s |
| 95% | 1,000 | 577,250 operations/s | 303,260 operations/s | 623,945 operations/s |
| 50% | 10 | 13,526 operations/s | 3,520 operations/s | 16,423 operations/s |
| 50% | 100 | 97,744 operations/s | 19,476 operations/s | 131,117 operations/s |
| 50% | 1,000 | 456,117 operations/s | 124,005 operations/s | 431,785 operations/s |

#### No-commit-sync mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 1,206,271 operations/s | 635,953 operations/s | 582,129 operations/s |
| 95% | 100 | 1,630,169 operations/s | 1,465,689 operations/s | 1,311,886 operations/s |
| 95% | 1,000 | 1,903,327 operations/s | 1,684,108 operations/s | 1,218,496 operations/s |
| 50% | 10 | 1,038,748 operations/s | 322,575 operations/s | 431,459 operations/s |
| 50% | 100 | 1,291,368 operations/s | 616,656 operations/s | 889,998 operations/s |
| 50% | 1,000 | 1,540,729 operations/s | 892,067 operations/s | 1,300,531 operations/s |

### Enumeration

The 0% case returns no entries. This report omits its entries-per-second value because that rate does not apply.

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| selectivity=1 | 102,925,598 entries/s | 59,784,052 entries/s | 153,284 entries/s |
| selectivity=10 | 134,158,372 entries/s | 106,534,269 entries/s | 916,364 entries/s |
| selectivity=100 | 222,435,919 entries/s | 96,136,629 entries/s | 1,684,628 entries/s |

### Ordered operations

Redis does not provide the required ordered-cursor API. The 0% range case returns no entries. This report omits its entries-per-second value.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| full-forward | 223,266,726 entries/s | 102,842,805 entries/s |
| full-reverse | 246,115,759 entries/s | 111,000,693 entries/s |
| range/selectivity=1 | 109,167,892 entries/s | 68,528,659 entries/s |
| range/selectivity=10 | 132,767,069 entries/s | 98,083,723 entries/s |
| range/selectivity=100 | 222,368,743 entries/s | 101,660,130 entries/s |
| seek-and-read=1 | 2,302,385 entries/s | 927,603 entries/s |
| seek-and-read=10 | 19,228,796 entries/s | 9,119,927 entries/s |
| seek-and-read=100 | 65,084,417 entries/s | 54,471,352 entries/s |
| seek-and-read=1000 | 85,934,942 entries/s | 95,107,526 entries/s |

### Access distribution

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| records=10000/hot-80-20 | 3,875,633 reads/s | 1,002,331 reads/s | 163,313 reads/s |
| records=10000/uniform | 3,703,829 reads/s | 874,449 reads/s | 162,641 reads/s |
| records=100000/hot-80-20 | 2,353,761 reads/s | 1,263,522 reads/s | 156,621 reads/s |
| records=100000/uniform | 1,918,291 reads/s | 1,162,819 reads/s | 154,120 reads/s |

### Collections

KVLite and bbolt use native buckets. Redis uses logical key prefixes.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 45,749 keys/s | 64,468 keys/s | 34,682 keys/s |
| write/collections=1/depth=3 | 56,040 keys/s | 53,404 keys/s | 42,516 keys/s |
| write/collections=100/depth=1 | 48,237 keys/s | 54,191 keys/s | 41,782 keys/s |
| write/collections=100/depth=3 | 53,335 keys/s | 42,982 keys/s | 44,348 keys/s |
| read/collections=1/depth=1 | 1,748,492 reads/s | 825,505 reads/s | 158,292 reads/s |
| read/collections=1/depth=3 | 1,394,178 reads/s | 609,201 reads/s | 156,792 reads/s |
| read/collections=100/depth=1 | 1,482,613 reads/s | 776,432 reads/s | 156,034 reads/s |
| read/collections=100/depth=3 | 1,195,636 reads/s | 649,494 reads/s | 154,195 reads/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 1,726,308 keys/s | 1,034,823 keys/s | 874,385 keys/s |
| write/collections=1/depth=3 | 1,370,982 keys/s | 940,078 keys/s | 720,832 keys/s |
| write/collections=100/depth=1 | 1,741,920 keys/s | 2,219,017 keys/s | 603,885 keys/s |
| write/collections=100/depth=3 | 1,767,597 keys/s | 2,291,104 keys/s | 489,125 keys/s |

### Latency

The large scale reports one, eight, and 32 clients.

#### Durable

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Read | 1 | KVLite | 250 ns | 333 ns | 459 ns | 303.79 µs |
| Read | 1 | bbolt | 458 ns | 750 ns | 1.54 µs | 1.21 ms |
| Read | 1 | Redis | 5.79 µs | 6.79 µs | 12.46 µs | 4.35 ms |
| Read | 8 | KVLite | 416 ns | 666 ns | 1.00 µs | 18.43 ms |
| Read | 8 | bbolt | 541 ns | 1.50 µs | 5.71 µs | 24.51 ms |
| Read | 8 | Redis | 29.87 µs | 64.79 µs | 100.42 µs | 9.03 ms |
| Read | 32 | KVLite | 416 ns | 584 ns | 958 ns | 61.42 ms |
| Read | 32 | bbolt | 625 ns | 38.88 µs | 159.50 µs | 32.59 ms |
| Read | 32 | Redis | 60.54 µs | 245.66 µs | 462.29 µs | 12.01 ms |
| Update | 1 | KVLite | 555.57 µs | 1.55 ms | 2.96 ms | 242.19 ms |
| Update | 1 | bbolt | 1.07 ms | 1.43 ms | 1.78 ms | 12.21 ms |
| Update | 1 | Redis | 531.13 µs | 791.25 µs | 1.34 ms | 9.20 ms |
| Update | 8 | KVLite | 584.66 µs | 1.43 ms | 2.33 ms | 5.47 ms |
| Update | 8 | bbolt | 19.88 ms | 24.71 ms | 28.20 ms | 43.55 ms |
| Update | 8 | Redis | 596.70 µs | 1.23 ms | 2.14 ms | 5.80 ms |
| Update | 32 | KVLite | 705.16 µs | 2.00 ms | 3.67 ms | 5.78 ms |
| Update | 32 | bbolt | 23.02 ms | 29.92 ms | 32.26 ms | 36.53 ms |
| Update | 32 | Redis | 977.78 µs | 2.14 ms | 2.98 ms | 4.32 ms |
| Mixed | 1 | KVLite | 583 ns | 198.83 µs | 642.16 µs | 7.00 ms |
| Mixed | 1 | bbolt | 750 ns | 325.66 µs | 1.11 ms | 8.26 ms |
| Mixed | 1 | Redis | 6.46 µs | 413.69 µs | 1.11 ms | 10.21 ms |
| Mixed | 8 | KVLite | 458 ns | 565.29 µs | 928.48 µs | 5.80 ms |
| Mixed | 8 | bbolt | 3.12 µs | 6.18 ms | 20.78 ms | 40.27 ms |
| Mixed | 8 | Redis | 74.87 µs | 1.86 ms | 2.77 ms | 17.00 ms |
| Mixed | 32 | KVLite | 375 ns | 686.62 µs | 1.51 ms | 9.57 ms |
| Mixed | 32 | bbolt | 3.00 µs | 9.13 ms | 24.04 ms | 33.98 ms |
| Mixed | 32 | Redis | 1.05 ms | 3.40 ms | 4.52 ms | 9.85 ms |

#### No commit sync

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Update | 1 | KVLite | 1.62 µs | 3.54 µs | 5.54 µs | 297.12 µs |
| Update | 1 | bbolt | 8.54 µs | 14.50 µs | 20.71 µs | 3.36 ms |
| Update | 1 | Redis | 6.96 µs | 8.50 µs | 14.29 µs | 353.04 µs |
| Update | 8 | KVLite | 1.67 µs | 3.71 µs | 6.75 µs | 6.73 ms |
| Update | 8 | bbolt | 8.79 µs | 15.88 µs | 3.64 ms | 9.13 ms |
| Update | 8 | Redis | 29.38 µs | 72.12 µs | 92.80 µs | 4.24 ms |
| Update | 32 | KVLite | 1.67 µs | 3.79 µs | 2.35 ms | 8.21 ms |
| Update | 32 | bbolt | 8.96 µs | 2.59 ms | 6.78 ms | 11.74 ms |
| Update | 32 | Redis | 113.04 µs | 257.12 µs | 349.82 µs | 2.51 ms |
| Mixed | 1 | KVLite | 292 ns | 1.46 µs | 2.17 µs | 237.13 µs |
| Mixed | 1 | bbolt | 459 ns | 7.67 µs | 10.33 µs | 3.33 ms |
| Mixed | 1 | Redis | 5.67 µs | 7.50 µs | 11.83 µs | 2.74 ms |
| Mixed | 8 | KVLite | 292 ns | 1.54 µs | 3.25 µs | 9.68 ms |
| Mixed | 8 | bbolt | 542 ns | 9.42 µs | 26.12 µs | 15.20 ms |
| Mixed | 8 | Redis | 31.04 µs | 65.29 µs | 96.94 µs | 6.20 ms |
| Mixed | 32 | KVLite | 292 ns | 1.58 µs | 4.29 µs | 14.84 ms |
| Mixed | 32 | bbolt | 583 ns | 11.54 µs | 927.99 µs | 21.99 ms |
| Mixed | 32 | Redis | 72.58 µs | 238.62 µs | 386.00 µs | 8.08 ms |

### Life cycle

Redis does not have the same embedded life-cycle boundary.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| close-after-writes | 2.05 ms | 10.21 µs |
| create-load-close | 48.54 ms | 56.00 ms |
| open-clean | 91.55 µs | 2.54 ms |
| recover-after-process-kill | 4.99 ms | 2.92 ms |

### Persistent bytes

`persistent-B` is the KVLite database plus WAL, the bbolt database, or the Redis AOF. These files have different maintenance and compaction rules.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| read/random/hits=0/misses=above/value=128/clients=1 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=0/misses=below/value=128/clients=1 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=0/misses=between/value=128/clients=1 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=50/value=128/clients=1 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=100/value=128/clients=8 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=100/value=128/clients=32 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| read/random/key=32768/value=128/clients=1 | 37,728,256 B | 52,056,064 B | 16,858,764 B |
| read/random/value=32/clients=1 | 1,359,872 B | 2,097,152 B | 750,401 B |
| read/random/value=128/clients=1 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| read/random/value=1024/clients=1 | 41,619,456 B | 35,549,184 B | 10,690,401 B |
| read/random/value=3072/clients=1 | 41,627,648 B | 58,118,144 B | 31,170,401 B |
| read/random/value=65536/clients=1 | 35,700,736 B | 51,425,280 B | 33,578,124 B |
| read/random/value=1048576/clients=1 | 67,653,632 B | 84,045,824 B | 67,112,076 B |
| read/sequential/hits=100/value=128/clients=1 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| update/random/grow=32-1024/clients=1 | 3,904,151 B | 4,194,304 B | 1,819,476 B |
| update/random/grow=32-1024/clients=1/batch=100 | 23,491,011 B | 33,640,448 B | 11,443,376 B |
| update/random/grow=128-65536/clients=1 | 36,556,544 B | 50,573,312 B | 65,670,376 B |
| update/random/shrink=1024-32/clients=1 | 41,752,600 B | 35,549,184 B | 10,766,470 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,695,143 B | 35,549,184 B | 11,444,370 B |
| update/random/shrink=65536-128/clients=1 | 35,937,726 B | 51,425,280 B | 33,815,706 B |
| update/random/value=32/clients=1 | 2,031,954 B | 2,097,152 B | 825,476 B |
| update/random/value=128/clients=1 | 4,682,885 B | 4,194,304 B | 1,892,573 B |
| update/random/value=128/clients=1/batch=10 | 5,457,093 B | 4,194,304 B | 3,469,573 B |
| update/random/value=128/clients=1/batch=100 | 5,421,529 B | 4,194,304 B | 3,443,473 B |
| update/random/value=128/clients=1/batch=1000 | 5,328,254 B | 8,388,608 B | 3,440,863 B |
| update/random/value=128/clients=8 | 5,198,535 B | 4,194,304 B | 2,270,973 B |
| update/random/value=128/clients=8/batch=100 | 5,345,099 B | 4,194,304 B | 3,443,473 B |
| update/random/value=128/clients=32 | 5,189,677 B | 4,194,304 B | 2,270,973 B |
| update/random/value=128/clients=32/batch=100 | 5,231,416 B | 4,194,304 B | 3,443,473 B |
| update/random/value=1024/clients=1 | 42,744,600 B | 35,549,184 B | 11,760,470 B |
| update/random/value=3072/clients=1 | 44,802,840 B | 58,118,144 B | 34,290,518 B |
| update/random/value=65536/clients=1 | 36,298,896 B | 51,425,280 B | 99,225,706 B |
| update/sequential/value=128/clients=1/batch=10 | 5,212,464 B | 4,194,304 B | 3,469,573 B |
| update/sequential/value=128/clients=1/batch=100 | 5,129,407 B | 4,194,304 B | 3,443,473 B |
| update/sequential/value=128/clients=1/batch=1000 | 5,122,417 B | 4,194,304 B | 3,440,863 B |
| insert/random/value=128/clients=1 | 3,855,000 B | 4,194,304 B | 1,892,573 B |
| insert/random/value=128/clients=1/batch=10 | 6,866,304 B | 8,388,608 B | 3,469,573 B |
| insert/random/value=128/clients=1/batch=100 | 6,670,681 B | 8,388,608 B | 3,443,473 B |
| insert/random/value=128/clients=1/batch=1000 | 6,270,082 B | 8,388,608 B | 3,440,863 B |
| insert/random/value=128/clients=8 | 4,575,134 B | 8,388,608 B | 2,270,973 B |
| insert/random/value=128/clients=8/batch=100 | 6,337,755 B | 8,388,608 B | 3,443,473 B |
| insert/random/value=128/clients=32 | 4,520,417 B | 8,388,608 B | 2,270,973 B |
| insert/random/value=128/clients=32/batch=100 | 5,833,023 B | 8,388,608 B | 3,443,473 B |
| insert/random/value=65536/clients=1 | 72,244,992 B | 85,176,320 B | 99,225,706 B |
| insert/sequential/value=128/clients=1 | 3,923,221 B | 4,194,304 B | 1,892,573 B |
| insert/sequential/value=128/clients=1/batch=10 | 6,620,138 B | 8,388,608 B | 3,469,573 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,371,443 B | 8,388,608 B | 3,443,473 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,191,218 B | 8,388,608 B | 3,440,863 B |
| mixed/read=50/value=128/clients=1 | 5,374,667 B | 4,194,304 B | 2,580,573 B |
| mixed/read=50/value=128/clients=8 | 5,267,222 B | 4,194,304 B | 2,580,573 B |
| mixed/read=50/value=128/clients=32 | 5,253,882 B | 4,194,304 B | 2,580,573 B |
| mixed/read=95/value=128/clients=1 | 4,262,339 B | 4,194,304 B | 1,806,573 B |
| mixed/read=95/value=128/clients=8 | 4,252,630 B | 4,194,304 B | 1,806,573 B |
| mixed/read=95/value=128/clients=32 | 4,250,553 B | 4,194,304 B | 1,806,573 B |
| mixed/read=95/value=65536/clients=1 | 39,422,516 B | 51,425,280 B | 50,039,206 B |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 3,904,151 B | 2,469,888 B | 1,819,476 B |
| update/random/grow=32-1024/clients=1/batch=100 | 23,491,011 B | 19,460,096 B | 11,443,376 B |
| update/random/grow=128-65536/clients=1 | 36,556,544 B | 38,326,272 B | 65,670,376 B |
| update/random/shrink=1024-32/clients=1 | 41,752,600 B | 35,549,184 B | 10,766,470 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,695,143 B | 35,549,184 B | 11,444,370 B |
| update/random/shrink=65536-128/clients=1 | 35,937,726 B | 51,425,280 B | 33,815,706 B |
| update/random/value=32/clients=1 | 2,031,954 B | 2,097,152 B | 825,476 B |
| update/random/value=128/clients=1 | 4,682,885 B | 4,194,304 B | 1,892,573 B |
| update/random/value=128/clients=1/batch=10 | 5,457,093 B | 4,194,304 B | 3,469,573 B |
| update/random/value=128/clients=1/batch=100 | 5,421,529 B | 4,194,304 B | 3,443,473 B |
| update/random/value=128/clients=1/batch=1000 | 5,328,254 B | 6,082,560 B | 3,440,863 B |
| update/random/value=128/clients=8 | 5,268,569 B | 4,194,304 B | 2,270,973 B |
| update/random/value=128/clients=8/batch=100 | 5,421,538 B | 4,194,304 B | 3,443,473 B |
| update/random/value=128/clients=32 | 5,268,569 B | 4,194,304 B | 2,270,973 B |
| update/random/value=128/clients=32/batch=100 | 5,421,511 B | 4,194,304 B | 3,443,473 B |
| update/random/value=1024/clients=1 | 42,744,600 B | 35,549,184 B | 11,760,470 B |
| update/random/value=3072/clients=1 | 44,802,840 B | 58,118,144 B | 34,290,518 B |
| update/random/value=65536/clients=1 | 36,298,896 B | 51,425,280 B | 99,225,706 B |
| update/sequential/value=128/clients=1/batch=10 | 5,212,464 B | 4,194,304 B | 3,469,573 B |
| update/sequential/value=128/clients=1/batch=100 | 5,129,407 B | 4,194,304 B | 3,443,473 B |
| update/sequential/value=128/clients=1/batch=1000 | 5,122,417 B | 4,194,304 B | 3,440,863 B |
| insert/random/value=128/clients=1 | 3,855,000 B | 4,194,304 B | 1,892,573 B |
| insert/random/value=128/clients=1/batch=10 | 6,866,304 B | 5,910,528 B | 3,469,573 B |
| insert/random/value=128/clients=1/batch=100 | 6,670,681 B | 6,303,744 B | 3,443,473 B |
| insert/random/value=128/clients=1/batch=1000 | 6,270,082 B | 7,663,616 B | 3,440,863 B |
| insert/random/value=128/clients=8 | 4,657,330 B | 4,263,936 B | 2,270,973 B |
| insert/random/value=128/clients=8/batch=100 | 6,671,401 B | 6,279,168 B | 3,443,473 B |
| insert/random/value=128/clients=32 | 4,656,617 B | 4,259,840 B | 2,270,973 B |
| insert/random/value=128/clients=32/batch=100 | 6,661,467 B | 6,266,880 B | 3,443,473 B |
| insert/random/value=65536/clients=1 | 72,244,992 B | 73,461,760 B | 99,225,706 B |
| insert/sequential/value=128/clients=1 | 3,923,221 B | 4,194,304 B | 1,892,573 B |
| insert/sequential/value=128/clients=1/batch=10 | 6,620,138 B | 6,971,392 B | 3,469,573 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,371,443 B | 6,971,392 B | 3,443,473 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,191,218 B | 6,971,392 B | 3,440,863 B |
| mixed/read=50/value=128/clients=1 | 5,374,667 B | 4,194,304 B | 2,580,573 B |
| mixed/read=50/value=128/clients=8 | 5,374,667 B | 4,575,232 B | 2,580,573 B |
| mixed/read=50/value=128/clients=32 | 5,374,667 B | 4,743,168 B | 2,580,573 B |
| mixed/read=95/value=128/clients=1 | 4,262,339 B | 4,194,304 B | 1,806,573 B |
| mixed/read=95/value=128/clients=8 | 4,262,339 B | 4,751,360 B | 1,806,573 B |
| mixed/read=95/value=128/clients=32 | 4,262,339 B | 4,997,120 B | 1,806,573 B |
| mixed/read=95/value=65536/clients=1 | 39,422,516 B | 51,425,280 B | 50,039,206 B |
