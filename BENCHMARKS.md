# KVLite benchmarks

This report compares KVLite with bbolt and Redis at the public call boundary. It contains the median of 15 measured runs at the complete `large --workloads=all --storage=volume` scale. The same run then profiled every KVLite case.

| Item | Value |
| --- | --- |
| Run time | `2026-10-17T01:57:27Z` |
| Timing duration | 2 hours 31 minutes 45 seconds |
| Profile capture | 39 minutes 56 seconds, KVLite cases only |
| KVLite | `689a4dc706f30cadf2bb5adb900af4dcca62c8a4`; the only untracked file was a Markdown note outside the build |
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
| Point read | Read-only, random, 128-byte value, eight clients | 5,985,140 keys/s | 988,029 keys/s | 246,384 keys/s | **KVLite: 6.06x bbolt; 24.29x Redis** |
| Point-read p99 | Read-only, eight clients | 792 ns | 5.33 µs | 99.67 µs | **KVLite: 6.73x advantage over bbolt; 125.84x over Redis** |
| Point update | Durable, random, 128-byte value, one client, batch of 100 | 113,511 keys/s | 27,053 keys/s | 39,352 keys/s | **KVLite: 4.20x bbolt; 2.88x Redis** |
| Point insert | Durable, random, 128-byte value, one client, batch of 100 | 125,512 keys/s | 28,999 keys/s | 42,909 keys/s | **KVLite: 4.33x bbolt; 2.93x Redis** |
| Mixed work | Durable, 95% reads, eight clients | 148,031 operations/s | 8,111 operations/s | 14,266 operations/s | **KVLite: 18.25x bbolt; 10.38x Redis** |
| Mixed work | No commit sync, 95% reads, eight clients | 2,116,491 operations/s | 358,264 operations/s | 224,632 operations/s | **KVLite: 5.91x bbolt; 9.42x Redis** |
| Key delete | No commit sync, random, one client | 403,867 keys/s | 80,063 keys/s | 110,016 keys/s | **KVLite: 5.04x bbolt; 3.67x Redis** |
| Large-key read | Read-only, random, 32 KiB key, one client | 77,414 keys/s | 62,654 keys/s | 45,132 keys/s | **KVLite: 1.24x bbolt; 1.72x Redis** |
| Clean open | Life cycle | 76.70 µs | 2.96 ms | Not comparable | **KVLite: 38.62x advantage over bbolt** |

These ratios apply only to the named workloads and public call boundaries. KVLite did not lead every case. bbolt read 64 KiB values 1.31x faster and 1 MiB values 1.34x faster. Redis led six of 13 no-commit-sync insert cases. The full tables show every case.

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
- KVLite: `689a4dc706f30cadf2bb5adb900af4dcca62c8a4`
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

Setup, fixture loading, final stored-data checks, and close operations were outside the core timers. The named life-cycle cases measure open, close, and recovery work separately. The runner ran each life-cycle case in its own process. Before the timed recovery open, the harness waits until the killed helper process releases its file lock.

After the 15 timed rounds, the runner profiled every KVLite case again with 15 repeats. It recorded CPU profiles, allocation profiles, and execution traces. The profile pass does not change the timing values in this report. KVLite rows also report WAL commits, WAL bytes, write calls, and storage bytes written per operation.

## Changes since the previous report

The previous report measured `053ecf9` on 2026-10-16. That commit was the work-in-progress form of the WAL entry patches. The history was then rewritten: this run measures the reviewed WAL commits (`39f1eff` to `689a4dc`) and four performance fixes (`f0c901a`, `d9c39a3`, `b252453`, `f92f38f`). The host did not change. As a geometric mean over 189 cases, KVLite times went down by 3.3%, and bbolt times did not change.

| Workload group | KVLite time ratio | bbolt time ratio |
| --- | ---: | ---: |
| Durable reopen after deletion | 0.046 | 0.610 |
| Durable life cycle | 0.517 | 1.019 |
| Access-distribution reads | 0.833 | 1.037 |
| Read transactions | 0.866 | 0.998 |
| Durable point reads | 0.918 | 1.031 |
| No-commit-sync mixed work | 0.942 | 1.057 |
| Ordered operations | 0.973 | 1.003 |
| No-commit-sync point updates | 0.984 | 0.996 |
| No-commit-sync point inserts | 1.015 | 0.995 |
| Durable point updates | 1.002 | 1.022 |
| Durable point inserts | 1.092 | 1.017 |

A ratio below 1 means a lower time. Each ratio is the geometric mean of the main metric of each case in the group.

- A durable open is fast again. Commit `f0c901a` moved the directory sync from every writable open to the first WAL sync of a handle, as SQLite does for a new journal or WAL file. The clean open went from 1.54 ms to 76.7 µs, and the durable reopen after deletion from 2.21 ms to 102 µs.
- Reads are faster. Commit `d9c39a3` reads the header of a one-page node in a small inlined function, and commit `f92f38f` keeps `LookupMappedNode` under the inline budget. In paired micro benchmarks against `053ecf9`, point reads are 19% faster, absent-key reads 21% faster, 100 reads in one transaction 22% faster, and a full scan 6% faster.
- WAL bytes per operation did not change in any case (all within 1%). The WAL record rules are the same as in `053ecf9`.

Paired runs of `053ecf9` and `689a4dc` in one container, with a same-binary control, checked each case that looked slower:

- The no-commit-sync reopen after deletion was 1.98 times slower in this run, but 0.92 times in paired runs (p = 0.27). Its KVLite samples vary between runs (median 35 µs to 69 µs), while bbolt stays near 35 µs.
- Durable inserts and updates in batches of 1,000 (1.6 times), a durable update with 1,024-byte values (1.28 times), and repeated durable page reuse (1.22 times) did not reproduce. In the paired durable runs, the same-binary control alone varied from 0.72 to 1.75 times. A batch-1000 case makes one commit, and since `f0c901a` that commit also syncs the directory, which the open did before it. The total work for each handle did not change.
- The recovery open after a killed process went from 1.92 ms to 2.91 ms. This change is not significant (p = 0.15), and the samples of both runs overlap.
- In a micro benchmark, the WAL encoding of a one-page transaction is 6.5 ns slower (10%). Commit `1d8a2e3` builds a list of pending pages during encoding. Point writes take about 2 µs, so this cost is below the resolution of the write benchmarks.

## Interpretation

These statements apply only to this run and its tested workloads.

- KVLite led 92 of 118 durable cases. bbolt led nine. Redis led 17.
- KVLite led 47 of 71 no-commit-sync cases. bbolt led six. Redis led 18.
- Each count uses one main metric for each case: the median time per operation, or p99 for latency cases. It excludes storage-size metrics.
- KVLite led 40 of 43 read cases, including point reads, read transactions, scans, ordered reads, access distributions, collection reads, and read latency. bbolt led 64 KiB value reads, 1 MiB value reads, and `seek-and-read=1000`.
- KVLite led 15 of 21 durable point-update cases and 12 of 13 durable point-insert cases. Redis led the other cases.
- KVLite led 14 of 21 no-commit-sync point-update cases and seven of 13 no-commit-sync point-insert cases. Redis led most of the other cases.
- KVLite led acknowledged mixed work in six of seven cases in each mode. bbolt led the cases with 64 KiB values.
- KVLite led 14 of 28 deletion, reuse, and reopen cases across both modes. Redis led 12. bbolt led two.
- KVLite opened a clean database 38.62x faster than bbolt. The recovery open after a killed process took 2.91 ms for KVLite and 2.89 ms for bbolt. bbolt closed after writes 187x faster, because KVLite checkpoints its WAL during close.

## Limits

- This is one run on one container host. It is not a bare-metal Linux result.
- Docker volume storage includes the OrbStack virtual machine and host storage path. It does not isolate physical-device sync latency.
- Durable write times depend on storage sync latency. That latency changed by up to four times between rounds of the same binary.
- Each timed run opens a new handle. Since `f0c901a`, the first durable commit of a handle syncs the directory, so cases with few commits include that sync.
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
| read/random/hits=0/misses=above/value=128/clients=1 | 5,578,209 keys/s | 1,680,948 keys/s | 144,981 keys/s |
| read/random/hits=0/misses=below/value=128/clients=1 | 5,582,900 keys/s | 2,040,725 keys/s | 146,883 keys/s |
| read/random/hits=0/misses=between/value=128/clients=1 | 5,019,074 keys/s | 1,732,209 keys/s | 145,625 keys/s |
| read/random/hits=50/value=128/clients=1 | 3,837,562 keys/s | 1,496,668 keys/s | 155,216 keys/s |
| read/random/hits=100/value=128/clients=8 | 5,985,140 keys/s | 988,029 keys/s | 246,384 keys/s |
| read/random/hits=100/value=128/clients=32 | 4,698,856 keys/s | 884,473 keys/s | 340,841 keys/s |
| read/random/key=32768/value=128/clients=1 | 77,414 keys/s | 62,654 keys/s | 45,132 keys/s |
| read/random/value=32/clients=1 | 3,496,652 keys/s | 1,351,304 keys/s | 174,429 keys/s |
| read/random/value=128/clients=1 | 3,145,178 keys/s | 1,432,150 keys/s | 163,271 keys/s |
| read/random/value=1024/clients=1 | 1,955,206 keys/s | 1,340,000 keys/s | 158,472 keys/s |
| read/random/value=3072/clients=1 | 1,519,095 keys/s | 1,157,964 keys/s | 132,915 keys/s |
| read/random/value=65536/clients=1 | 182,944 keys/s | 240,460 keys/s | 48,963 keys/s |
| read/random/value=1048576/clients=1 | 18,854 keys/s | 25,256 keys/s | 6,607 keys/s |
| read/sequential/hits=100/value=128/clients=1 | 4,406,907 keys/s | 1,560,242 keys/s | 168,187 keys/s |
| update/random/grow=32-1024/clients=1 | 1,665 keys/s | 826 keys/s | 1,683 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 30,702 keys/s | 13,758 keys/s | 38,520 keys/s |
| update/random/grow=128-65536/clients=1 | 1,299 keys/s | 845 keys/s | 398 keys/s |
| update/random/shrink=1024-32/clients=1 | 1,904 keys/s | 918 keys/s | 2,125 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 56,374 keys/s | 12,967 keys/s | 42,956 keys/s |
| update/random/shrink=65536-128/clients=1 | 1,796 keys/s | 1,006 keys/s | 1,300 keys/s |
| update/random/value=32/clients=1 | 1,494 keys/s | 865 keys/s | 1,810 keys/s |
| update/random/value=128/clients=1 | 1,840 keys/s | 934 keys/s | 1,740 keys/s |
| update/random/value=128/clients=1/batch=10 | 17,602 keys/s | 5,321 keys/s | 13,945 keys/s |
| update/random/value=128/clients=1/batch=100 | 113,511 keys/s | 27,053 keys/s | 39,352 keys/s |
| update/random/value=128/clients=1/batch=1000 | 351,878 keys/s | 117,066 keys/s | 287,648 keys/s |
| update/random/value=128/clients=8 | 13,232 keys/s | 384 keys/s | 11,192 keys/s |
| update/random/value=128/clients=8/batch=100 | 257,964 keys/s | 21,874 keys/s | 217,225 keys/s |
| update/random/value=128/clients=32 | 42,717 keys/s | 1,332 keys/s | 10,534 keys/s |
| update/random/value=128/clients=32/batch=100 | 424,393 keys/s | 21,564 keys/s | 385,810 keys/s |
| update/random/value=1024/clients=1 | 1,435 keys/s | 932 keys/s | 1,543 keys/s |
| update/random/value=3072/clients=1 | 1,576 keys/s | 818 keys/s | 1,590 keys/s |
| update/random/value=65536/clients=1 | 1,236 keys/s | 885 keys/s | 1,016 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 18,436 keys/s | 7,327 keys/s | 12,467 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 138,217 keys/s | 76,563 keys/s | 40,877 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 758,898 keys/s | 395,489 keys/s | 319,256 keys/s |
| insert/random/value=128/clients=1 | 2,050 keys/s | 844 keys/s | 1,978 keys/s |
| insert/random/value=128/clients=1/batch=10 | 17,311 keys/s | 5,346 keys/s | 15,594 keys/s |
| insert/random/value=128/clients=1/batch=100 | 125,512 keys/s | 28,999 keys/s | 42,909 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 363,124 keys/s | 231,425 keys/s | 326,102 keys/s |
| insert/random/value=128/clients=8 | 13,623 keys/s | 413 keys/s | 6,947 keys/s |
| insert/random/value=128/clients=8/batch=100 | 227,574 keys/s | 29,564 keys/s | 210,154 keys/s |
| insert/random/value=128/clients=32 | 39,128 keys/s | 1,493 keys/s | 9,477 keys/s |
| insert/random/value=128/clients=32/batch=100 | 394,224 keys/s | 28,036 keys/s | 413,439 keys/s |
| insert/random/value=65536/clients=1 | 951 keys/s | 842 keys/s | 670 keys/s |
| insert/sequential/value=128/clients=1 | 2,077 keys/s | 952 keys/s | 2,045 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 17,663 keys/s | 10,116 keys/s | 13,661 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 139,829 keys/s | 83,232 keys/s | 43,252 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 623,721 keys/s | 246,006 keys/s | 314,994 keys/s |
| mixed/read=50/value=128/clients=1 | 3,961 operations/s | 1,646 operations/s | 3,377 operations/s |
| mixed/read=50/value=128/clients=8 | 24,408 operations/s | 773 operations/s | 12,075 operations/s |
| mixed/read=50/value=128/clients=32 | 62,121 operations/s | 2,692 operations/s | 24,707 operations/s |
| mixed/read=95/value=128/clients=1 | 39,517 operations/s | 5,578 operations/s | 9,604 operations/s |
| mixed/read=95/value=128/clients=8 | 148,031 operations/s | 8,111 operations/s | 14,266 operations/s |
| mixed/read=95/value=128/clients=32 | 362,931 operations/s | 25,946 operations/s | 16,869 operations/s |
| mixed/read=95/value=65536/clients=1 | 12,265 operations/s | 13,585 operations/s | 7,350 operations/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 201,760 keys/s | 86,838 keys/s | 108,789 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 264,716 keys/s | 258,460 keys/s | 651,753 keys/s |
| update/random/grow=128-65536/clients=1 | 30,094 keys/s | 30,408 keys/s | 29,277 keys/s |
| update/random/shrink=1024-32/clients=1 | 384,743 keys/s | 95,308 keys/s | 90,209 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 603,679 keys/s | 299,864 keys/s | 1,058,986 keys/s |
| update/random/shrink=65536-128/clients=1 | 151,400 keys/s | 45,345 keys/s | 133,509 keys/s |
| update/random/value=32/clients=1 | 379,643 keys/s | 54,833 keys/s | 111,845 keys/s |
| update/random/value=128/clients=1 | 329,063 keys/s | 77,681 keys/s | 91,266 keys/s |
| update/random/value=128/clients=1/batch=10 | 890,078 keys/s | 203,409 keys/s | 425,750 keys/s |
| update/random/value=128/clients=1/batch=100 | 918,336 keys/s | 444,132 keys/s | 1,263,412 keys/s |
| update/random/value=128/clients=1/batch=1000 | 1,444,366 keys/s | 591,423 keys/s | 1,378,176 keys/s |
| update/random/value=128/clients=8 | 282,309 keys/s | 57,396 keys/s | 199,646 keys/s |
| update/random/value=128/clients=8/batch=100 | 675,949 keys/s | 334,220 keys/s | 1,087,951 keys/s |
| update/random/value=128/clients=32 | 289,344 keys/s | 51,633 keys/s | 207,640 keys/s |
| update/random/value=128/clients=32/batch=100 | 700,473 keys/s | 369,078 keys/s | 1,338,980 keys/s |
| update/random/value=1024/clients=1 | 281,659 keys/s | 94,562 keys/s | 91,835 keys/s |
| update/random/value=3072/clients=1 | 237,962 keys/s | 87,141 keys/s | 86,091 keys/s |
| update/random/value=65536/clients=1 | 25,598 keys/s | 38,125 keys/s | 32,865 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 2,169,293 keys/s | 624,514 keys/s | 542,367 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 3,246,363 keys/s | 2,058,082 keys/s | 998,202 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 3,544,360 keys/s | 2,725,094 keys/s | 1,443,593 keys/s |
| insert/random/value=128/clients=1 | 480,286 keys/s | 64,819 keys/s | 98,538 keys/s |
| insert/random/value=128/clients=1/batch=10 | 477,434 keys/s | 161,315 keys/s | 473,293 keys/s |
| insert/random/value=128/clients=1/batch=100 | 613,548 keys/s | 337,679 keys/s | 856,158 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 1,092,978 keys/s | 800,008 keys/s | 950,913 keys/s |
| insert/random/value=128/clients=8 | 177,010 keys/s | 46,095 keys/s | 187,678 keys/s |
| insert/random/value=128/clients=8/batch=100 | 424,664 keys/s | 235,429 keys/s | 977,962 keys/s |
| insert/random/value=128/clients=32 | 175,807 keys/s | 38,150 keys/s | 226,228 keys/s |
| insert/random/value=128/clients=32/batch=100 | 451,024 keys/s | 292,943 keys/s | 1,031,440 keys/s |
| insert/random/value=65536/clients=1 | 23,545 keys/s | 24,081 keys/s | 31,868 keys/s |
| insert/sequential/value=128/clients=1 | 465,917 keys/s | 62,417 keys/s | 111,596 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 947,844 keys/s | 431,512 keys/s | 471,192 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 2,064,673 keys/s | 1,589,501 keys/s | 952,512 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 3,755,694 keys/s | 2,623,431 keys/s | 974,094 keys/s |
| mixed/read=50/value=128/clients=1 | 765,083 operations/s | 156,202 operations/s | 141,515 operations/s |
| mixed/read=50/value=128/clients=8 | 560,637 operations/s | 88,531 operations/s | 228,598 operations/s |
| mixed/read=50/value=128/clients=32 | 524,714 operations/s | 69,218 operations/s | 244,165 operations/s |
| mixed/read=95/value=128/clients=1 | 1,988,394 operations/s | 642,527 operations/s | 153,385 operations/s |
| mixed/read=95/value=128/clients=8 | 2,116,491 operations/s | 358,264 operations/s | 224,632 operations/s |
| mixed/read=95/value=128/clients=32 | 2,092,218 operations/s | 292,322 operations/s | 270,078 operations/s |
| mixed/read=95/value=65536/clients=1 | 162,061 operations/s | 184,254 operations/s | 49,648 operations/s |

### Key deletion: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 1,543 ack-keys/s | 583 ack-keys/s | 1,784 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 6,574,660 B | 4,194,304 B | 2,080,401 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 84,713 ack-keys/s | 40,548 ack-keys/s | 145,141 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 6,199,232 B | 4,194,304 B | 1,951,901 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 569,083 ack-keys/s | 230,027 ack-keys/s | 389,135 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,778,787 B | 8,388,608 B | 1,950,561 B |
| `existing/random/clients=8` | `ack-keys/s` | 11,432 ack-keys/s | 394 ack-keys/s | 11,153 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 6,341,620 B | 4,194,304 B | 2,080,401 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 1,255 ack-keys/s | 807 ack-keys/s | 1,497 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 39,177,875 B | 51,425,280 B | 33,587,340 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 1,786 ack-keys/s | 908 ack-keys/s | 1,675 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 5,834,801 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 162,203 ack-keys/s | 91,514 ack-keys/s | 65,257 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 3,739,438 B | 4,194,304 B | 1,951,901 B |
| `missing/random/clients=1` | `ack-keys/s` | 322,917 ack-keys/s | 902 ack-keys/s | 154,307 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,485,696 B | 4,194,304 B | 1,720,401 B |

### Key deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 403,867 ack-keys/s | 80,063 ack-keys/s | 110,016 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 6,574,660 B | 4,194,304 B | 2,080,401 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 870,465 ack-keys/s | 471,630 ack-keys/s | 2,869,670 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 6,199,232 B | 4,194,304 B | 1,951,901 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 1,639,089 ack-keys/s | 861,985 ack-keys/s | 3,153,153 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,778,787 B | 6,045,696 B | 1,950,561 B |
| `existing/random/clients=8` | `ack-keys/s` | 277,000 ack-keys/s | 56,027 ack-keys/s | 234,899 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 6,574,776 B | 4,194,304 B | 2,080,401 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 23,758 ack-keys/s | 26,314 ack-keys/s | 123,634 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 39,177,877 B | 51,425,280 B | 33,587,340 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 450,650 ack-keys/s | 81,157 ack-keys/s | 121,899 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 5,834,801 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 4,423,921 ack-keys/s | 2,467,326 ack-keys/s | 2,641,475 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 3,739,438 B | 4,194,304 B | 1,951,901 B |
| `missing/random/clients=1` | `ack-keys/s` | 3,034,343 ack-keys/s | 251,105 ack-keys/s | 164,873 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,485,696 B | 4,194,304 B | 1,720,401 B |

### Bucket deletion: durable

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 1,813 buckets/s | 870 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 3,734,992 B | 4,194,304 B |

### Bucket deletion: no-commit-sync

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 77,217 buckets/s | 74,439 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 3,734,992 B | 4,194,304 B |

### Read after delete: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 665.95 µs | 1.98 ms | 634.70 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 1.12 µs | 1.46 µs | 13.46 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 1.50 µs | 14.67 µs | 23.62 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 1.88 µs | 26.42 µs | 32.88 µs |

### Read after delete: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 2.82 µs | 20.44 µs | 7.05 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 291 ns | 542 ns | 6.58 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 375 ns | 1.96 µs | 8.17 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 500 ns | 2.96 µs | 13.46 µs |

### Page reuse: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 67,625 cycle-keys/s | 22,247 cycle-keys/s | 124,203 cycle-keys/s |
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
| `random-half/batch=100` | `cycle-keys/s` | 495,892 cycle-keys/s | 358,362 cycle-keys/s | 1,061,819 cycle-keys/s |
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
| `ten-cycles/batch=100` | `cycle-keys/s` | 34,224 cycle-keys/s | 22,187 cycle-keys/s | 91,101 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 12,638,038 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 10,360,028 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,679 | — | — |

### Repeated page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 713,819 cycle-keys/s | 496,220 cycle-keys/s | 1,576,817 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 12,322,505 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 10,360,028 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,679 | — | — |

### Tail reclamation: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 44,578 ack-keys/s | 65,838 ack-keys/s | 47,959 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,013,230 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 2.25 ms | 63.71 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 138.54 µs | 1.34 ms | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Tail reclamation: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 1,733,094 ack-keys/s | 1,531,827 ack-keys/s | 2,733,967 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,013,230 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 133.38 µs | 62.74 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 60.75 µs | 41.25 µs | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Reopen after deletion: durable

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `alternating-half` | `reopen-ns` | 102.08 µs | 1.46 ms |

### Reopen after deletion: no-commit-sync

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `alternating-half` | `reopen-ns` | 68.71 µs | 33.88 µs |

### Transactions

#### Read-only

| Keys per transaction | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| 1 | 1,699,046 keys/s | 1,057,094 keys/s | 154,674 keys/s |
| 10 | 2,903,981 keys/s | 1,923,146 keys/s | 933,651 keys/s |
| 100 | 3,065,368 keys/s | 1,909,833 keys/s | 2,011,981 keys/s |
| 1,000 | 3,062,199 keys/s | 2,117,002 keys/s | 2,386,973 keys/s |

#### Durable mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 8,864 operations/s | 8,268 operations/s | 16,019 operations/s |
| 95% | 100 | 124,754 operations/s | 65,907 operations/s | 152,228 operations/s |
| 95% | 1,000 | 687,602 operations/s | 347,344 operations/s | 345,406 operations/s |
| 50% | 10 | 13,632 operations/s | 5,834 operations/s | 13,914 operations/s |
| 50% | 100 | 122,874 operations/s | 25,898 operations/s | 54,975 operations/s |
| 50% | 1,000 | 503,643 operations/s | 127,446 operations/s | 323,163 operations/s |

#### No-commit-sync mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 1,296,340 operations/s | 640,007 operations/s | 557,644 operations/s |
| 95% | 100 | 1,730,183 operations/s | 1,452,698 operations/s | 1,236,591 operations/s |
| 95% | 1,000 | 1,947,993 operations/s | 1,784,394 operations/s | 1,171,728 operations/s |
| 50% | 10 | 1,119,473 operations/s | 328,672 operations/s | 446,671 operations/s |
| 50% | 100 | 1,325,929 operations/s | 622,522 operations/s | 924,100 operations/s |
| 50% | 1,000 | 1,603,673 operations/s | 848,283 operations/s | 1,210,986 operations/s |

### Enumeration

The 0% case returns no entries. This report omits its entries-per-second value because that rate does not apply.

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| selectivity=1 | 108,598,869 entries/s | 67,618,972 entries/s | 157,068 entries/s |
| selectivity=10 | 133,643,565 entries/s | 110,091,105 entries/s | 929,790 entries/s |
| selectivity=100 | 231,517,093 entries/s | 101,164,955 entries/s | 1,718,516 entries/s |

### Ordered operations

Redis does not provide the required ordered-cursor API. The 0% range case returns no entries. This report omits its entries-per-second value.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| full-forward | 247,248,432 entries/s | 104,228,437 entries/s |
| full-reverse | 250,633,818 entries/s | 111,210,296 entries/s |
| range/selectivity=1 | 109,424,663 entries/s | 67,517,578 entries/s |
| range/selectivity=10 | 134,571,128 entries/s | 97,541,953 entries/s |
| range/selectivity=100 | 227,273,440 entries/s | 102,572,730 entries/s |
| seek-and-read=1 | 2,366,024 entries/s | 909,509 entries/s |
| seek-and-read=10 | 18,969,560 entries/s | 9,026,214 entries/s |
| seek-and-read=100 | 65,560,246 entries/s | 57,873,140 entries/s |
| seek-and-read=1000 | 86,360,395 entries/s | 95,803,555 entries/s |

### Access distribution

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| records=10000/hot-80-20 | 3,695,031 reads/s | 1,003,413 reads/s | 167,399 reads/s |
| records=10000/uniform | 3,389,362 reads/s | 891,373 reads/s | 166,249 reads/s |
| records=100000/hot-80-20 | 2,246,979 reads/s | 1,351,095 reads/s | 160,496 reads/s |
| records=100000/uniform | 2,069,225 reads/s | 1,122,332 reads/s | 155,667 reads/s |

### Collections

KVLite and bbolt use native buckets. Redis uses logical key prefixes.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 53,754 keys/s | 81,998 keys/s | 38,713 keys/s |
| write/collections=1/depth=3 | 54,787 keys/s | 60,336 keys/s | 49,432 keys/s |
| write/collections=100/depth=1 | 57,961 keys/s | 46,784 keys/s | 46,288 keys/s |
| write/collections=100/depth=3 | 57,419 keys/s | 30,860 keys/s | 43,071 keys/s |
| read/collections=1/depth=1 | 1,838,241 reads/s | 837,488 reads/s | 158,497 reads/s |
| read/collections=1/depth=3 | 1,387,967 reads/s | 633,938 reads/s | 158,244 reads/s |
| read/collections=100/depth=1 | 1,471,807 reads/s | 826,751 reads/s | 157,784 reads/s |
| read/collections=100/depth=3 | 1,151,866 reads/s | 647,267 reads/s | 155,126 reads/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 1,335,526 keys/s | 1,081,604 keys/s | 849,953 keys/s |
| write/collections=1/depth=3 | 1,451,728 keys/s | 1,131,346 keys/s | 733,996 keys/s |
| write/collections=100/depth=1 | 1,729,360 keys/s | 2,285,457 keys/s | 652,168 keys/s |
| write/collections=100/depth=3 | 1,970,434 keys/s | 2,428,575 keys/s | 513,177 keys/s |

### Latency

The large scale reports one, eight, and 32 clients.

#### Durable

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Read | 1 | KVLite | 291 ns | 334 ns | 459 ns | 396.32 µs |
| Read | 1 | bbolt | 417 ns | 708 ns | 1.29 µs | 2.35 ms |
| Read | 1 | Redis | 5.58 µs | 6.62 µs | 11.17 µs | 4.78 ms |
| Read | 8 | KVLite | 458 ns | 625 ns | 792 ns | 17.97 ms |
| Read | 8 | bbolt | 500 ns | 1.33 µs | 5.33 µs | 20.62 ms |
| Read | 8 | Redis | 26.83 µs | 64.42 µs | 99.67 µs | 9.01 ms |
| Read | 32 | KVLite | 458 ns | 667 ns | 917 ns | 55.81 ms |
| Read | 32 | bbolt | 625 ns | 43.33 µs | 162.38 µs | 30.41 ms |
| Read | 32 | Redis | 58.46 µs | 229.29 µs | 468.40 µs | 11.15 ms |
| Update | 1 | KVLite | 588.21 µs | 2.21 ms | 2.64 ms | 13.54 ms |
| Update | 1 | bbolt | 1.10 ms | 5.03 ms | 8.48 ms | 298.16 ms |
| Update | 1 | Redis | 540.67 µs | 926.41 µs | 1.64 ms | 8.14 ms |
| Update | 8 | KVLite | 593.21 µs | 867.17 µs | 1.30 ms | 5.11 ms |
| Update | 8 | bbolt | 20.91 ms | 24.40 ms | 26.93 ms | 44.84 ms |
| Update | 8 | Redis | 636.67 µs | 1.50 ms | 2.40 ms | 6.81 ms |
| Update | 32 | KVLite | 690.21 µs | 1.11 ms | 1.68 ms | 5.89 ms |
| Update | 32 | bbolt | 24.06 ms | 28.89 ms | 31.92 ms | 36.89 ms |
| Update | 32 | Redis | 1.36 ms | 3.04 ms | 4.27 ms | 10.21 ms |
| Mixed | 1 | KVLite | 542 ns | 283.58 µs | 673.04 µs | 6.89 ms |
| Mixed | 1 | bbolt | 708 ns | 132.75 µs | 1.23 ms | 8.44 ms |
| Mixed | 1 | Redis | 16.21 µs | 710.13 µs | 2.28 ms | 23.42 ms |
| Mixed | 8 | KVLite | 459 ns | 558.08 µs | 781.71 µs | 5.00 ms |
| Mixed | 8 | bbolt | 3.88 µs | 6.34 ms | 20.87 ms | 36.75 ms |
| Mixed | 8 | Redis | 145.62 µs | 2.61 ms | 4.33 ms | 25.06 ms |
| Mixed | 32 | KVLite | 416 ns | 665.23 µs | 1.17 ms | 7.51 ms |
| Mixed | 32 | bbolt | 3.54 µs | 9.62 ms | 24.21 ms | 32.99 ms |
| Mixed | 32 | Redis | 2.33 ms | 4.77 ms | 5.97 ms | 19.26 ms |

#### No commit sync

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Update | 1 | KVLite | 1.58 µs | 3.38 µs | 5.21 µs | 251.50 µs |
| Update | 1 | bbolt | 8.50 µs | 14.42 µs | 20.21 µs | 3.41 ms |
| Update | 1 | Redis | 7.21 µs | 10.41 µs | 14.50 µs | 375.75 µs |
| Update | 8 | KVLite | 1.62 µs | 3.46 µs | 6.17 µs | 6.70 ms |
| Update | 8 | bbolt | 8.62 µs | 16.00 µs | 3.74 ms | 9.56 ms |
| Update | 8 | Redis | 29.42 µs | 65.41 µs | 92.42 µs | 4.07 ms |
| Update | 32 | KVLite | 1.62 µs | 3.58 µs | 2.51 ms | 8.16 ms |
| Update | 32 | bbolt | 8.83 µs | 2.80 ms | 6.40 ms | 10.83 ms |
| Update | 32 | Redis | 106.08 µs | 216.09 µs | 308.33 µs | 3.10 ms |
| Mixed | 1 | KVLite | 292 ns | 1.46 µs | 2.17 µs | 267.92 µs |
| Mixed | 1 | bbolt | 459 ns | 7.62 µs | 10.54 µs | 3.81 ms |
| Mixed | 1 | Redis | 5.54 µs | 7.38 µs | 11.12 µs | 2.55 ms |
| Mixed | 8 | KVLite | 292 ns | 1.50 µs | 3.21 µs | 10.27 ms |
| Mixed | 8 | bbolt | 542 ns | 8.83 µs | 25.38 µs | 16.74 ms |
| Mixed | 8 | Redis | 29.54 µs | 64.41 µs | 95.86 µs | 6.11 ms |
| Mixed | 32 | KVLite | 292 ns | 1.54 µs | 4.67 µs | 13.64 ms |
| Mixed | 32 | bbolt | 542 ns | 10.58 µs | 675.42 µs | 21.42 ms |
| Mixed | 32 | Redis | 65.71 µs | 234.21 µs | 428.35 µs | 7.10 ms |

### Life cycle

Redis does not have the same embedded life-cycle boundary.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| close-after-writes | 2.11 ms | 11.29 µs |
| create-load-close | 45.19 ms | 60.09 ms |
| open-clean | 76.70 µs | 2.96 ms |
| recover-after-process-kill | 2.91 ms | 2.89 ms |

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
| update/random/grow=32-1024/clients=1 | 3,904,042 B | 4,194,304 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 23,491,011 B | 33,640,448 B | 11,443,301 B |
| update/random/grow=128-65536/clients=1 | 36,556,544 B | 50,573,312 B | 65,670,204 B |
| update/random/shrink=1024-32/clients=1 | 41,751,476 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,694,042 B | 35,549,184 B | 11,443,301 B |
| update/random/shrink=65536-128/clients=1 | 35,871,412 B | 51,425,280 B | 33,750,124 B |
| update/random/value=32/clients=1 | 2,031,895 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 4,682,826 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 5,457,034 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 5,421,470 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 5,328,195 B | 8,388,608 B | 3,440,691 B |
| update/random/value=128/clients=8 | 5,198,401 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 5,345,040 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 5,189,641 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 5,231,348 B | 4,194,304 B | 3,443,301 B |
| update/random/value=1024/clients=1 | 42,743,476 B | 35,549,184 B | 11,759,401 B |
| update/random/value=3072/clients=1 | 44,799,668 B | 58,118,144 B | 34,287,401 B |
| update/random/value=65536/clients=1 | 36,232,436 B | 51,425,280 B | 99,160,124 B |
| update/sequential/value=128/clients=1/batch=10 | 5,212,324 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 5,129,249 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 5,122,259 B | 4,194,304 B | 3,440,691 B |
| insert/random/value=128/clients=1 | 3,853,012 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 6,864,316 B | 8,388,608 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 6,668,693 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 6,268,094 B | 8,388,608 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 4,572,442 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 6,332,559 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 4,514,783 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 5,819,590 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=65536/clients=1 | 71,979,152 B | 85,176,320 B | 99,160,124 B |
| insert/sequential/value=128/clients=1 | 3,921,233 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 6,618,150 B | 8,388,608 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,369,455 B | 8,388,608 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,189,230 B | 8,388,608 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 5,374,608 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 5,266,647 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 5,253,507 B | 4,194,304 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 4,260,351 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 4,250,501 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 4,248,531 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=65536/clients=1 | 39,356,106 B | 51,425,280 B | 49,973,624 B |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 3,904,042 B | 2,469,888 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 23,491,011 B | 19,394,560 B | 11,443,301 B |
| update/random/grow=128-65536/clients=1 | 36,556,544 B | 38,391,808 B | 65,670,204 B |
| update/random/shrink=1024-32/clients=1 | 41,751,476 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,694,042 B | 35,549,184 B | 11,443,301 B |
| update/random/shrink=65536-128/clients=1 | 35,871,266 B | 51,425,280 B | 33,750,124 B |
| update/random/value=32/clients=1 | 2,031,895 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 4,682,826 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 5,457,034 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 5,421,470 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 5,328,195 B | 6,082,560 B | 3,440,691 B |
| update/random/value=128/clients=8 | 5,268,510 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 5,421,479 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 5,268,510 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 5,421,461 B | 4,194,304 B | 3,443,301 B |
| update/random/value=1024/clients=1 | 42,743,476 B | 35,549,184 B | 11,759,401 B |
| update/random/value=3072/clients=1 | 44,799,668 B | 58,118,144 B | 34,287,401 B |
| update/random/value=65536/clients=1 | 36,232,436 B | 51,425,280 B | 99,160,124 B |
| update/sequential/value=128/clients=1/batch=10 | 5,212,324 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 5,129,249 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 5,122,259 B | 4,194,304 B | 3,440,691 B |
| insert/random/value=128/clients=1 | 3,853,012 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 6,864,316 B | 5,910,528 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 6,668,693 B | 6,303,744 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 6,268,094 B | 7,663,616 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 4,657,675 B | 4,259,840 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 6,659,123 B | 6,283,264 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 4,656,577 B | 4,263,936 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 6,654,316 B | 6,270,976 B | 3,443,301 B |
| insert/random/value=65536/clients=1 | 71,979,152 B | 73,461,760 B | 99,160,124 B |
| insert/sequential/value=128/clients=1 | 3,921,233 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 6,618,150 B | 6,971,392 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,369,455 B | 6,971,392 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,189,230 B | 6,971,392 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 5,374,608 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 5,374,608 B | 4,571,136 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 5,374,608 B | 4,886,528 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 4,260,351 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 4,260,351 B | 4,198,400 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 4,260,351 B | 4,288,512 B | 1,806,401 B |
| mixed/read=95/value=65536/clients=1 | 39,356,106 B | 51,425,280 B | 49,973,624 B |
