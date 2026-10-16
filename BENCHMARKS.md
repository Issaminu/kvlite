# KVLite benchmarks

This report compares KVLite with bbolt and Redis at the public call boundary. It contains the median of 15 measured runs at the complete `large --workloads=all --storage=volume` scale. The same run then profiled every KVLite case.

| Item | Value |
| --- | --- |
| Run time | `2026-10-16T00:43:49Z` |
| Timing duration | 2 hours 25 minutes 40 seconds |
| Profile capture | 35 minutes 21 seconds, KVLite cases only |
| KVLite | `053ecf90142cba7912a40f00e8e24fbd00eeaf74`; the only untracked file was a Markdown note outside the build |
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
| Point read | Read-only, random, 128-byte value, eight clients | 5,193,199 keys/s | 1,028,809 keys/s | 239,863 keys/s | **KVLite: 5.05x bbolt; 21.65x Redis** |
| Point-read p99 | Read-only, eight clients | 875 ns | 7.29 µs | 101.25 µs | **KVLite: 8.33x advantage over bbolt; 115.72x over Redis** |
| Point update | Durable, random, 128-byte value, one client, batch of 100 | 106,405 keys/s | 28,952 keys/s | 41,787 keys/s | **KVLite: 3.68x bbolt; 2.55x Redis** |
| Point insert | Durable, random, 128-byte value, one client, batch of 100 | 124,087 keys/s | 29,087 keys/s | 41,763 keys/s | **KVLite: 4.27x bbolt; 2.97x Redis** |
| Mixed work | Durable, 95% reads, eight clients | 154,553 operations/s | 8,033 operations/s | 12,088 operations/s | **KVLite: 19.24x bbolt; 12.79x Redis** |
| Mixed work | No commit sync, 95% reads, eight clients | 1,846,318 operations/s | 372,698 operations/s | 235,780 operations/s | **KVLite: 4.95x bbolt; 7.83x Redis** |
| Key delete | No commit sync, random, one client | 387,543 keys/s | 76,185 keys/s | 108,574 keys/s | **KVLite: 5.09x bbolt; 3.57x Redis** |
| Large-key read | Read-only, random, 32 KiB key, one client | 80,425 keys/s | 64,394 keys/s | 42,283 keys/s | **KVLite: 1.25x bbolt; 1.90x Redis** |
| Clean open | Life cycle | 1.54 ms | 2.90 ms | Not comparable | **KVLite: 1.89x advantage over bbolt** |

These ratios apply only to the named workloads and public call boundaries. KVLite did not lead every case. bbolt read 64 KiB values 1.16x faster and 1 MiB values 1.46x faster. Redis led seven of 13 no-commit-sync insert cases. The full tables show every case. In this run, a clean open was 22 times slower than in the previous report. [Changes since the previous report](#changes-since-the-previous-report) gives the cause and a measured fix.

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
- KVLite: `053ecf90142cba7912a40f00e8e24fbd00eeaf74`
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

Setup, fixture loading, final stored-data checks, and close operations were outside the core timers. The named life-cycle cases measure open, close, and recovery work separately. The runner ran each life-cycle case in its own process. Before the timed recovery open, the harness now waits until the killed helper process releases its file lock.

After the 15 timed rounds, the runner profiled every KVLite case again with 15 repeats. It recorded CPU profiles, allocation profiles, and execution traces. The profile pass does not change the timing values in this report. KVLite rows also report WAL commits, WAL bytes, write calls, and storage bytes written per operation.

## Changes since the previous report

The previous report measured `ca4fdae` on 2026-10-15. The host changed between the runs: OrbStack updated its Linux kernel from `7.0.14` to `7.2.9`. bbolt and Redis code did not change, so their changes show host drift. As a geometric mean over 189 cases, KVLite times went down by 11.0% and bbolt times went down by 7.8%.

| Workload group | KVLite time ratio | bbolt time ratio |
| --- | ---: | ---: |
| Durable point inserts | 0.74 | 0.93 |
| No-commit-sync point inserts | 0.66 | 0.87 |
| Durable page reuse, repeated | 0.78 | 0.96 |
| No-commit-sync page reuse, repeated | 0.67 | 0.87 |
| Durable bucket deletion | 0.73 | 0.97 |
| No-commit-sync key deletes | 0.80 | 0.82 |
| No-commit-sync point updates | 0.80 | 0.81 |
| Durable point updates | 1.00 | 0.92 |
| Durable point reads | 0.94 | 0.95 |
| Read transactions | 1.01 | 0.96 |
| Ordered operations | 0.99 | 0.96 |
| Durable reopen after deletion | 32.97 | 1.78 |

A ratio below 1 means a lower time. Each ratio is the geometric mean of the main metric of each case in the group. Paired runs of `ca4fdae` and `053ecf9` in one container, with a same-binary control, checked each group that looked slower:

- A durable open of an existing database is much slower: 22 times in this run, and about 60 times in paired runs. Commit `79abd51` syncs the parent directory on every writable open, and `Close` removes the WAL, so each open creates its name again. The clean open went from 68.7 µs to 1.54 ms, and the durable reopen after deletion from 67 µs to 2.21 ms. A tested patch defers this sync to the first WAL sync of a handle, as SQLite does for a new journal or WAL file. With the patch, the clean open is 20 times faster and the reopen 17 times faster than without it (p < 0.001). Cases with writes did not change.
- In micro benchmarks, point reads are 2% to 6% slower, more for absent keys, and full scans are 3% to 4% slower. A bisection shows the step at commit `acc72ee`, which merged the leaf and branch codecs. The profiles show the extra time in its new header reader, which is not inlined and copies a 32-byte header for each page. A tested patch reads the header of a one-page node with no checksum check in a small inlined function. With the patch, KVBench point reads of absent keys are 22% faster than in this run, point reads of present keys 7% to 19% faster, read transactions of 10 or more keys 10% to 12% faster, and a full forward scan 11% faster (p < 0.05). Large-entry reads and the full reverse scan do not change.
- An open and close without commit sync is 20% slower, about 3 µs. Commits `5e78513` (symbolic link resolution) and `5331110` (a WAL truncate on every writable open) add it.
- Durable point updates and read transactions did not change outside the noise of this host.
- The recovery open after a killed process went from 60.8 ms to 1.92 ms. This change comes from the harness: it now waits until the killed process releases its file lock. Before that fix, most samples measured one 50 ms lock retry.

The WAL format also changed. Inserts, value growth, and deletes now write node patches, and the first change to each page after a checkpoint writes a complete page image:

| Case | WAL bytes per operation, previous report | This run |
| --- | ---: | ---: |
| Random insert, 128-byte value | 3,565 | 367 |
| Sequential insert, 128-byte value | 3,952 | 436 |
| Shrink from 64 KiB to 128 bytes | 2,252 | 171 |
| Random update, 128-byte value | 59 | 1,197 |
| Random update, 3,072-byte value | 59 | 3,172 |

A complete page image lets recovery repair a page that a crash tore during a checkpoint. PostgreSQL uses the same method. In this run, the larger update records did not make no-commit-sync updates slower.

## Interpretation

These statements apply only to this run and its tested workloads.

- KVLite led 95 of 118 durable cases. bbolt led nine. Redis led 14.
- KVLite led 45 of 71 no-commit-sync cases. bbolt led six. Redis led 20.
- Each count uses one main metric for each case: the median time per operation, or p99 for latency cases. It excludes storage-size metrics.
- KVLite led 40 of 43 read cases, including point reads, read transactions, scans, ordered reads, access distributions, collection reads, and read latency. bbolt led 64 KiB value reads, 1 MiB value reads, and `seek-and-read=1000`.
- KVLite led 15 of 21 durable point-update cases and 12 of 13 durable point-insert cases.
- KVLite led 13 of 21 no-commit-sync point-update cases and six of 13 no-commit-sync point-insert cases. Redis led most of the other cases.
- KVLite led durable acknowledged mixed work in six of seven cases. bbolt led the case with 64 KiB values.
- KVLite led 15 of 28 deletion, reuse, and reopen cases across both modes. Redis led 12. bbolt led one.
- KVLite recovered after a killed process faster than bbolt: 1.92 ms against 2.91 ms. bbolt closed after writes 194x faster, because KVLite checkpoints its WAL during close.

## Limits

- This is one run on one container host. It is not a bare-metal Linux result.
- Docker volume storage includes the OrbStack virtual machine and host storage path. It does not isolate physical-device sync latency.
- Durable write times depend on storage sync latency. That latency changed by up to four times between rounds of the same binary.
- The measured commit `053ecf9` is marked as work in progress. Its WAL changes were not reviewed before the run.
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
| read/random/hits=0/misses=above/value=128/clients=1 | 5,027,387 keys/s | 1,789,458 keys/s | 143,888 keys/s |
| read/random/hits=0/misses=below/value=128/clients=1 | 4,946,828 keys/s | 1,972,682 keys/s | 144,568 keys/s |
| read/random/hits=0/misses=between/value=128/clients=1 | 4,321,601 keys/s | 1,865,256 keys/s | 143,695 keys/s |
| read/random/hits=50/value=128/clients=1 | 3,516,039 keys/s | 1,700,950 keys/s | 154,348 keys/s |
| read/random/hits=100/value=128/clients=8 | 5,193,199 keys/s | 1,028,809 keys/s | 239,863 keys/s |
| read/random/hits=100/value=128/clients=32 | 3,848,757 keys/s | 931,698 keys/s | 343,954 keys/s |
| read/random/key=32768/value=128/clients=1 | 80,425 keys/s | 64,394 keys/s | 42,283 keys/s |
| read/random/value=32/clients=1 | 3,089,572 keys/s | 1,370,169 keys/s | 172,124 keys/s |
| read/random/value=128/clients=1 | 2,925,561 keys/s | 1,429,145 keys/s | 168,612 keys/s |
| read/random/value=1024/clients=1 | 1,797,742 keys/s | 1,376,021 keys/s | 160,164 keys/s |
| read/random/value=3072/clients=1 | 1,408,209 keys/s | 1,182,953 keys/s | 133,145 keys/s |
| read/random/value=65536/clients=1 | 205,828 keys/s | 239,184 keys/s | 48,593 keys/s |
| read/random/value=1048576/clients=1 | 18,142 keys/s | 26,432 keys/s | 6,763 keys/s |
| read/sequential/hits=100/value=128/clients=1 | 3,786,606 keys/s | 1,522,639 keys/s | 166,428 keys/s |
| update/random/grow=32-1024/clients=1 | 1,671 keys/s | 880 keys/s | 1,647 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 34,638 keys/s | 14,645 keys/s | 48,790 keys/s |
| update/random/grow=128-65536/clients=1 | 653 keys/s | 879 keys/s | 726 keys/s |
| update/random/shrink=1024-32/clients=1 | 1,918 keys/s | 942 keys/s | 1,979 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 50,589 keys/s | 12,922 keys/s | 38,771 keys/s |
| update/random/shrink=65536-128/clients=1 | 1,839 keys/s | 980 keys/s | 1,891 keys/s |
| update/random/value=32/clients=1 | 1,474 keys/s | 822 keys/s | 1,493 keys/s |
| update/random/value=128/clients=1 | 1,818 keys/s | 978 keys/s | 1,458 keys/s |
| update/random/value=128/clients=1/batch=10 | 19,723 keys/s | 5,577 keys/s | 12,503 keys/s |
| update/random/value=128/clients=1/batch=100 | 106,405 keys/s | 28,952 keys/s | 41,787 keys/s |
| update/random/value=128/clients=1/batch=1000 | 564,815 keys/s | 117,205 keys/s | 281,402 keys/s |
| update/random/value=128/clients=8 | 14,082 keys/s | 379 keys/s | 10,271 keys/s |
| update/random/value=128/clients=8/batch=100 | 265,145 keys/s | 21,548 keys/s | 215,221 keys/s |
| update/random/value=128/clients=32 | 41,416 keys/s | 1,325 keys/s | 13,926 keys/s |
| update/random/value=128/clients=32/batch=100 | 476,347 keys/s | 21,367 keys/s | 429,143 keys/s |
| update/random/value=1024/clients=1 | 1,829 keys/s | 944 keys/s | 1,435 keys/s |
| update/random/value=3072/clients=1 | 1,602 keys/s | 925 keys/s | 1,391 keys/s |
| update/random/value=65536/clients=1 | 763 keys/s | 886 keys/s | 1,126 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 17,994 keys/s | 8,096 keys/s | 12,526 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 153,021 keys/s | 70,167 keys/s | 43,238 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 886,768 keys/s | 423,827 keys/s | 334,092 keys/s |
| insert/random/value=128/clients=1 | 2,019 keys/s | 989 keys/s | 1,920 keys/s |
| insert/random/value=128/clients=1/batch=10 | 18,348 keys/s | 5,609 keys/s | 12,894 keys/s |
| insert/random/value=128/clients=1/batch=100 | 124,087 keys/s | 29,087 keys/s | 41,763 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 596,948 keys/s | 229,331 keys/s | 351,126 keys/s |
| insert/random/value=128/clients=8 | 13,694 keys/s | 403 keys/s | 8,963 keys/s |
| insert/random/value=128/clients=8/batch=100 | 252,352 keys/s | 28,685 keys/s | 200,720 keys/s |
| insert/random/value=128/clients=32 | 37,842 keys/s | 1,444 keys/s | 9,910 keys/s |
| insert/random/value=128/clients=32/batch=100 | 421,287 keys/s | 29,121 keys/s | 409,932 keys/s |
| insert/random/value=65536/clients=1 | 1,044 keys/s | 854 keys/s | 1,115 keys/s |
| insert/sequential/value=128/clients=1 | 2,066 keys/s | 967 keys/s | 1,967 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 17,397 keys/s | 8,732 keys/s | 13,090 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 146,011 keys/s | 83,910 keys/s | 40,360 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 893,448 keys/s | 293,049 keys/s | 321,892 keys/s |
| mixed/read=50/value=128/clients=1 | 3,577 operations/s | 1,559 operations/s | 3,503 operations/s |
| mixed/read=50/value=128/clients=8 | 24,159 operations/s | 766 operations/s | 11,153 operations/s |
| mixed/read=50/value=128/clients=32 | 65,068 operations/s | 2,686 operations/s | 28,457 operations/s |
| mixed/read=95/value=128/clients=1 | 36,152 operations/s | 9,799 operations/s | 9,730 operations/s |
| mixed/read=95/value=128/clients=8 | 154,553 operations/s | 8,033 operations/s | 12,088 operations/s |
| mixed/read=95/value=128/clients=32 | 333,778 operations/s | 25,710 operations/s | 16,973 operations/s |
| mixed/read=95/value=65536/clients=1 | 11,751 operations/s | 14,896 operations/s | 7,318 operations/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 208,201 keys/s | 88,864 keys/s | 110,538 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 270,334 keys/s | 258,612 keys/s | 642,922 keys/s |
| update/random/grow=128-65536/clients=1 | 29,797 keys/s | 31,561 keys/s | 29,383 keys/s |
| update/random/shrink=1024-32/clients=1 | 391,857 keys/s | 86,069 keys/s | 93,639 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 608,926 keys/s | 303,607 keys/s | 1,023,826 keys/s |
| update/random/shrink=65536-128/clients=1 | 144,046 keys/s | 46,521 keys/s | 127,525 keys/s |
| update/random/value=32/clients=1 | 414,583 keys/s | 61,803 keys/s | 100,301 keys/s |
| update/random/value=128/clients=1 | 333,323 keys/s | 80,394 keys/s | 92,236 keys/s |
| update/random/value=128/clients=1/batch=10 | 839,316 keys/s | 194,530 keys/s | 613,832 keys/s |
| update/random/value=128/clients=1/batch=100 | 947,687 keys/s | 417,558 keys/s | 1,204,342 keys/s |
| update/random/value=128/clients=1/batch=1000 | 1,282,214 keys/s | 617,784 keys/s | 1,393,916 keys/s |
| update/random/value=128/clients=8 | 296,149 keys/s | 55,562 keys/s | 193,262 keys/s |
| update/random/value=128/clients=8/batch=100 | 667,609 keys/s | 335,778 keys/s | 1,140,494 keys/s |
| update/random/value=128/clients=32 | 249,315 keys/s | 53,036 keys/s | 194,967 keys/s |
| update/random/value=128/clients=32/batch=100 | 730,775 keys/s | 366,092 keys/s | 1,392,601 keys/s |
| update/random/value=1024/clients=1 | 283,696 keys/s | 90,735 keys/s | 93,135 keys/s |
| update/random/value=3072/clients=1 | 216,872 keys/s | 87,695 keys/s | 83,563 keys/s |
| update/random/value=65536/clients=1 | 25,130 keys/s | 37,864 keys/s | 33,742 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 1,903,627 keys/s | 612,244 keys/s | 559,879 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 3,159,369 keys/s | 2,255,819 keys/s | 1,186,597 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 3,579,402 keys/s | 2,259,570 keys/s | 1,447,967 keys/s |
| insert/random/value=128/clients=1 | 487,848 keys/s | 74,687 keys/s | 96,076 keys/s |
| insert/random/value=128/clients=1/batch=10 | 499,550 keys/s | 163,233 keys/s | 463,774 keys/s |
| insert/random/value=128/clients=1/batch=100 | 609,190 keys/s | 316,643 keys/s | 851,770 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 962,598 keys/s | 877,123 keys/s | 979,193 keys/s |
| insert/random/value=128/clients=8 | 179,673 keys/s | 40,193 keys/s | 181,730 keys/s |
| insert/random/value=128/clients=8/batch=100 | 354,737 keys/s | 226,455 keys/s | 1,002,309 keys/s |
| insert/random/value=128/clients=32 | 196,123 keys/s | 37,086 keys/s | 217,689 keys/s |
| insert/random/value=128/clients=32/batch=100 | 519,849 keys/s | 302,113 keys/s | 942,613 keys/s |
| insert/random/value=65536/clients=1 | 23,627 keys/s | 24,804 keys/s | 32,435 keys/s |
| insert/sequential/value=128/clients=1 | 474,549 keys/s | 63,772 keys/s | 108,881 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 912,304 keys/s | 420,825 keys/s | 479,861 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 2,575,722 keys/s | 1,613,601 keys/s | 830,426 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 3,672,953 keys/s | 2,353,737 keys/s | 957,935 keys/s |
| mixed/read=50/value=128/clients=1 | 730,468 operations/s | 153,066 operations/s | 139,616 operations/s |
| mixed/read=50/value=128/clients=8 | 531,026 operations/s | 85,257 operations/s | 222,464 operations/s |
| mixed/read=50/value=128/clients=32 | 518,072 operations/s | 75,231 operations/s | 240,616 operations/s |
| mixed/read=95/value=128/clients=1 | 1,802,198 operations/s | 673,036 operations/s | 153,648 operations/s |
| mixed/read=95/value=128/clients=8 | 1,846,318 operations/s | 372,698 operations/s | 235,780 operations/s |
| mixed/read=95/value=128/clients=32 | 1,756,946 operations/s | 356,405 operations/s | 267,263 operations/s |
| mixed/read=95/value=65536/clients=1 | 179,269 operations/s | 199,245 operations/s | 49,504 operations/s |

### Key deletion: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 1,719 ack-keys/s | 578 ack-keys/s | 1,775 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 6,574,660 B | 4,194,304 B | 2,080,401 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 79,796 ack-keys/s | 40,743 ack-keys/s | 140,401 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 6,199,232 B | 4,194,304 B | 1,951,901 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 590,188 ack-keys/s | 243,981 ack-keys/s | 479,717 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,778,787 B | 8,388,608 B | 1,950,561 B |
| `existing/random/clients=8` | `ack-keys/s` | 11,652 ack-keys/s | 394 ack-keys/s | 11,046 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 6,340,531 B | 4,194,304 B | 2,080,401 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 1,263 ack-keys/s | 844 ack-keys/s | 1,715 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 39,177,880 B | 51,425,280 B | 33,587,340 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 1,716 ack-keys/s | 903 ack-keys/s | 969 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 5,834,801 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 155,604 ack-keys/s | 90,512 ack-keys/s | 76,744 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 3,739,438 B | 4,194,304 B | 1,951,901 B |
| `missing/random/clients=1` | `ack-keys/s` | 355,122 ack-keys/s | 818 ack-keys/s | 154,709 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,485,696 B | 4,194,304 B | 1,720,401 B |

### Key deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 387,543 ack-keys/s | 76,185 ack-keys/s | 108,574 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 6,574,660 B | 4,194,304 B | 2,080,401 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 858,254 ack-keys/s | 473,629 ack-keys/s | 2,709,826 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 6,199,232 B | 4,194,304 B | 1,951,901 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 1,686,507 ack-keys/s | 853,496 ack-keys/s | 3,226,287 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,778,787 B | 6,045,696 B | 1,950,561 B |
| `existing/random/clients=8` | `ack-keys/s` | 271,732 ack-keys/s | 58,382 ack-keys/s | 223,626 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 6,573,872 B | 4,194,304 B | 2,080,401 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 23,146 ack-keys/s | 26,039 ack-keys/s | 119,312 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 39,112,345 B | 51,425,280 B | 33,587,340 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 420,815 ack-keys/s | 81,334 ack-keys/s | 125,054 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 5,834,801 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 3,868,216 ack-keys/s | 2,568,220 ack-keys/s | 2,660,393 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 3,739,438 B | 4,194,304 B | 1,951,901 B |
| `missing/random/clients=1` | `ack-keys/s` | 3,119,202 ack-keys/s | 237,640 ack-keys/s | 173,827 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,485,696 B | 4,194,304 B | 1,720,401 B |

### Bucket deletion: durable

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 1,786 buckets/s | 906 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 3,734,992 B | 4,194,304 B |

### Bucket deletion: no-commit-sync

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 98,762 buckets/s | 71,975 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 3,734,992 B | 4,194,304 B |

### Read after delete: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 631.92 µs | 1.22 ms | 595.37 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 1.17 µs | 1.42 µs | 13.67 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 1.54 µs | 2.42 µs | 26.54 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 1.83 µs | 6.42 µs | 33.75 µs |

### Read after delete: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 2.59 µs | 21.32 µs | 7.16 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 292 ns | 583 ns | 6.58 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 375 ns | 2.00 µs | 8.46 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 541 ns | 2.96 µs | 13.54 µs |

### Page reuse: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 73,034 cycle-keys/s | 22,213 cycle-keys/s | 112,882 cycle-keys/s |
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
| `random-half/batch=100` | `cycle-keys/s` | 473,638 cycle-keys/s | 353,928 cycle-keys/s | 1,054,472 cycle-keys/s |
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
| `ten-cycles/batch=100` | `cycle-keys/s` | 41,670 cycle-keys/s | 22,195 cycle-keys/s | 76,926 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 12,638,038 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 10,360,028 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,679 | — | — |

### Repeated page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 684,832 cycle-keys/s | 494,551 cycle-keys/s | 1,493,572 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 12,322,505 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 10,360,028 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,679 | — | — |

### Tail reclamation: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 45,348 ack-keys/s | 57,221 ack-keys/s | 51,665 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,013,230 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 1.87 ms | 76.71 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 1.42 ms | 1.34 ms | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Tail reclamation: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 1,870,751 ack-keys/s | 1,768,746 ack-keys/s | 2,570,095 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,013,230 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 127.82 µs | 64.41 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 25.83 µs | 41.83 µs | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Reopen after deletion: durable

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `alternating-half` | `reopen-ns` | 2.21 ms | 2.39 ms |

### Reopen after deletion: no-commit-sync

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `alternating-half` | `reopen-ns` | 34.63 µs | 36.37 µs |

### Transactions

#### Read-only

| Keys per transaction | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| 1 | 1,511,240 keys/s | 1,104,887 keys/s | 154,281 keys/s |
| 10 | 2,506,693 keys/s | 1,849,325 keys/s | 927,245 keys/s |
| 100 | 2,627,920 keys/s | 1,986,976 keys/s | 2,050,087 keys/s |
| 1,000 | 2,620,384 keys/s | 2,005,714 keys/s | 2,412,750 keys/s |

#### Durable mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 12,803 operations/s | 2,243 operations/s | 9,474 operations/s |
| 95% | 100 | 133,285 operations/s | 24,824 operations/s | 142,596 operations/s |
| 95% | 1,000 | 768,109 operations/s | 172,120 operations/s | 570,358 operations/s |
| 50% | 10 | 15,154 operations/s | 3,381 operations/s | 14,884 operations/s |
| 50% | 100 | 127,947 operations/s | 26,391 operations/s | 87,324 operations/s |
| 50% | 1,000 | 580,512 operations/s | 123,718 operations/s | 338,593 operations/s |

#### No-commit-sync mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 1,212,527 operations/s | 666,621 operations/s | 575,832 operations/s |
| 95% | 100 | 1,619,425 operations/s | 1,349,392 operations/s | 1,232,920 operations/s |
| 95% | 1,000 | 1,868,849 operations/s | 1,652,067 operations/s | 1,081,725 operations/s |
| 50% | 10 | 1,074,815 operations/s | 320,713 operations/s | 454,449 operations/s |
| 50% | 100 | 1,368,160 operations/s | 605,573 operations/s | 860,507 operations/s |
| 50% | 1,000 | 1,464,264 operations/s | 845,117 operations/s | 1,028,756 operations/s |

### Enumeration

The 0% case returns no entries. This report omits its entries-per-second value because that rate does not apply.

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| selectivity=1 | 106,126,852 entries/s | 70,414,135 entries/s | 159,540 entries/s |
| selectivity=10 | 127,488,349 entries/s | 108,771,485 entries/s | 932,750 entries/s |
| selectivity=100 | 211,070,398 entries/s | 100,477,003 entries/s | 1,699,314 entries/s |

### Ordered operations

Redis does not provide the required ordered-cursor API. The 0% range case returns no entries. This report omits its entries-per-second value.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| full-forward | 217,233,746 entries/s | 103,424,082 entries/s |
| full-reverse | 243,090,987 entries/s | 110,720,593 entries/s |
| range/selectivity=1 | 107,665,475 entries/s | 68,613,993 entries/s |
| range/selectivity=10 | 130,065,536 entries/s | 98,213,944 entries/s |
| range/selectivity=100 | 213,612,723 entries/s | 101,979,463 entries/s |
| seek-and-read=1 | 2,372,044 entries/s | 997,369 entries/s |
| seek-and-read=10 | 19,227,574 entries/s | 8,709,608 entries/s |
| seek-and-read=100 | 64,980,184 entries/s | 55,710,285 entries/s |
| seek-and-read=1000 | 86,173,527 entries/s | 96,551,518 entries/s |

### Access distribution

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| records=10000/hot-80-20 | 2,923,894 reads/s | 959,409 reads/s | 165,055 reads/s |
| records=10000/uniform | 2,833,345 reads/s | 960,726 reads/s | 164,476 reads/s |
| records=100000/hot-80-20 | 1,993,977 reads/s | 1,406,406 reads/s | 157,878 reads/s |
| records=100000/uniform | 1,694,943 reads/s | 1,211,390 reads/s | 155,510 reads/s |

### Collections

KVLite and bbolt use native buckets. Redis uses logical key prefixes.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 60,783 keys/s | 82,588 keys/s | 37,644 keys/s |
| write/collections=1/depth=3 | 60,541 keys/s | 83,901 keys/s | 46,362 keys/s |
| write/collections=100/depth=1 | 53,609 keys/s | 49,962 keys/s | 50,890 keys/s |
| write/collections=100/depth=3 | 65,921 keys/s | 45,643 keys/s | 44,123 keys/s |
| read/collections=1/depth=1 | 1,743,762 reads/s | 838,927 reads/s | 159,563 reads/s |
| read/collections=1/depth=3 | 1,258,850 reads/s | 633,810 reads/s | 156,726 reads/s |
| read/collections=100/depth=1 | 1,464,603 reads/s | 830,447 reads/s | 156,479 reads/s |
| read/collections=100/depth=3 | 1,068,043 reads/s | 657,133 reads/s | 154,149 reads/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 1,267,866 keys/s | 1,334,172 keys/s | 950,292 keys/s |
| write/collections=1/depth=3 | 1,503,243 keys/s | 1,073,609 keys/s | 742,595 keys/s |
| write/collections=100/depth=1 | 1,710,792 keys/s | 2,317,217 keys/s | 640,172 keys/s |
| write/collections=100/depth=3 | 2,306,265 keys/s | 2,361,251 keys/s | 556,828 keys/s |

### Latency

The large scale reports one, eight, and 32 clients.

#### Durable

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Read | 1 | KVLite | 334 ns | 417 ns | 542 ns | 376.13 µs |
| Read | 1 | bbolt | 417 ns | 708 ns | 1.42 µs | 2.20 ms |
| Read | 1 | Redis | 5.62 µs | 6.71 µs | 11.71 µs | 4.36 ms |
| Read | 8 | KVLite | 500 ns | 667 ns | 875 ns | 18.67 ms |
| Read | 8 | bbolt | 541 ns | 1.42 µs | 7.29 µs | 19.96 ms |
| Read | 8 | Redis | 26.96 µs | 65.17 µs | 101.25 µs | 8.53 ms |
| Read | 32 | KVLite | 500 ns | 667 ns | 834 ns | 71.26 ms |
| Read | 32 | bbolt | 667 ns | 48.79 µs | 164.63 µs | 27.71 ms |
| Read | 32 | Redis | 58.79 µs | 229.17 µs | 458.08 µs | 12.13 ms |
| Update | 1 | KVLite | 599.41 µs | 1.11 ms | 2.05 ms | 11.92 ms |
| Update | 1 | bbolt | 1.06 ms | 1.61 ms | 2.89 ms | 15.78 ms |
| Update | 1 | Redis | 588.88 µs | 842.38 µs | 1.90 ms | 6.81 ms |
| Update | 8 | KVLite | 643.28 µs | 934.25 µs | 1.28 ms | 6.51 ms |
| Update | 8 | bbolt | 21.22 ms | 25.26 ms | 27.69 ms | 47.16 ms |
| Update | 8 | Redis | 624.46 µs | 1.59 ms | 2.29 ms | 6.14 ms |
| Update | 32 | KVLite | 696.17 µs | 1.07 ms | 1.66 ms | 5.06 ms |
| Update | 32 | bbolt | 24.14 ms | 28.77 ms | 31.39 ms | 33.97 ms |
| Update | 32 | Redis | 1.36 ms | 2.91 ms | 3.87 ms | 6.80 ms |
| Mixed | 1 | KVLite | 625 ns | 368.25 µs | 692.11 µs | 6.97 ms |
| Mixed | 1 | bbolt | 708 ns | 417.75 µs | 1.21 ms | 10.36 ms |
| Mixed | 1 | Redis | 12.54 µs | 678.54 µs | 2.21 ms | 18.81 ms |
| Mixed | 8 | KVLite | 542 ns | 608.54 µs | 822.50 µs | 11.94 ms |
| Mixed | 8 | bbolt | 3.79 µs | 6.56 ms | 21.55 ms | 34.77 ms |
| Mixed | 8 | Redis | 128.96 µs | 2.53 ms | 3.78 ms | 63.58 ms |
| Mixed | 32 | KVLite | 499 ns | 767.83 µs | 1.32 ms | 14.08 ms |
| Mixed | 32 | bbolt | 3.12 µs | 9.14 ms | 24.40 ms | 32.48 ms |
| Mixed | 32 | Redis | 1.92 ms | 4.23 ms | 5.31 ms | 16.56 ms |

#### No commit sync

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Update | 1 | KVLite | 1.62 µs | 3.50 µs | 5.50 µs | 276.46 µs |
| Update | 1 | bbolt | 8.42 µs | 14.38 µs | 20.96 µs | 4.13 ms |
| Update | 1 | Redis | 7.12 µs | 8.83 µs | 13.83 µs | 482.60 µs |
| Update | 8 | KVLite | 1.67 µs | 3.71 µs | 6.58 µs | 7.08 ms |
| Update | 8 | bbolt | 8.58 µs | 15.54 µs | 3.72 ms | 10.20 ms |
| Update | 8 | Redis | 29.34 µs | 66.20 µs | 96.27 µs | 3.49 ms |
| Update | 32 | KVLite | 1.62 µs | 3.71 µs | 2.70 ms | 8.42 ms |
| Update | 32 | bbolt | 8.71 µs | 2.60 ms | 6.61 ms | 11.44 ms |
| Update | 32 | Redis | 103.92 µs | 209.39 µs | 274.41 µs | 3.00 ms |
| Mixed | 1 | KVLite | 375 ns | 1.46 µs | 2.04 µs | 242.22 µs |
| Mixed | 1 | bbolt | 459 ns | 7.63 µs | 10.63 µs | 4.18 ms |
| Mixed | 1 | Redis | 5.58 µs | 7.38 µs | 11.42 µs | 2.84 ms |
| Mixed | 8 | KVLite | 375 ns | 1.54 µs | 3.46 µs | 8.24 ms |
| Mixed | 8 | bbolt | 542 ns | 9.33 µs | 26.17 µs | 16.62 ms |
| Mixed | 8 | Redis | 30.79 µs | 62.96 µs | 96.17 µs | 6.71 ms |
| Mixed | 32 | KVLite | 375 ns | 1.58 µs | 4.00 µs | 17.07 ms |
| Mixed | 32 | bbolt | 542 ns | 11.42 µs | 529.50 µs | 22.54 ms |
| Mixed | 32 | Redis | 61.71 µs | 226.63 µs | 391.38 µs | 6.94 ms |

### Life cycle

Redis does not have the same embedded life-cycle boundary.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| close-after-writes | 1.99 ms | 10.25 µs |
| create-load-close | 50.47 ms | 62.25 ms |
| open-clean | 1.54 ms | 2.90 ms |
| recover-after-process-kill | 1.92 ms | 2.91 ms |

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
| update/random/value=128/clients=8 | 5,198,426 B | 4,194,304 B | 2,270,801 B |
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
| insert/random/value=128/clients=8 | 4,570,347 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 6,332,559 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 4,515,868 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 5,826,544 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=65536/clients=1 | 71,979,152 B | 85,176,320 B | 99,160,124 B |
| insert/sequential/value=128/clients=1 | 3,921,233 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 6,618,150 B | 8,388,608 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,369,455 B | 8,388,608 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,189,230 B | 8,388,608 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 5,374,608 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 5,266,640 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 5,253,514 B | 4,194,304 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 4,260,351 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 4,250,426 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 4,248,563 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=65536/clients=1 | 39,356,106 B | 51,425,280 B | 49,973,624 B |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 3,904,042 B | 2,469,888 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 23,491,011 B | 19,394,560 B | 11,443,301 B |
| update/random/grow=128-65536/clients=1 | 36,556,544 B | 38,391,808 B | 65,670,204 B |
| update/random/shrink=1024-32/clients=1 | 41,751,476 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,694,042 B | 35,549,184 B | 11,443,301 B |
| update/random/shrink=65536-128/clients=1 | 35,871,558 B | 51,425,280 B | 33,750,124 B |
| update/random/value=32/clients=1 | 2,031,895 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 4,682,826 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 5,457,034 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 5,421,470 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 5,328,195 B | 6,082,560 B | 3,440,691 B |
| update/random/value=128/clients=8 | 5,268,510 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 5,421,479 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 5,268,510 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 5,421,443 B | 4,194,304 B | 3,443,301 B |
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
| insert/random/value=128/clients=8 | 4,654,352 B | 4,263,936 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 6,664,828 B | 6,283,264 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 4,657,327 B | 4,263,936 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 6,658,860 B | 6,279,168 B | 3,443,301 B |
| insert/random/value=65536/clients=1 | 71,979,152 B | 73,461,760 B | 99,160,124 B |
| insert/sequential/value=128/clients=1 | 3,921,233 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 6,618,150 B | 6,971,392 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,369,455 B | 6,971,392 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,189,230 B | 6,971,392 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 5,374,608 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 5,374,608 B | 4,845,568 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 5,374,608 B | 4,988,928 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 4,260,351 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 4,260,351 B | 4,530,176 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 4,260,351 B | 4,960,256 B | 1,806,401 B |
| mixed/read=95/value=65536/clients=1 | 39,356,106 B | 51,425,280 B | 49,973,624 B |
