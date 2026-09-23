# KVLite benchmarks

This report compares KVLite with bbolt and Redis at the public call boundary. It contains the median of 15 measured runs from the complete `large --workloads=all --storage=volume` profile.

| Item | Value |
| --- | --- |
| Run time | `2026-09-22T23:52:19Z` |
| Elapsed time | 1 hour 41 minutes 29.95 seconds |
| KVLite | `31648b340b70f83355fcfbd8af4ed23427bf9055` from a clean working tree |
| bbolt | `v1.4.3` |
| Redis | `8.8.0` |
| Platform | Linux `arm64`, four CPUs in containers |
| Profile | `large --workloads=all --storage=volume` |
| Warm-up | One complete unrecorded round |
| Measurement | 15 measured rounds |

## Results at a glance

A higher throughput value is better. A lower latency or life-cycle value is better. This section highlights important operations where KVLite led this run.

| Category | Mode and workload | KVLite | bbolt | Redis | Highlight |
| --- | --- | ---: | ---: | ---: | --- |
| Point read | Read-only, random, 128-byte value, eight clients | 3,389,456 keys/s | 756,987 keys/s | 185,680 keys/s | **KVLite: 4.48x bbolt; 18.25x Redis** |
| Point-read p99 | Read-only, eight clients | 1.17 µs | 8.54 µs | 174.75 µs | **KVLite: 7.33x advantage over bbolt; 149.87x over Redis** |
| Point insert | Durable, random, 128-byte value, eight clients | 8,239 keys/s | 509 keys/s | 6,428 keys/s | **KVLite: 1.28x Redis; 16.18x bbolt** |
| Mixed work | Durable, 95% reads, eight clients | 70,918 operations/s | 10,326 operations/s | 16,238 operations/s | **KVLite: 4.37x Redis; 6.87x bbolt** |
| Point update | No commit sync, random, one client | 189,772 keys/s | 66,340 keys/s | 99,344 keys/s | **KVLite: 1.91x Redis; 2.86x bbolt** |
| Mixed work | No commit sync, 95% reads, eight clients | 856,196 operations/s | 310,903 operations/s | 171,369 operations/s | **KVLite: 2.75x bbolt; 5.00x Redis** |
| Clean open | Life cycle | 69.08 µs | 2.48 ms | Not comparable | **KVLite: 35.91x advantage** |

These ratios apply only to the named workloads and public call boundaries.

## Scope

This run measured every workload group and every case variant in the large profile. It covers point operations, transactions, enumeration, ordered operations, access distributions, latency, collections, storage size, and embedded life-cycle operations.

This report uses one complete run. It does not reuse values from an older run.

## Fairness and API boundaries

The runner gave each engine the same fixed operation schedule and data set. It ran the engines serially. It changed the engine order and durability-mode order between measured rounds. Each engine used the same four-CPU set and Docker storage class.

| Logical operation | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| Point read | `DB.Get` | One `DB.View` transaction | `GET` |
| Grouped read | One read transaction | One read transaction | `MGET` |
| Point write, one client | `DB.Put` | `DB.Update` | `SET` |
| Point write, many clients | `DB.Put` | `DB.Batch` | `SET` |
| Batch write | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Mixed transaction | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Prefix enumeration | Ordered prefix cursor | Ordered prefix cursor | `SCAN` and `MGET` |

bbolt does not provide a direct `DB.Get` call. Use the grouped read results when one transaction must contain several reads. Redis includes its client, server, and local network path. These are public call comparisons. They do not compare identical internal operations.

## Durability modes

| Mode | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| Durable | `SyncFull` | `NoSync=false`; `NoGrowSync=false`; `NoFreelistSync=false` | AOF with `appendfsync always` |
| No commit sync | `SyncNone` | `NoSync=true`; `NoGrowSync=true`; `NoFreelistSync=false` | AOF with `appendfsync no` |

A successful durable write waits for the durability boundary of its engine. In no-commit-sync mode, no engine asks storage to sync during commit. Durable and no-commit-sync results have different guarantees. This report keeps them separate.

## Test environment

- CPU set: `0-3`
- Go: `go1.26.7 linux/arm64`
- KVLite: `31648b340b70f83355fcfbd8af4ed23427bf9055` from a clean working tree
- bbolt: `v1.4.3`
- Redis client: `go-redis v9.22.0`
- Redis server: `8.8.0`, `jemalloc-5.3.0`
- Container system: OrbStack with Linux kernel `7.0.14-orbstack-00380-ga7e0a2dc9535`
- Storage driver: `overlay2`
- Host CPUs visible to the container system: 14
- CPUs assigned to each measured process and its Redis server: four
- Benchmark data storage: temporary Docker volume

The Docker volume path includes the OrbStack virtual machine and the host storage path. It does not prove bare-metal device performance. Other containers were active on the host. CPU assignment reduced direct CPU competition. Other host work could still affect the results.

## Method

The runner first completed its correctness tests. It then completed one unrecorded warm-up round. It created a new database fixture for each benchmark process. It did not reuse a measured database from the warm-up round.

The runner recorded 15 fixed-work rounds. It changed the engine order and durability-mode order between rounds. Pure reads ran once per round because commit-sync settings do not affect them.

The large profile uses 10,000 records for its main cases. It uses 100,000 point reads, 1,000 single-client point writes, 3,200 concurrent point writes, and 10,000 mixed operations. Read-latency cases use 1,000,000 recorded operations and 10,000 unrecorded fixture warm-up reads. Update-latency cases use 10,000 operations. Mixed-latency cases use 100,000 operations.

Setup, fixture loading, final stored-data checks, and close operations were outside the core timers. The named life-cycle cases measure open, close, and recovery work separately.

## Interpretation

These statements apply only to this run and its tested workloads.

- KVLite led every point-read throughput case.
- KVLite had the lowest point-read p99 latency at eight clients. Its result gave it a 7.33x advantage over bbolt and a 149.87x advantage over Redis.
- KVLite led durable acknowledged mixed work at all tested client counts and read shares.
- KVLite led no-commit-sync acknowledged mixed work in all tested client cases. It led most single-key update cases. Redis led most random concurrent insert and batch cases.
- KVLite led the durable random point-insert case at eight clients. It was 1.28x faster than Redis and 16.18x faster than bbolt.
- KVLite and bbolt each led some ordered-operation cases. KVLite led the 1% enumeration case. bbolt led the 10% and 100% cases.
- KVLite opened a clean database 35.91x faster than bbolt.

## Limits

- This is one run on one container host. It is not a bare-metal Linux result.
- Docker volume storage includes the OrbStack virtual machine and host storage path. It does not isolate physical-device sync latency.
- The report contains medians from 15 measured rounds. It does not contain a confidence interval or a significance test.
- Some durable write, transaction, and latency cases had high round-to-round variation. Treat their medians as environment-specific signals, not exact limits.
- A ratio from median values does not prove a statistically significant difference.
- Read tests use a warm operating-system cache. The suite does not claim cold-cache performance.
- The suite does not rank combined CPU use or peak resident memory across embedded and client-server designs.
- Redis has no result for ordered cursor or embedded life-cycle operations.
- Persistent byte counts cover different file designs and maintenance rules. They are not a direct storage-efficiency ranking.

## Reproduce

```sh
cd benchmarks/kvbench
KVBENCH_RESULTS_DIR=/tmp/kvlite-kvbench-large-all-volume \
./run-docker.sh large --workloads=all --storage=volume
```

The runner uses pinned Go and Redis container images. The Go module pins bbolt and the Redis client. Change the results directory for a new run.

## Full results

Every value is the median of 15 measured runs.

### Acknowledged operations

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| read/random/hits=0/misses=above/value=128/clients=1 | 3,731,598 keys/s | 1,500,597 keys/s | 120,822 keys/s |
| read/random/hits=0/misses=below/value=128/clients=1 | 3,493,400 keys/s | 1,571,013 keys/s | 120,415 keys/s |
| read/random/hits=0/misses=between/value=128/clients=1 | 2,939,180 keys/s | 1,454,428 keys/s | 120,886 keys/s |
| read/random/hits=50/value=128/clients=1 | 2,347,652 keys/s | 1,377,567 keys/s | 128,106 keys/s |
| read/random/hits=100/value=128/clients=8 | 3,389,456 keys/s | 756,987 keys/s | 185,680 keys/s |
| read/random/hits=100/value=128/clients=32 | 2,936,331 keys/s | 831,496 keys/s | 246,648 keys/s |
| read/random/value=32/clients=1 | 2,194,798 keys/s | 1,239,044 keys/s | 143,812 keys/s |
| read/random/value=128/clients=1 | 1,958,304 keys/s | 1,154,393 keys/s | 138,318 keys/s |
| read/random/value=1024/clients=1 | 1,216,689 keys/s | 1,120,165 keys/s | 125,339 keys/s |
| read/random/value=3072/clients=1 | 999,913 keys/s | 921,815 keys/s | 104,186 keys/s |
| read/sequential/hits=100/value=128/clients=1 | 2,568,369 keys/s | 1,104,914 keys/s | 137,616 keys/s |
| update/random/grow=32-1024/clients=1 | 910 keys/s | 676 keys/s | 1,307 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 22,465 keys/s | 12,622 keys/s | 42,716 keys/s |
| update/random/shrink=1024-32/clients=1 | 1,582 keys/s | 757 keys/s | 1,562 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 40,406 keys/s | 12,129 keys/s | 62,190 keys/s |
| update/random/value=32/clients=1 | 864 keys/s | 494 keys/s | 1,574 keys/s |
| update/random/value=128/clients=1 | 1,126 keys/s | 676 keys/s | 1,564 keys/s |
| update/random/value=128/clients=1/batch=10 | 9,174 keys/s | 4,615 keys/s | 11,195 keys/s |
| update/random/value=128/clients=1/batch=100 | 48,584 keys/s | 19,738 keys/s | 63,795 keys/s |
| update/random/value=128/clients=1/batch=1000 | 185,919 keys/s | 98,902 keys/s | 330,760 keys/s |
| update/random/value=128/clients=8 | 8,623 keys/s | 502 keys/s | 8,619 keys/s |
| update/random/value=128/clients=8/batch=100 | 136,167 keys/s | 19,390 keys/s | 191,943 keys/s |
| update/random/value=128/clients=32 | 22,789 keys/s | 1,867 keys/s | 17,358 keys/s |
| update/random/value=128/clients=32/batch=100 | 270,032 keys/s | 18,843 keys/s | 429,871 keys/s |
| update/random/value=1024/clients=1 | 1,291 keys/s | 752 keys/s | 1,379 keys/s |
| update/random/value=3072/clients=1 | 1,035 keys/s | 705 keys/s | 1,365 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 11,829 keys/s | 7,944 keys/s | 10,801 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 83,687 keys/s | 47,256 keys/s | 73,142 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 346,755 keys/s | 263,617 keys/s | 302,747 keys/s |
| insert/random/value=128/clients=1 | 1,400 keys/s | 737 keys/s | 1,464 keys/s |
| insert/random/value=128/clients=1/batch=10 | 8,920 keys/s | 3,391 keys/s | 14,537 keys/s |
| insert/random/value=128/clients=1/batch=100 | 37,864 keys/s | 20,713 keys/s | 59,747 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 231,549 keys/s | 164,569 keys/s | 322,791 keys/s |
| insert/random/value=128/clients=8 | 8,239 keys/s | 509 keys/s | 6,428 keys/s |
| insert/random/value=128/clients=8/batch=100 | 138,194 keys/s | 19,420 keys/s | 207,202 keys/s |
| insert/random/value=128/clients=32 | 19,215 keys/s | 1,957 keys/s | 15,307 keys/s |
| insert/random/value=128/clients=32/batch=100 | 248,840 keys/s | 20,654 keys/s | 379,880 keys/s |
| insert/sequential/value=128/clients=1 | 1,304 keys/s | 752 keys/s | 1,408 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 10,378 keys/s | 7,013 keys/s | 14,474 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 79,006 keys/s | 42,606 keys/s | 59,132 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 341,739 keys/s | 235,100 keys/s | 313,250 keys/s |
| mixed/read=50/value=128/clients=1 | 2,732 operations/s | 1,455 operations/s | 1,255 operations/s |
| mixed/read=50/value=128/clients=8 | 13,263 operations/s | 996 operations/s | 4,307 operations/s |
| mixed/read=50/value=128/clients=32 | 30,263 operations/s | 3,735 operations/s | 15,933 operations/s |
| mixed/read=95/value=128/clients=1 | 23,164 operations/s | 14,066 operations/s | 13,815 operations/s |
| mixed/read=95/value=128/clients=8 | 70,918 operations/s | 10,326 operations/s | 16,238 operations/s |
| mixed/read=95/value=128/clients=32 | 175,067 operations/s | 35,608 operations/s | 21,594 operations/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 110,582 keys/s | 63,057 keys/s | 85,871 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 115,247 keys/s | 189,277 keys/s | 492,347 keys/s |
| update/random/shrink=1024-32/clients=1 | 154,107 keys/s | 65,844 keys/s | 102,012 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 247,561 keys/s | 210,913 keys/s | 1,049,460 keys/s |
| update/random/value=32/clients=1 | 214,182 keys/s | 46,891 keys/s | 107,385 keys/s |
| update/random/value=128/clients=1 | 189,772 keys/s | 66,340 keys/s | 99,344 keys/s |
| update/random/value=128/clients=1/batch=10 | 317,315 keys/s | 152,869 keys/s | 452,296 keys/s |
| update/random/value=128/clients=1/batch=100 | 383,134 keys/s | 299,629 keys/s | 1,000,361 keys/s |
| update/random/value=128/clients=1/batch=1000 | 495,367 keys/s | 418,236 keys/s | 1,020,543 keys/s |
| update/random/value=128/clients=8 | 174,766 keys/s | 44,989 keys/s | 163,073 keys/s |
| update/random/value=128/clients=8/batch=100 | 342,293 keys/s | 271,661 keys/s | 973,873 keys/s |
| update/random/value=128/clients=32 | 167,035 keys/s | 46,084 keys/s | 169,875 keys/s |
| update/random/value=128/clients=32/batch=100 | 358,020 keys/s | 284,399 keys/s | 849,840 keys/s |
| update/random/value=1024/clients=1 | 120,878 keys/s | 66,069 keys/s | 88,256 keys/s |
| update/random/value=3072/clients=1 | 117,955 keys/s | 58,132 keys/s | 72,195 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 1,174,979 keys/s | 510,868 keys/s | 458,144 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 1,725,026 keys/s | 1,611,997 keys/s | 1,005,632 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 2,023,498 keys/s | 2,113,575 keys/s | 1,093,997 keys/s |
| insert/random/value=128/clients=1 | 221,694 keys/s | 51,581 keys/s | 104,924 keys/s |
| insert/random/value=128/clients=1/batch=10 | 232,649 keys/s | 135,198 keys/s | 450,470 keys/s |
| insert/random/value=128/clients=1/batch=100 | 341,919 keys/s | 261,979 keys/s | 871,844 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 611,401 keys/s | 639,431 keys/s | 1,018,212 keys/s |
| insert/random/value=128/clients=8 | 116,760 keys/s | 39,736 keys/s | 168,221 keys/s |
| insert/random/value=128/clients=8/batch=100 | 275,019 keys/s | 222,424 keys/s | 921,743 keys/s |
| insert/random/value=128/clients=32 | 110,145 keys/s | 35,859 keys/s | 163,243 keys/s |
| insert/random/value=128/clients=32/batch=100 | 292,888 keys/s | 248,296 keys/s | 928,342 keys/s |
| insert/sequential/value=128/clients=1 | 216,792 keys/s | 54,143 keys/s | 101,036 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 616,015 keys/s | 340,103 keys/s | 458,570 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 1,872,583 keys/s | 1,420,298 keys/s | 960,764 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 2,851,277 keys/s | 2,190,893 keys/s | 1,048,882 keys/s |
| mixed/read=50/value=128/clients=1 | 367,014 operations/s | 121,798 operations/s | 118,376 operations/s |
| mixed/read=50/value=128/clients=8 | 292,820 operations/s | 74,160 operations/s | 161,227 operations/s |
| mixed/read=50/value=128/clients=32 | 285,943 operations/s | 68,461 operations/s | 183,687 operations/s |
| mixed/read=95/value=128/clients=1 | 1,172,270 operations/s | 596,825 operations/s | 137,075 operations/s |
| mixed/read=95/value=128/clients=8 | 856,196 operations/s | 310,903 operations/s | 171,369 operations/s |
| mixed/read=95/value=128/clients=32 | 1,055,575 operations/s | 325,997 operations/s | 197,670 operations/s |

### Transactions

#### Read-only

| Keys per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 1,345,161 keys/s | 1,047,005 keys/s | 134,938 keys/s |
| 10 | 2,097,624 keys/s | 1,522,385 keys/s | 776,474 keys/s |
| 100 | 1,871,808 keys/s | 1,626,220 keys/s | 1,639,403 keys/s |
| 1,000 | 1,750,072 keys/s | 1,603,156 keys/s | 1,890,993 keys/s |

#### Durable mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 8,323 operations/s | 4,866 operations/s | 11,903 operations/s |
| 95% | 100 | 67,094 operations/s | 39,488 operations/s | 133,591 operations/s |
| 95% | 1,000 | 407,789 operations/s | 246,960 operations/s | 444,650 operations/s |
| 50% | 10 | 8,244 operations/s | 2,451 operations/s | 14,402 operations/s |
| 50% | 100 | 56,762 operations/s | 23,747 operations/s | 114,070 operations/s |
| 50% | 1,000 | 240,833 operations/s | 106,103 operations/s | 396,346 operations/s |

#### No-commit-sync mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 824,955 operations/s | 475,312 operations/s | 544,859 operations/s |
| 95% | 100 | 1,112,620 operations/s | 1,124,633 operations/s | 1,139,764 operations/s |
| 95% | 1,000 | 1,176,393 operations/s | 1,394,913 operations/s | 1,145,073 operations/s |
| 50% | 10 | 451,987 operations/s | 256,404 operations/s | 465,208 operations/s |
| 50% | 100 | 532,488 operations/s | 474,026 operations/s | 918,843 operations/s |
| 50% | 1,000 | 653,258 operations/s | 651,025 operations/s | 861,590 operations/s |

### Enumeration

The 0% case returns no entries. This report omits its entries-per-second value because that rate does not apply.

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| selectivity=1 | 7,537,613 entries/s | 6,955,849 entries/s | 108,906 entries/s |
| selectivity=10 | 6,477,531 entries/s | 7,081,245 entries/s | 459,714 entries/s |
| selectivity=100 | 2,807,628 entries/s | 6,639,291 entries/s | 947,693 entries/s |

### Ordered operations

Redis does not provide the required ordered-cursor API. The 0% range case returns no entries. This report omits its entries-per-second value.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| full-forward | 7,433,949 entries/s | 7,441,783 entries/s |
| full-reverse | 7,271,823 entries/s | 7,388,624 entries/s |
| range/selectivity=1 | 6,910,010 entries/s | 6,917,777 entries/s |
| range/selectivity=10 | 7,073,942 entries/s | 7,413,030 entries/s |
| range/selectivity=100 | 6,988,452 entries/s | 7,804,135 entries/s |
| seek-and-read=1 | 905,920 entries/s | 779,799 entries/s |
| seek-and-read=10 | 5,500,114 entries/s | 4,877,504 entries/s |
| seek-and-read=100 | 6,990,133 entries/s | 7,245,217 entries/s |
| seek-and-read=1000 | 6,981,948 entries/s | 7,339,595 entries/s |

### Access distribution

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| records=10000/hot-80-20 | 1,976,983 reads/s | 962,863 reads/s | 138,653 reads/s |
| records=10000/uniform | 1,869,413 reads/s | 957,493 reads/s | 138,908 reads/s |
| records=100000/hot-80-20 | 1,428,009 reads/s | 1,129,271 reads/s | 133,099 reads/s |
| records=100000/uniform | 1,277,002 reads/s | 1,009,484 reads/s | 130,830 reads/s |

### Collections

KVLite and bbolt use native buckets. Redis uses logical key prefixes.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 57,851 keys/s | 40,944 keys/s | 47,755 keys/s |
| write/collections=1/depth=3 | 75,166 keys/s | 43,297 keys/s | 48,142 keys/s |
| write/collections=100/depth=1 | 55,654 keys/s | 42,989 keys/s | 46,362 keys/s |
| write/collections=100/depth=3 | 60,320 keys/s | 38,122 keys/s | 63,240 keys/s |
| read/collections=1/depth=1 | 1,334,977 reads/s | 729,984 reads/s | 137,116 reads/s |
| read/collections=1/depth=3 | 770,901 reads/s | 576,646 reads/s | 134,411 reads/s |
| read/collections=100/depth=1 | 1,160,717 reads/s | 764,516 reads/s | 136,438 reads/s |
| read/collections=100/depth=3 | 632,471 reads/s | 568,374 reads/s | 134,456 reads/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 1,358,364 keys/s | 1,015,949 keys/s | 683,246 keys/s |
| write/collections=1/depth=3 | 1,281,184 keys/s | 1,096,872 keys/s | 583,229 keys/s |
| write/collections=100/depth=1 | 1,325,463 keys/s | 1,646,890 keys/s | 688,783 keys/s |
| write/collections=100/depth=3 | 1,782,607 keys/s | 1,760,451 keys/s | 646,976 keys/s |

### Latency

The large profile reports one, eight, and 32 clients.

#### Durable

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Read | 1 | KVLite | 458 ns | 584 ns | 708 ns | 653.76 µs |
| Read | 1 | bbolt | 542 ns | 1.25 µs | 2.12 µs | 2.95 ms |
| Read | 1 | Redis | 6.75 µs | 8.25 µs | 13.83 µs | 4.08 ms |
| Read | 8 | KVLite | 584 ns | 792 ns | 1.17 µs | 27.15 ms |
| Read | 8 | bbolt | 625 ns | 1.75 µs | 8.54 µs | 23.38 ms |
| Read | 8 | Redis | 29.75 µs | 96.25 µs | 174.75 µs | 9.13 ms |
| Read | 32 | KVLite | 625 ns | 833 ns | 1.21 µs | 58.31 ms |
| Read | 32 | bbolt | 708 ns | 6.92 µs | 208.50 µs | 32.16 ms |
| Read | 32 | Redis | 77.21 µs | 317.88 µs | 611.26 µs | 13.11 ms |
| Update | 1 | KVLite | 682.09 µs | 3.12 ms | 4.79 ms | 309.53 ms |
| Update | 1 | bbolt | 1.48 ms | 5.79 ms | 7.88 ms | 332.50 ms |
| Update | 1 | Redis | 1.01 ms | 2.76 ms | 3.70 ms | 110.96 ms |
| Update | 8 | KVLite | 1.06 ms | 1.92 ms | 2.85 ms | 11.69 ms |
| Update | 8 | bbolt | 15.73 ms | 19.14 ms | 21.88 ms | 34.81 ms |
| Update | 8 | Redis | 767.63 µs | 1.42 ms | 1.88 ms | 8.69 ms |
| Update | 32 | KVLite | 1.78 ms | 3.58 ms | 5.95 ms | 10.77 ms |
| Update | 32 | bbolt | 16.98 ms | 20.63 ms | 23.37 ms | 27.17 ms |
| Update | 32 | Redis | 1.12 ms | 1.72 ms | 2.08 ms | 5.07 ms |
| Mixed | 1 | KVLite | 833 ns | 417.42 µs | 1.11 ms | 246.66 ms |
| Mixed | 1 | bbolt | 792 ns | 206.36 µs | 1.36 ms | 17.58 ms |
| Mixed | 1 | Redis | 8.38 µs | 541.67 µs | 1.42 ms | 102.98 ms |
| Mixed | 8 | KVLite | 708 ns | 1.03 ms | 1.80 ms | 10.69 ms |
| Mixed | 8 | bbolt | 1.29 µs | 5.11 ms | 16.70 ms | 30.48 ms |
| Mixed | 8 | Redis | 78.21 µs | 1.75 ms | 2.72 ms | 86.00 ms |
| Mixed | 32 | KVLite | 625 ns | 1.27 ms | 2.35 ms | 32.93 ms |
| Mixed | 32 | bbolt | 1.33 µs | 7.00 ms | 17.42 ms | 23.74 ms |
| Mixed | 32 | Redis | 1.35 ms | 2.66 ms | 3.72 ms | 24.83 ms |

#### No commit sync

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Update | 1 | KVLite | 3.25 µs | 5.96 µs | 9.38 µs | 1.90 ms |
| Update | 1 | bbolt | 11.17 µs | 19.62 µs | 26.67 µs | 3.28 ms |
| Update | 1 | Redis | 9.46 µs | 11.41 µs | 17.42 µs | 380.71 µs |
| Update | 8 | KVLite | 3.29 µs | 6.33 µs | 15.88 µs | 9.43 ms |
| Update | 8 | bbolt | 11.29 µs | 23.58 µs | 4.21 ms | 10.31 ms |
| Update | 8 | Redis | 29.46 µs | 76.38 µs | 111.37 µs | 4.50 ms |
| Update | 32 | KVLite | 3.29 µs | 7.04 µs | 4.46 ms | 10.78 ms |
| Update | 32 | bbolt | 11.17 µs | 3.18 ms | 7.09 ms | 11.74 ms |
| Update | 32 | Redis | 102.92 µs | 281.50 µs | 480.38 µs | 6.24 ms |
| Mixed | 1 | KVLite | 542 ns | 2.71 µs | 3.96 µs | 1.14 ms |
| Mixed | 1 | bbolt | 625 ns | 9.92 µs | 15.12 µs | 2.84 ms |
| Mixed | 1 | Redis | 6.75 µs | 8.92 µs | 13.12 µs | 3.15 ms |
| Mixed | 8 | KVLite | 542 ns | 3.21 µs | 8.54 µs | 9.11 ms |
| Mixed | 8 | bbolt | 708 ns | 12.12 µs | 31.67 µs | 16.60 ms |
| Mixed | 8 | Redis | 30.38 µs | 95.63 µs | 179.29 µs | 7.04 ms |
| Mixed | 32 | KVLite | 542 ns | 3.42 µs | 131.00 µs | 18.03 ms |
| Mixed | 32 | bbolt | 708 ns | 13.17 µs | 1.48 ms | 22.25 ms |
| Mixed | 32 | Redis | 85.38 µs | 328.07 µs | 603.80 µs | 7.21 ms |

### Life cycle

Redis does not have the same embedded life-cycle boundary.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| close-after-writes | 2.36 ms | 15.29 µs |
| create-load-close | 41.19 ms | 59.18 ms |
| open-clean | 69.08 µs | 2.48 ms |
| recover-after-process-kill | 56.56 ms | 53.09 ms |

### Persistent bytes

`persistent-B` is the KVLite database plus WAL, the bbolt database, or the Redis AOF. These files have different maintenance and compaction rules.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| insert/random/value=128/clients=1 | 6,813,404 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 8,546,083 B | 8,388,608 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 8,762,411 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 9,056,868 B | 8,388,608 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 5,617,300 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 7,394,697 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 7,883,182 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 6,332,520 B | 8,388,608 B | 3,443,301 B |
| insert/sequential/value=128/clients=1 | 7,093,022 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 8,768,334 B | 8,388,608 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,807,299 B | 8,388,608 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,216,290 B | 8,388,608 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 5,243,390 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 5,091,095 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 4,959,845 B | 4,194,304 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 4,477,240 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 4,465,700 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 4,456,015 B | 4,194,304 B | 1,806,401 B |
| read/random/hits=0/misses=above/value=128/clients=1 | 3,481,600 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=0/misses=below/value=128/clients=1 | 3,481,600 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=0/misses=between/value=128/clients=1 | 3,481,600 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=50/value=128/clients=1 | 3,481,600 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=100/value=128/clients=8 | 3,481,600 B | 4,194,304 B | 1,720,401 B |
| read/random/hits=100/value=128/clients=32 | 3,481,600 B | 4,194,304 B | 1,720,401 B |
| read/random/value=32/clients=1 | 1,355,776 B | 2,097,152 B | 750,401 B |
| read/random/value=128/clients=1 | 3,481,600 B | 4,194,304 B | 1,720,401 B |
| read/random/value=1024/clients=1 | 41,615,360 B | 35,549,184 B | 10,690,401 B |
| read/random/value=3072/clients=1 | 41,623,552 B | 58,118,144 B | 31,170,401 B |
| read/sequential/hits=100/value=128/clients=1 | 3,481,600 B | 4,194,304 B | 1,720,401 B |
| update/random/grow=32-1024/clients=1 | 2,902,236 B | 4,194,304 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 24,389,244 B | 33,640,448 B | 11,443,301 B |
| update/random/shrink=1024-32/clients=1 | 41,749,360 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,711,220 B | 35,549,184 B | 11,443,301 B |
| update/random/value=32/clients=1 | 3,412,080 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 5,471,600 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 6,663,010 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 5,326,300 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 5,807,525 B | 8,388,608 B | 3,440,691 B |
| update/random/value=128/clients=8 | 5,654,585 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 3,662,405 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 5,538,730 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 5,386,375 B | 4,194,304 B | 3,443,301 B |
| update/random/value=1024/clients=1 | 42,741,360 B | 35,549,184 B | 11,759,401 B |
| update/random/value=3072/clients=1 | 44,797,552 B | 58,118,144 B | 34,287,401 B |
| update/sequential/value=128/clients=1/batch=10 | 6,781,570 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 5,251,275 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 5,131,125 B | 4,194,304 B | 3,440,691 B |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| insert/random/value=128/clients=1 | 6,813,404 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 8,546,083 B | 5,910,528 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 8,762,411 B | 6,303,744 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 9,056,868 B | 7,663,616 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 6,529,950 B | 4,268,032 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 8,568,266 B | 6,303,744 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 6,541,110 B | 4,259,840 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 8,583,503 B | 6,275,072 B | 3,443,301 B |
| insert/sequential/value=128/clients=1 | 7,093,022 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 8,768,334 B | 6,971,392 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,807,299 B | 6,971,392 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,216,290 B | 6,971,392 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 5,243,390 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 5,242,680 B | 4,280,320 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 5,242,040 B | 4,354,048 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 4,477,240 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 4,477,240 B | 4,284,416 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 4,477,240 B | 4,194,304 B | 1,806,401 B |
| update/random/grow=32-1024/clients=1 | 2,902,236 B | 2,469,888 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 24,389,244 B | 19,394,560 B | 11,443,301 B |
| update/random/shrink=1024-32/clients=1 | 41,749,360 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,711,220 B | 35,549,184 B | 11,443,301 B |
| update/random/value=32/clients=1 | 3,412,080 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 5,471,600 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 6,663,010 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 5,326,300 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 5,807,525 B | 6,082,560 B | 3,440,691 B |
| update/random/value=128/clients=8 | 5,754,180 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 5,167,795 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 5,754,180 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 5,168,435 B | 4,194,304 B | 3,443,301 B |
| update/random/value=1024/clients=1 | 42,741,360 B | 35,549,184 B | 11,759,401 B |
| update/random/value=3072/clients=1 | 44,797,552 B | 58,118,144 B | 34,287,401 B |
| update/sequential/value=128/clients=1/batch=10 | 6,781,570 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 5,251,275 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 5,131,125 B | 4,194,304 B | 3,440,691 B |
