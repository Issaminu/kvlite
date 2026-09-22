# KVLite benchmarks

This report compares KVLite with bbolt and Redis at the caller API boundary. It contains the median of five measured runs from the complete `medium --workloads=all` profile.

| Item | Value |
| --- | --- |
| Run time | `2026-09-22T21:01:03Z` |
| Elapsed time | 3 minutes 47.96 seconds |
| KVLite | `b29f396ca4bd367f9ac1f90d0678bc92b56affaa` with the recorded working-tree patch |
| bbolt | `v1.4.3` |
| Redis | `8.8.0` |
| Platform | Linux `arm64`, four CPUs in containers |
| Profile | `medium --workloads=all` |
| Warm-up | One complete unrecorded round |
| Measurement | Five measured rounds |

## Results at a glance

A higher throughput value is better. A lower latency or life-cycle value is better. This section highlights workloads where KVLite led the comparison.

| Category | Mode and workload | KVLite | bbolt | Redis | Highlight |
| --- | --- | ---: | ---: | ---: | --- |
| Point read | Read-only, random, 128-byte value, eight clients | 3,054,512 keys/s | 462,491 keys/s | 156,523 keys/s | **KVLite: 6.60x bbolt; 19.51x Redis** |
| Point-read p99 | Read-only, eight clients | 1.88 µs | 6.13 µs | 174.67 µs | **KVLite: 3.27x advantage over bbolt; 93.16x over Redis** |
| Batch update | Durable, random, 100 keys, eight clients | 185,244 keys/s | 23,426 keys/s | 176,537 keys/s | **KVLite and Redis are within 5%** |
| Mixed work | Durable, 95% reads, eight clients | 29,951 operations/s | 10,128 operations/s | 19,527 operations/s | **KVLite: 1.53x Redis; 2.96x bbolt** |
| Point update | No commit sync, random, one client | 188,286 keys/s | 69,308 keys/s | 104,468 keys/s | **KVLite: 1.80x Redis; 2.72x bbolt** |
| Mixed work | No commit sync, 95% reads, eight clients | 1,203,486 operations/s | 318,263 operations/s | 142,542 operations/s | **KVLite: 3.78x bbolt; 8.44x Redis** |
| Clean open | Life cycle | 79.54 µs | 2.85 ms | Not comparable | **KVLite: 35.86x advantage** |

These ratios apply only to the named workloads and API boundaries.

## Scope

This run measured every workload group in the medium profile. It includes the medium decision set for point operations, transactions, enumeration, ordered operations, access distributions, latency, collections, storage size, and embedded life-cycle operations.

The medium profile uses a selected case set. It does not contain every case variant in the large profile. The report does not reuse results from an older run.

## Fairness and API boundaries

The runner gave each engine the same fixed operation schedule and data set. It ran the engines serially. It changed engine order and durability-mode order between measured rounds. Each engine used the same four-CPU set and Docker storage class.

| Logical operation | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| Point read | `DB.Get` | One `DB.View` transaction | `GET` |
| Grouped read | One read transaction | One read transaction | `MGET` |
| Point write, one client | `DB.Put` | `DB.Update` | `SET` |
| Point write, many clients | `DB.Put` | `DB.Batch` | `SET` |
| Batch write | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Mixed transaction | One write transaction | One write transaction | `MULTI` and `EXEC` |
| Prefix enumeration | Ordered prefix cursor | Ordered prefix cursor | `SCAN` and `MGET` |

bbolt does not provide a direct `DB.Get` API. Use the grouped read results when transaction setup must be shared across several reads. Redis includes its client, server, and local network path. These are caller-level comparisons, not comparisons of identical internal operations.

## Durability modes

| Mode | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| Durable | `SyncFull` | `NoSync=false`; `NoGrowSync=false`; `NoFreelistSync=false` | AOF with `appendfsync always` |
| No commit sync | `SyncNone` | `NoSync=true`; `NoGrowSync=true`; `NoFreelistSync=false` | AOF with `appendfsync no` |

A successful durable write waits for the durability boundary of its engine. In no-commit-sync mode, no engine asks storage to sync during commit. Durable and no-commit-sync results have different guarantees and must remain separate.

## Test environment

- CPU set: `0-3`
- Go: `go1.26.7 linux/arm64`
- KVLite: `b29f396ca4bd367f9ac1f90d0678bc92b56affaa` with the source hashes and working-tree patch recorded by the runner
- bbolt: `v1.4.3`
- Redis client: `go-redis v9.22.0`
- Redis server: `8.8.0`, `jemalloc-5.3.0`
- Container system: OrbStack with Linux kernel `7.0.14-orbstack-00380-ga7e0a2dc9535`
- Storage driver: `overlay2`
- Host CPUs visible to the container system: 14
- CPUs assigned to each measured process and its Redis server: four

Other containers were active on the host. CPU assignment reduced direct CPU competition. Other host work could still affect the results.

## Method

The runner first completed its correctness tests. It then completed one unrecorded warm-up round. It created a new database fixture for each benchmark process. It did not reuse a measured database from the warm-up round.

The runner recorded five fixed-work rounds. It changed engine and durability-mode order between rounds. Pure reads ran once per round because commit-sync settings do not affect them.

The medium profile uses 3,000 records for its main cases. It uses 10,000 point reads, 200 single-client point writes, 640 concurrent point writes, and 2,000 mixed operations. Read-latency cases use 100,000 recorded operations and 1,000 unrecorded fixture warm-up reads. Update-latency cases use 1,000 operations. Mixed-latency cases use 10,000 operations.

Setup, fixture loading, final stored-data checks, and close operations were outside the core timers. The named life-cycle cases measure open, close, and recovery work separately.

## Interpretation

These statements apply only to this run and its tested workloads.

- KVLite led every medium point-read throughput case.
- KVLite had the lowest point-read p99 latency at eight clients. Its result gave it a 3.27x advantage over bbolt and a 93.16x advantage over Redis.
- KVLite led durable 50% and 95% read mixed work.
- KVLite led no-commit-sync point operations and mixed work.
- KVLite led several ordered-operation cases. Several results were within 5%.
- KVLite opened a clean database 35.86x faster than bbolt.

## Limits

- This is one run on one container host. It is not a bare-metal Linux result.
- The report contains medians from five measured rounds. It does not contain a confidence interval or a significance test.
- A large profile uses 10 measured rounds and the complete case matrix. This medium run is an engineering comparison with selected cases.
- The 5% display band does not prove that two results are equal.
- Read tests use a warm operating-system cache. The suite does not claim cold-cache performance.
- The suite does not rank combined CPU use or peak resident memory across embedded and client-server designs.
- Redis has no result for ordered cursor or embedded life-cycle operations.
- Persistent byte counts cover different file designs and maintenance rules. They are not a direct storage-efficiency ranking.

## Reproduce

```sh
cd benchmarks/kvbench
KVBENCH_RESULTS_DIR=/tmp/kvlite-kvbench-medium-all \
./run-docker.sh medium --workloads=all
```

The runner uses pinned Go and Redis container images. The Go module pins bbolt and the Redis client. Change the results directory for a new run.

## Full results

Every value is the median of five measured runs.

### Acknowledged operations

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| read/random/value=128/clients=1 | 2,405,597 keys/s | 873,540 keys/s | 130,987 keys/s |
| read/random/value=3072/clients=1 | 1,028,719 keys/s | 751,267 keys/s | 99,230 keys/s |
| read/sequential/hits=100/value=128/clients=1 | 3,256,919 keys/s | 866,360 keys/s | 138,753 keys/s |
| read/random/hits=0/misses=between/value=128/clients=1 | 3,560,290 keys/s | 702,514 keys/s | 115,337 keys/s |
| read/random/hits=100/value=128/clients=8 | 3,054,512 keys/s | 462,491 keys/s | 156,523 keys/s |
| update/random/grow=32-1024/clients=1 | 534 keys/s | 230 keys/s | 689 keys/s |
| update/random/value=128/clients=1 | 781 keys/s | 226 keys/s | 821 keys/s |
| update/random/value=128/clients=8 | 2,758 keys/s | 516 keys/s | 5,739 keys/s |
| update/random/value=128/clients=1/batch=100 | 34,517 keys/s | 24,254 keys/s | 53,728 keys/s |
| update/random/value=128/clients=8/batch=100 | 185,244 keys/s | 23,426 keys/s | 176,537 keys/s |
| insert/random/value=128/clients=1 | 479 keys/s | 237 keys/s | 427 keys/s |
| insert/random/value=128/clients=8 | 3,368 keys/s | 525 keys/s | 5,913 keys/s |
| insert/random/value=128/clients=1/batch=100 | 39,905 keys/s | 19,214 keys/s | 58,380 keys/s |
| mixed/read=95/value=128/clients=8 | 29,951 operations/s | 10,128 operations/s | 19,527 operations/s |
| mixed/read=50/value=128/clients=8 | 7,441 operations/s | 993 operations/s | 2,892 operations/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 107,630 keys/s | 66,571 keys/s | 80,748 keys/s |
| update/random/value=128/clients=1 | 188,286 keys/s | 69,308 keys/s | 104,468 keys/s |
| update/random/value=128/clients=8 | 201,194 keys/s | 26,072 keys/s | 140,579 keys/s |
| update/random/value=128/clients=1/batch=100 | 472,855 keys/s | 382,981 keys/s | 929,881 keys/s |
| update/random/value=128/clients=8/batch=100 | 229,980 keys/s | 262,159 keys/s | 739,817 keys/s |
| insert/random/value=128/clients=1 | 211,220 keys/s | 31,974 keys/s | 95,659 keys/s |
| insert/random/value=128/clients=8 | 202,294 keys/s | 22,015 keys/s | 118,284 keys/s |
| insert/random/value=128/clients=1/batch=100 | 242,699 keys/s | 235,070 keys/s | 914,468 keys/s |
| mixed/read=95/value=128/clients=8 | 1,203,486 operations/s | 318,263 operations/s | 142,542 operations/s |
| mixed/read=50/value=128/clients=8 | 239,627 operations/s | 49,207 operations/s | 128,048 operations/s |

### Transactions

#### Read-only

| Keys per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 1,135,892 keys/s | 622,203 keys/s | 132,119 keys/s |
| 100 | 1,796,226 keys/s | 1,768,288 keys/s | 1,165,706 keys/s |
| 1,000 | 1,778,681 keys/s | 1,629,023 keys/s | 675,841 keys/s |

#### Durable mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 5,502 operations/s | 5,423 operations/s | 6,502 operations/s |
| 95% | 100 | 39,637 operations/s | 50,548 operations/s | 63,450 operations/s |
| 50% | 10 | 2,997 operations/s | 5,275 operations/s | 4,415 operations/s |
| 50% | 100 | 53,320 operations/s | 29,571 operations/s | 86,402 operations/s |

#### No-commit-sync mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 807,851 operations/s | 430,894 operations/s | 456,825 operations/s |
| 95% | 100 | 1,170,841 operations/s | 841,787 operations/s | 958,603 operations/s |
| 50% | 10 | 418,562 operations/s | 256,963 operations/s | 449,872 operations/s |
| 50% | 100 | 500,742 operations/s | 532,854 operations/s | 634,812 operations/s |

### Enumeration

The 0% case returns no entries. It is omitted because an entries-per-second value does not apply.

| Selectivity | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: |
| 1% | 7,125,654 entries/s | 8,369,335 entries/s | 114,031 entries/s |
| 10% | 7,312,144 entries/s | 8,281,827 entries/s | 572,327 entries/s |
| 100% | 3,065,594 entries/s | 7,313,481 entries/s | 1,211,047 entries/s |

### Ordered operations

Redis does not provide the required ordered-cursor API. The 0% range case returns no entries and is omitted.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| full-forward | 8,204,819 entries/s | 7,853,115 entries/s |
| full-reverse | 6,630,908 entries/s | 6,981,388 entries/s |
| range/selectivity=1 | 7,602,366 entries/s | 7,008,912 entries/s |
| range/selectivity=10 | 7,488,040 entries/s | 8,051,504 entries/s |
| range/selectivity=100 | 7,708,834 entries/s | 7,534,114 entries/s |
| seek-and-read=1 | 931,873 entries/s | 883,630 entries/s |
| seek-and-read=100 | 6,877,555 entries/s | 6,777,529 entries/s |
| seek-and-read=1000 | 8,096,596 entries/s | 8,031,561 entries/s |

### Access distribution

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| records=10000/hot-80-20 | 2,115,089 reads/s | 786,991 reads/s | 133,638 reads/s |
| records=10000/uniform | 1,959,129 reads/s | 692,552 reads/s | 109,491 reads/s |

### Collections

KVLite and bbolt use native buckets. Redis uses logical key prefixes.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 34,274 keys/s | 30,347 keys/s | 34,156 keys/s |
| write/collections=100/depth=3 | 40,883 keys/s | 36,010 keys/s | 43,569 keys/s |
| read/collections=1/depth=1 | 1,118,606 reads/s | 676,775 reads/s | 126,173 reads/s |
| read/collections=100/depth=3 | 512,444 reads/s | 395,582 reads/s | 130,137 reads/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 1,618,445 keys/s | 1,596,907 keys/s | 833,061 keys/s |
| write/collections=100/depth=3 | 1,859,354 keys/s | 1,655,509 keys/s | 738,762 keys/s |

### Latency

Every latency row uses eight clients.

#### Durable

| Operation | Engine | p50 | p95 | p99 | Maximum |
| --- | --- | ---: | ---: | ---: | ---: |
| Read | KVLite | 541 ns | 750 ns | 1.88 µs | 12.00 ms |
| Read | bbolt | 625 ns | 2.17 µs | 6.13 µs | 17.90 ms |
| Read | Redis | 29.63 µs | 89.67 µs | 174.67 µs | 9.13 ms |
| Update | KVLite | 1.63 ms | 5.78 ms | 7.21 ms | 9.32 ms |
| Update | bbolt | 14.98 ms | 17.95 ms | 18.63 ms | 19.70 ms |
| Update | Redis | 1.11 ms | 3.37 ms | 5.10 ms | 8.76 ms |
| Mixed | KVLite | 708 ns | 1.79 ms | 4.61 ms | 12.53 ms |
| Mixed | bbolt | 1.13 µs | 4.01 ms | 16.40 ms | 22.56 ms |
| Mixed | Redis | 82.83 µs | 2.28 ms | 4.08 ms | 264.75 ms |

#### No commit sync

| Operation | Engine | p50 | p95 | p99 | Maximum |
| --- | --- | ---: | ---: | ---: | ---: |
| Update | KVLite | 3.46 µs | 9.46 µs | 1.87 ms | 3.60 ms |
| Update | bbolt | 12.17 µs | 62.71 µs | 4.34 ms | 6.44 ms |
| Update | Redis | 29.67 µs | 95.71 µs | 137.42 µs | 2.17 ms |
| Mixed | KVLite | 500 ns | 3.42 µs | 28.50 µs | 4.96 ms |
| Mixed | bbolt | 750 ns | 11.63 µs | 32.67 µs | 10.39 ms |
| Mixed | Redis | 31.46 µs | 98.50 µs | 282.38 µs | 3.11 ms |

### Life cycle

Redis does not have the same embedded life-cycle boundary.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| close-after-writes | 2.59 ms | 22.13 µs |
| create-load-close | 15.70 ms | 27.45 ms |
| open-clean | 79.54 µs | 2.85 ms |
| recover-after-process-kill | 55.98 ms | 54.22 ms |

### Persistent bytes

`persistent-B` is the KVLite database plus WAL, the bbolt database, or the Redis AOF. These files have different maintenance and compaction rules.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| read/random/value=128/clients=1 | 1,048,576 B | 2,097,152 B | 516,198 B |
| read/random/value=3072/clients=1 | 12,492,800 B | 16,777,216 B | 9,351,198 B |
| read/sequential/hits=100/value=128/clients=1 | 1,048,576 B | 2,097,152 B | 516,198 B |
| read/random/hits=0/misses=between/value=128/clients=1 | 1,048,576 B | 2,097,152 B | 516,198 B |
| read/random/hits=100/value=128/clients=8 | 1,048,576 B | 2,097,152 B | 516,198 B |
| update/random/grow=32-1024/clients=1 | 1,284,298 B | 1,048,576 B | 438,998 B |
| update/random/value=128/clients=1 | 1,450,416 B | 2,097,152 B | 550,598 B |
| update/random/value=128/clients=8 | 2,302,301 B | 2,097,152 B | 626,278 B |
| update/random/value=128/clients=1/batch=100 | 1,869,936 B | 2,097,152 B | 1,033,068 B |
| update/random/value=128/clients=8/batch=100 | 3,268,926 B | 2,097,152 B | 1,033,068 B |
| insert/random/value=128/clients=1 | 1,720,042 B | 2,097,152 B | 550,598 B |
| insert/random/value=128/clients=8 | 2,635,989 B | 2,097,152 B | 626,278 B |
| insert/random/value=128/clients=1/batch=100 | 2,368,516 B | 2,097,152 B | 1,033,068 B |
| mixed/read=95/value=128/clients=8 | 1,245,701 B | 2,097,152 B | 533,398 B |
| mixed/read=50/value=128/clients=8 | 3,003,096 B | 2,097,152 B | 688,198 B |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 1,284,298 B | 585,728 B | 438,998 B |
| update/random/value=128/clients=1 | 1,450,416 B | 2,097,152 B | 550,598 B |
| update/random/value=128/clients=8 | 2,329,856 B | 2,097,152 B | 626,278 B |
| update/random/value=128/clients=1/batch=100 | 1,869,936 B | 2,097,152 B | 1,033,068 B |
| update/random/value=128/clients=8/batch=100 | 1,862,076 B | 2,097,152 B | 1,033,068 B |
| insert/random/value=128/clients=1 | 1,720,042 B | 2,097,152 B | 550,598 B |
| insert/random/value=128/clients=8 | 3,181,586 B | 2,097,152 B | 626,278 B |
| insert/random/value=128/clients=1/batch=100 | 2,368,516 B | 2,097,152 B | 1,033,068 B |
| mixed/read=95/value=128/clients=8 | 1,247,576 B | 2,097,152 B | 533,398 B |
| mixed/read=50/value=128/clients=8 | 3,048,176 B | 2,097,152 B | 688,198 B |
