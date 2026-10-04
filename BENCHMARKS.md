# KVLite benchmarks

This report compares KVLite with bbolt and Redis at the public call boundary. It contains the median of 15 measured runs at the complete `large --workloads=all --storage=volume` scale.

| Item | Value |
| --- | --- |
| Run time | `2026-10-03T02:30:16Z` |
| Elapsed time | 2 hours 35 minutes 43 seconds |
| KVLite | `c0fff00ec19edad774bc70b36ccc7d5bf1d534d1` from a clean working tree |
| bbolt | `v1.4.3` |
| Redis | `8.8.0` |
| Platform | Linux `arm64`, four CPUs in containers |
| Scale | `large --workloads=all --storage=volume` |
| Warm-up | One complete unrecorded round |
| Measurement | 15 measured rounds |

## Results at a glance

A higher throughput value is better. A lower latency or life-cycle value is better. This section shows cases where KVLite had the best median.

| Category | Mode and workload | KVLite | bbolt | Redis | Highlight |
| --- | --- | ---: | ---: | ---: | --- |
| Point read | Read-only, random, 128-byte value, eight clients | 3,306,425 keys/s | 893,510 keys/s | 181,213 keys/s | **KVLite: 3.70x bbolt; 18.25x Redis** |
| Point-read p99 | Read-only, eight clients | 2.04 µs | 7.54 µs | 169.46 µs | **KVLite: 3.70x advantage over bbolt; 83.03x over Redis** |
| Point update | Durable, random, 128-byte value, one client, batch of 100 | 87,213 keys/s | 20,401 keys/s | 45,206 keys/s | **KVLite: 4.27x bbolt; 1.93x Redis** |
| Mixed work | Durable, 95% reads, eight clients | 122,394 operations/s | 8,625 operations/s | 14,323 operations/s | **KVLite: 14.19x bbolt; 8.55x Redis** |
| Mixed work | No commit sync, 95% reads, eight clients | 1,021,348 operations/s | 332,597 operations/s | 163,943 operations/s | **KVLite: 3.07x bbolt; 6.23x Redis** |
| Key delete | No commit sync, random, one client | 201,255 keys/s | 64,736 keys/s | 101,353 keys/s | **KVLite: 3.11x bbolt; 1.99x Redis** |
| Clean open | Life cycle | 48.04 µs | 2.91 ms | Not comparable | **KVLite: 60.55x advantage over bbolt** |

These ratios apply only to the named workloads and public call boundaries.

## Scope

This run measured every workload group and every case variant at the large scale. It covers point operations, deletion, transactions, enumeration, ordered operations, access distributions, latency, collections, storage size, and embedded life-cycle operations.

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
| Key delete | `DB.Delete` | One `DB.Update` transaction | `DEL` |
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
- KVLite: `c0fff00ec19edad774bc70b36ccc7d5bf1d534d1` from a clean working tree
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

The large scale uses 10,000 records for its main cases. It also tests key and bucket deletion, delete visibility, page reuse, tail reclamation, and reopen after deletion. It uses 100,000 point reads, 1,000 single-client point writes, 3,200 concurrent point writes, and 10,000 mixed operations. Read-latency cases use 1,000,000 recorded operations and 10,000 unrecorded fixture warm-up reads. Update-latency cases use 10,000 operations. Mixed-latency cases use 100,000 operations.

Setup, fixture loading, final stored-data checks, and close operations were outside the core timers. The named life-cycle cases measure open, close, and recovery work separately.

## Interpretation

These statements apply only to this run and its tested workloads.

- KVLite led 80 of 107 durable cases. bbolt led six. Redis led 21.
- KVLite led 41 of 65 no-commit-sync cases. bbolt led four. Redis led 20.
- Each count uses one main metric for each case. It excludes storage size and duplicate throughput metrics. Latency cases use p99.
- KVLite led every point-read throughput case.
- KVLite led 36 of 38 read cases, including point reads, read transactions, scans, collection reads, and read latency.
- KVLite led 12 of 18 durable point-update cases and six of 12 durable point-insert cases.
- KVLite led 13 of 18 no-commit-sync point-update cases and five of 12 no-commit-sync point-insert cases.
- KVLite led durable acknowledged mixed work at all tested client counts and read shares.
- KVLite led eight of 26 deletion, reuse, and reopen cases across both modes.
- KVLite opened a clean database 60.55x faster than bbolt.

## Limits

- This is one run on one container host. It is not a bare-metal Linux result.
- Docker volume storage includes the OrbStack virtual machine and host storage path. It does not isolate physical-device sync latency.
- Read tests use a warm operating-system cache. The suite does not claim cold-cache performance.
- The suite does not rank combined CPU use or peak resident memory across embedded and client-server designs.
- Redis has no result for ordered cursor or embedded life-cycle operations.
- KVLite performs a final checkpoint during close. The close-after-writes case does not compare the same work across the embedded engines.
- Persistent byte counts cover different file designs and maintenance rules. They are not a direct storage-efficiency ranking.

## Reproduce

```sh
cd benchmarks/kvbench
./run-docker.sh large --workloads=all --storage=volume
```

The runner uses pinned Go and Redis container images. The Go module pins bbolt and the Redis client.

## Full results

Every value is the median of 15 measured runs.

### Acknowledged operations

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| read/random/hits=0/misses=above/value=128/clients=1 | 4,297,050 keys/s | 1,774,840 keys/s | 122,834 keys/s |
| read/random/hits=0/misses=below/value=128/clients=1 | 4,127,878 keys/s | 1,826,642 keys/s | 122,399 keys/s |
| read/random/hits=0/misses=between/value=128/clients=1 | 3,536,319 keys/s | 1,700,672 keys/s | 121,947 keys/s |
| read/random/hits=50/value=128/clients=1 | 2,904,666 keys/s | 1,517,656 keys/s | 129,135 keys/s |
| read/random/hits=100/value=128/clients=8 | 3,306,425 keys/s | 893,510 keys/s | 181,213 keys/s |
| read/random/hits=100/value=128/clients=32 | 3,636,903 keys/s | 886,551 keys/s | 241,904 keys/s |
| read/random/value=32/clients=1 | 2,612,609 keys/s | 1,344,046 keys/s | 142,634 keys/s |
| read/random/value=128/clients=1 | 2,395,158 keys/s | 1,348,953 keys/s | 135,808 keys/s |
| read/random/value=1024/clients=1 | 1,469,960 keys/s | 1,279,835 keys/s | 127,755 keys/s |
| read/random/value=3072/clients=1 | 1,181,123 keys/s | 1,086,090 keys/s | 103,721 keys/s |
| read/sequential/hits=100/value=128/clients=1 | 3,021,112 keys/s | 1,416,498 keys/s | 132,813 keys/s |
| update/random/grow=32-1024/clients=1 | 1,011 keys/s | 479 keys/s | 1,358 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 23,923 keys/s | 13,563 keys/s | 35,340 keys/s |
| update/random/shrink=1024-32/clients=1 | 1,599 keys/s | 823 keys/s | 1,792 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 43,625 keys/s | 12,914 keys/s | 47,645 keys/s |
| update/random/value=32/clients=1 | 1,491 keys/s | 682 keys/s | 1,710 keys/s |
| update/random/value=128/clients=1 | 1,683 keys/s | 514 keys/s | 1,624 keys/s |
| update/random/value=128/clients=1/batch=10 | 13,353 keys/s | 4,140 keys/s | 13,286 keys/s |
| update/random/value=128/clients=1/batch=100 | 87,213 keys/s | 20,401 keys/s | 45,206 keys/s |
| update/random/value=128/clients=1/batch=1000 | 301,816 keys/s | 107,186 keys/s | 308,858 keys/s |
| update/random/value=128/clients=8 | 12,112 keys/s | 422 keys/s | 9,010 keys/s |
| update/random/value=128/clients=8/batch=100 | 245,729 keys/s | 20,462 keys/s | 209,554 keys/s |
| update/random/value=128/clients=32 | 37,215 keys/s | 1,643 keys/s | 17,549 keys/s |
| update/random/value=128/clients=32/batch=100 | 471,266 keys/s | 20,370 keys/s | 446,074 keys/s |
| update/random/value=1024/clients=1 | 1,561 keys/s | 265 keys/s | 1,401 keys/s |
| update/random/value=3072/clients=1 | 1,569 keys/s | 221 keys/s | 1,177 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 15,584 keys/s | 7,727 keys/s | 12,935 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 118,612 keys/s | 34,897 keys/s | 47,779 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 418,087 keys/s | 282,542 keys/s | 313,710 keys/s |
| insert/random/value=128/clients=1 | 1,293 keys/s | 858 keys/s | 1,754 keys/s |
| insert/random/value=128/clients=1/batch=10 | 10,355 keys/s | 2,586 keys/s | 14,581 keys/s |
| insert/random/value=128/clients=1/batch=100 | 50,812 keys/s | 21,308 keys/s | 42,793 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 231,431 keys/s | 185,133 keys/s | 309,138 keys/s |
| insert/random/value=128/clients=8 | 7,264 keys/s | 429 keys/s | 6,997 keys/s |
| insert/random/value=128/clients=8/batch=100 | 176,217 keys/s | 20,380 keys/s | 195,803 keys/s |
| insert/random/value=128/clients=32 | 16,369 keys/s | 1,677 keys/s | 15,848 keys/s |
| insert/random/value=128/clients=32/batch=100 | 373,016 keys/s | 18,675 keys/s | 369,837 keys/s |
| insert/sequential/value=128/clients=1 | 1,366 keys/s | 803 keys/s | 1,859 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 5,999 keys/s | 7,571 keys/s | 12,904 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 92,835 keys/s | 33,706 keys/s | 44,317 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 439,060 keys/s | 235,167 keys/s | 323,384 keys/s |
| mixed/read=50/value=128/clients=1 | 3,309 operations/s | 1,138 operations/s | 2,941 operations/s |
| mixed/read=50/value=128/clients=8 | 20,762 operations/s | 835 operations/s | 9,523 operations/s |
| mixed/read=50/value=128/clients=32 | 50,013 operations/s | 3,184 operations/s | 18,313 operations/s |
| mixed/read=95/value=128/clients=1 | 29,377 operations/s | 8,781 operations/s | 12,029 operations/s |
| mixed/read=95/value=128/clients=8 | 122,394 operations/s | 8,625 operations/s | 14,323 operations/s |
| mixed/read=95/value=128/clients=32 | 301,296 operations/s | 30,330 operations/s | 16,561 operations/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 93,055 keys/s | 65,657 keys/s | 72,348 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 133,986 keys/s | 223,540 keys/s | 441,429 keys/s |
| update/random/shrink=1024-32/clients=1 | 202,318 keys/s | 70,515 keys/s | 95,293 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 429,590 keys/s | 246,782 keys/s | 1,139,360 keys/s |
| update/random/value=32/clients=1 | 304,837 keys/s | 57,754 keys/s | 87,642 keys/s |
| update/random/value=128/clients=1 | 258,962 keys/s | 67,650 keys/s | 87,675 keys/s |
| update/random/value=128/clients=1/batch=10 | 634,178 keys/s | 171,114 keys/s | 400,187 keys/s |
| update/random/value=128/clients=1/batch=100 | 765,730 keys/s | 367,003 keys/s | 887,025 keys/s |
| update/random/value=128/clients=1/batch=1000 | 937,356 keys/s | 505,841 keys/s | 918,137 keys/s |
| update/random/value=128/clients=8 | 205,040 keys/s | 48,089 keys/s | 151,067 keys/s |
| update/random/value=128/clients=8/batch=100 | 619,470 keys/s | 308,580 keys/s | 998,395 keys/s |
| update/random/value=128/clients=32 | 204,959 keys/s | 45,382 keys/s | 178,463 keys/s |
| update/random/value=128/clients=32/batch=100 | 657,164 keys/s | 318,603 keys/s | 951,547 keys/s |
| update/random/value=1024/clients=1 | 144,163 keys/s | 67,776 keys/s | 83,286 keys/s |
| update/random/value=3072/clients=1 | 121,753 keys/s | 58,958 keys/s | 73,657 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 1,386,860 keys/s | 493,615 keys/s | 451,806 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 1,991,482 keys/s | 1,686,949 keys/s | 958,174 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 2,326,062 keys/s | 2,044,476 keys/s | 964,417 keys/s |
| insert/random/value=128/clients=1 | 183,570 keys/s | 61,186 keys/s | 95,232 keys/s |
| insert/random/value=128/clients=1/batch=10 | 226,406 keys/s | 153,782 keys/s | 436,519 keys/s |
| insert/random/value=128/clients=1/batch=100 | 313,820 keys/s | 286,684 keys/s | 921,176 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 726,544 keys/s | 566,151 keys/s | 1,023,069 keys/s |
| insert/random/value=128/clients=8 | 125,997 keys/s | 36,888 keys/s | 154,354 keys/s |
| insert/random/value=128/clients=8/batch=100 | 288,772 keys/s | 242,695 keys/s | 929,448 keys/s |
| insert/random/value=128/clients=32 | 125,773 keys/s | 33,808 keys/s | 171,793 keys/s |
| insert/random/value=128/clients=32/batch=100 | 301,193 keys/s | 253,131 keys/s | 940,022 keys/s |
| insert/sequential/value=128/clients=1 | 184,134 keys/s | 58,190 keys/s | 91,946 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 520,870 keys/s | 339,736 keys/s | 426,881 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 1,715,847 keys/s | 1,538,430 keys/s | 920,052 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 3,098,480 keys/s | 2,318,717 keys/s | 1,010,732 keys/s |
| mixed/read=50/value=128/clients=1 | 568,955 operations/s | 131,279 operations/s | 118,964 operations/s |
| mixed/read=50/value=128/clients=8 | 381,167 operations/s | 74,172 operations/s | 153,450 operations/s |
| mixed/read=50/value=128/clients=32 | 360,842 operations/s | 67,973 operations/s | 180,948 operations/s |
| mixed/read=95/value=128/clients=1 | 1,293,740 operations/s | 577,028 operations/s | 134,776 operations/s |
| mixed/read=95/value=128/clients=8 | 1,021,348 operations/s | 332,597 operations/s | 163,943 operations/s |
| mixed/read=95/value=128/clients=32 | 942,457 operations/s | 342,800 operations/s | 194,778 operations/s |

### Key deletion: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 1,225 ack-keys/s | 526 ack-keys/s | 1,288 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 7,062,101 B | 4,194,304 B | 2,080,401 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 64,649 ack-keys/s | 37,181 ack-keys/s | 145,185 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 6,825,211 B | 4,194,304 B | 1,951,901 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 291,034 ack-keys/s | 215,088 ack-keys/s | 872,571 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,087,864 B | 8,388,608 B | 1,950,561 B |
| `existing/random/clients=8` | `ack-keys/s` | 5,240 ack-keys/s | 429 ack-keys/s | 9,701 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 5,363,328 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 590 ack-keys/s | 520 ack-keys/s | 1,615 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 4,667,192 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 87,331 ack-keys/s | 86,700 ack-keys/s | 76,210 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 4,297,516 B | 4,194,304 B | 1,951,901 B |
| `missing/random/clients=1` | `ack-keys/s` | 328,366 ack-keys/s | 875 ack-keys/s | 132,371 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,485,696 B | 4,194,304 B | 1,720,401 B |

### Key deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 201,255 ack-keys/s | 64,736 ack-keys/s | 101,353 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 7,062,101 B | 4,194,304 B | 2,080,401 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 453,523 ack-keys/s | 383,011 ack-keys/s | 2,354,454 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 6,825,211 B | 4,194,304 B | 1,951,901 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 948,741 ack-keys/s | 718,816 ack-keys/s | 3,655,782 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,087,864 B | 6,045,696 B | 1,950,561 B |
| `existing/random/clients=8` | `ack-keys/s` | 164,130 ack-keys/s | 44,883 ack-keys/s | 173,071 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 7,161,493 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 230,376 ack-keys/s | 66,720 ack-keys/s | 102,669 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 4,667,192 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 1,899,538 ack-keys/s | 2,172,394 ack-keys/s | 2,428,795 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 4,297,516 B | 4,194,304 B | 1,951,901 B |
| `missing/random/clients=1` | `ack-keys/s` | 3,371,185 ack-keys/s | 199,859 ack-keys/s | 135,863 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,485,696 B | 4,194,304 B | 1,720,401 B |

### Bucket deletion: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 648 buckets/s | 816 buckets/s | — |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 4,289,378 B | 4,194,304 B | — |

### Bucket deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 32,268 buckets/s | 64,814 buckets/s | — |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 4,289,378 B | 4,194,304 B | — |

### Read after delete: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 1.08 ms | 2.00 ms | 684.74 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 1.21 µs | 1.50 µs | 15.54 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 2.71 µs | 3.87 µs | 32.50 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 5.04 µs | 7.75 µs | 58.75 µs |

### Read after delete: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 5.15 µs | 24.77 µs | 9.17 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 375 ns | 791 ns | 8.17 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 500 ns | 2.58 µs | 11.83 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 708 ns | 4.00 µs | 17.38 µs |

### Page reuse: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 43,492 cycle-keys/s | 21,543 cycle-keys/s | 122,766 cycle-keys/s |
| `random-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `random-half/batch=100` | `post-delete-persistent-B` | 14,448,677 B | 16,777,216 B | 5,676,151 B |
| `random-half/batch=100` | `active-post-reinsert-persistent-B` | 17,057,487 B | 16,777,216 B | 8,457,601 B |
| `random-half/batch=100` | `stable-post-reinsert-persistent-B` | 13,881,344 B | 16,777,216 B | — |
| `random-half/batch=100` | `reuse-active-growth-B` | 3,176,143 B | 0 B | 2,897,200 B |
| `random-half/batch=100` | `reuse-stable-growth-B` | 0 B | 0 B | — |
| `random-half/batch=100` | `reinsert-pages-reused` | 1,035 | — | — |

### Page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 258,300 cycle-keys/s | 298,487 cycle-keys/s | 1,025,095 cycle-keys/s |
| `random-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `random-half/batch=100` | `post-delete-persistent-B` | 14,448,677 B | 16,777,216 B | 5,676,151 B |
| `random-half/batch=100` | `active-post-reinsert-persistent-B` | 17,057,487 B | 16,777,216 B | 8,457,601 B |
| `random-half/batch=100` | `stable-post-reinsert-persistent-B` | 13,881,344 B | 16,777,216 B | — |
| `random-half/batch=100` | `reuse-active-growth-B` | 3,176,143 B | 0 B | 2,897,200 B |
| `random-half/batch=100` | `reuse-stable-growth-B` | 0 B | 0 B | — |
| `random-half/batch=100` | `reinsert-pages-reused` | 1,035 | — | — |

### Repeated page reuse: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 30,780 cycle-keys/s | 21,554 cycle-keys/s | 48,274 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 14,565,098 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 9,974,046 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,667 | — | — |

### Repeated page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 354,389 cycle-keys/s | 420,784 cycle-keys/s | 1,229,631 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 14,565,098 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 9,974,046 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,667 | — | — |

### Tail reclamation: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 45,922 ack-keys/s | 65,320 ack-keys/s | 64,889 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,353,131 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 2.58 ms | 103.58 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 89.96 µs | 1.27 ms | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Tail reclamation: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 1,059,600 ack-keys/s | 1,506,203 ack-keys/s | 2,347,895 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,353,131 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 461.04 µs | 63.58 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 36.71 µs | 53.33 µs | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Reopen after deletion: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `alternating-half` | `reopen-ns` | 77.58 µs | 1.26 ms | — |

### Reopen after deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `alternating-half` | `reopen-ns` | 56.54 µs | 46.50 µs | — |

### Transactions

#### Read-only

| Keys per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 1,478,405 keys/s | 1,051,684 keys/s | 110,813 keys/s |
| 10 | 2,097,457 keys/s | 1,843,000 keys/s | 751,091 keys/s |
| 100 | 2,133,813 keys/s | 1,912,498 keys/s | 1,638,855 keys/s |
| 1,000 | 2,228,808 keys/s | 2,026,876 keys/s | 1,991,706 keys/s |

#### Durable mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 15,285 operations/s | 7,718 operations/s | 12,361 operations/s |
| 95% | 100 | 135,569 operations/s | 64,141 operations/s | 130,572 operations/s |
| 95% | 1,000 | 713,593 operations/s | 325,788 operations/s | 435,906 operations/s |
| 50% | 10 | 16,279 operations/s | 4,833 operations/s | 13,025 operations/s |
| 50% | 100 | 124,848 operations/s | 22,454 operations/s | 123,265 operations/s |
| 50% | 1,000 | 540,297 operations/s | 125,372 operations/s | 338,113 operations/s |

#### No-commit-sync mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 1,003,331 operations/s | 515,857 operations/s | 521,845 operations/s |
| 95% | 100 | 1,440,694 operations/s | 1,047,950 operations/s | 1,091,875 operations/s |
| 95% | 1,000 | 1,649,306 operations/s | 1,337,100 operations/s | 1,033,582 operations/s |
| 50% | 10 | 805,997 operations/s | 258,944 operations/s | 417,005 operations/s |
| 50% | 100 | 1,115,301 operations/s | 491,476 operations/s | 810,191 operations/s |
| 50% | 1,000 | 1,287,033 operations/s | 746,874 operations/s | 904,535 operations/s |

### Enumeration

The 0% case returns no entries. This report omits its entries-per-second value because that rate does not apply.

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| selectivity=1 | 100,672,117 entries/s | 61,666,801 entries/s | 133,294 entries/s |
| selectivity=10 | 124,300,715 entries/s | 104,613,836 entries/s | 763,820 entries/s |
| selectivity=100 | 212,436,748 entries/s | 96,967,499 entries/s | 1,545,208 entries/s |

### Ordered operations

Redis does not provide the required ordered-cursor API. The 0% range case returns no entries. This report omits its entries-per-second value.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| full-forward | 228,864,487 entries/s | 102,720,296 entries/s |
| full-reverse | 93,289,830 entries/s | 108,639,501 entries/s |
| range/selectivity=1 | 105,993,709 entries/s | 65,189,003 entries/s |
| range/selectivity=10 | 124,584,109 entries/s | 97,191,893 entries/s |
| range/selectivity=100 | 216,490,108 entries/s | 101,287,351 entries/s |
| seek-and-read=1 | 2,313,069 entries/s | 952,208 entries/s |
| seek-and-read=10 | 19,184,291 entries/s | 8,792,212 entries/s |
| seek-and-read=100 | 66,517,003 entries/s | 52,057,139 entries/s |
| seek-and-read=1000 | 88,787,080 entries/s | 92,158,263 entries/s |

### Access distribution

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| records=10000/hot-80-20 | 2,449,683 reads/s | 925,847 reads/s | 138,312 reads/s |
| records=10000/uniform | 2,290,911 reads/s | 903,460 reads/s | 137,816 reads/s |
| records=100000/hot-80-20 | 1,720,275 reads/s | 1,233,687 reads/s | 131,982 reads/s |
| records=100000/uniform | 1,471,423 reads/s | 1,092,114 reads/s | 130,959 reads/s |

### Collections

KVLite and bbolt use native buckets. Redis uses logical key prefixes.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 60,513 keys/s | 29,245 keys/s | 38,186 keys/s |
| write/collections=1/depth=3 | 57,756 keys/s | 35,998 keys/s | 39,619 keys/s |
| write/collections=100/depth=1 | 61,805 keys/s | 40,392 keys/s | 41,867 keys/s |
| write/collections=100/depth=3 | 51,435 keys/s | 28,243 keys/s | 38,076 keys/s |
| read/collections=1/depth=1 | 1,532,636 reads/s | 821,198 reads/s | 136,009 reads/s |
| read/collections=1/depth=3 | 928,717 reads/s | 624,638 reads/s | 133,612 reads/s |
| read/collections=100/depth=1 | 1,195,270 reads/s | 804,210 reads/s | 136,838 reads/s |
| read/collections=100/depth=3 | 755,875 reads/s | 679,882 reads/s | 136,935 reads/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 1,188,046 keys/s | 1,019,959 keys/s | 753,366 keys/s |
| write/collections=1/depth=3 | 1,487,503 keys/s | 1,020,563 keys/s | 630,291 keys/s |
| write/collections=100/depth=1 | 1,675,712 keys/s | 1,818,201 keys/s | 720,662 keys/s |
| write/collections=100/depth=3 | 1,817,954 keys/s | 1,887,854 keys/s | 626,297 keys/s |

### Latency

The large scale reports one, eight, and 32 clients.

#### Durable

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Read | 1 | KVLite | 416 ns | 500 ns | 625 ns | 466.17 µs |
| Read | 1 | bbolt | 417 ns | 834 ns | 1.58 µs | 857.26 µs |
| Read | 1 | redis | 6.71 µs | 8.21 µs | 13.96 µs | 4.83 ms |
| Read | 8 | KVLite | 625 ns | 1.12 µs | 2.04 µs | 14.47 ms |
| Read | 8 | bbolt | 542 ns | 1.62 µs | 7.54 µs | 23.20 ms |
| Read | 8 | redis | 29.58 µs | 93.67 µs | 169.46 µs | 8.37 ms |
| Read | 32 | KVLite | 708 ns | 1.79 µs | 168.88 µs | 22.00 ms |
| Read | 32 | bbolt | 625 ns | 26.54 µs | 180.99 µs | 29.19 ms |
| Read | 32 | redis | 77.46 µs | 319.10 µs | 617.62 µs | 10.02 ms |
| Update | 1 | KVLite | 579.96 µs | 1.12 ms | 2.20 ms | 14.67 ms |
| Update | 1 | bbolt | 1.30 ms | 5.67 ms | 8.68 ms | 323.56 ms |
| Update | 1 | redis | 562.87 µs | 825.25 µs | 1.39 ms | 8.51 ms |
| Update | 8 | KVLite | 641.79 µs | 1.02 ms | 1.65 ms | 6.09 ms |
| Update | 8 | bbolt | 18.84 ms | 22.44 ms | 24.42 ms | 36.88 ms |
| Update | 8 | redis | 693.59 µs | 1.23 ms | 1.45 ms | 5.94 ms |
| Update | 32 | KVLite | 701.13 µs | 1.42 ms | 2.22 ms | 5.31 ms |
| Update | 32 | bbolt | 20.54 ms | 23.86 ms | 25.41 ms | 27.36 ms |
| Update | 32 | redis | 1.04 ms | 1.58 ms | 2.29 ms | 4.65 ms |
| Mixed | 1 | KVLite | 875 ns | 290.62 µs | 705.92 µs | 8.67 ms |
| Mixed | 1 | bbolt | 792 ns | 279.08 µs | 1.26 ms | 9.98 ms |
| Mixed | 1 | redis | 8.62 µs | 473.46 µs | 1.71 ms | 12.56 ms |
| Mixed | 8 | KVLite | 708 ns | 674.88 µs | 950.00 µs | 6.77 ms |
| Mixed | 8 | bbolt | 2.38 µs | 5.24 ms | 19.49 ms | 29.10 ms |
| Mixed | 8 | redis | 80.17 µs | 1.73 ms | 2.25 ms | 9.36 ms |
| Mixed | 32 | KVLite | 542 ns | 801.13 µs | 1.41 ms | 10.42 ms |
| Mixed | 32 | bbolt | 2.54 µs | 7.47 ms | 20.41 ms | 25.55 ms |
| Mixed | 32 | redis | 1.11 ms | 2.42 ms | 4.06 ms | 14.91 ms |

#### No commit sync

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Update | 1 | KVLite | 2.21 µs | 4.33 µs | 7.25 µs | 268.66 µs |
| Update | 1 | bbolt | 10.96 µs | 18.25 µs | 26.00 µs | 3.69 ms |
| Update | 1 | redis | 9.42 µs | 13.33 µs | 18.08 µs | 635.04 µs |
| Update | 8 | KVLite | 2.29 µs | 4.75 µs | 9.46 µs | 9.14 ms |
| Update | 8 | bbolt | 11.21 µs | 21.96 µs | 3.92 ms | 9.71 ms |
| Update | 8 | redis | 32.42 µs | 81.25 µs | 125.04 µs | 3.72 ms |
| Update | 32 | KVLite | 2.29 µs | 5.08 µs | 3.09 ms | 10.11 ms |
| Update | 32 | bbolt | 11.21 µs | 3.46 ms | 6.99 ms | 11.98 ms |
| Update | 32 | redis | 115.42 µs | 332.58 µs | 579.16 µs | 6.28 ms |
| Mixed | 1 | KVLite | 542 ns | 1.79 µs | 2.71 µs | 182.46 µs |
| Mixed | 1 | bbolt | 625 ns | 9.50 µs | 13.33 µs | 3.46 ms |
| Mixed | 1 | redis | 6.75 µs | 9.21 µs | 13.83 µs | 2.92 ms |
| Mixed | 8 | KVLite | 542 ns | 2.04 µs | 5.83 µs | 10.33 ms |
| Mixed | 8 | bbolt | 708 ns | 11.75 µs | 30.17 µs | 16.63 ms |
| Mixed | 8 | redis | 30.54 µs | 95.17 µs | 188.20 µs | 6.15 ms |
| Mixed | 32 | KVLite | 542 ns | 2.12 µs | 47.17 µs | 23.21 ms |
| Mixed | 32 | bbolt | 708 ns | 12.83 µs | 1.05 ms | 21.87 ms |
| Mixed | 32 | redis | 89.54 µs | 328.00 µs | 607.80 µs | 6.06 ms |

### Life cycle

Redis does not have the same embedded life-cycle boundary.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| close-after-writes | 2.07 ms | 18.17 µs |
| create-load-close | 35.03 ms | 57.57 ms |
| open-clean | 48.04 µs | 2.91 ms |
| recover-after-process-kill | 60.58 ms | 56.55 ms |

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
| read/random/value=32/clients=1 | 1,359,872 B | 2,097,152 B | 750,401 B |
| read/random/value=128/clients=1 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| read/random/value=1024/clients=1 | 41,619,456 B | 35,549,184 B | 10,690,401 B |
| read/random/value=3072/clients=1 | 41,627,648 B | 58,118,144 B | 31,170,401 B |
| read/sequential/hits=100/value=128/clients=1 | 3,485,696 B | 4,194,304 B | 1,720,401 B |
| update/random/grow=32-1024/clients=1 | 3,804,279 B | 4,194,304 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 22,667,941 B | 33,640,448 B | 11,443,301 B |
| update/random/shrink=1024-32/clients=1 | 41,751,464 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,695,324 B | 35,549,184 B | 11,443,301 B |
| update/random/value=32/clients=1 | 1,418,880 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 3,544,704 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 3,849,579 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 3,814,429 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 3,726,104 B | 8,388,608 B | 3,440,691 B |
| update/random/value=128/clients=8 | 3,604,429 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 3,741,779 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 3,595,779 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 3,645,754 B | 4,194,304 B | 3,443,301 B |
| update/random/value=1024/clients=1 | 41,678,464 B | 35,549,184 B | 11,759,401 B |
| update/random/value=3072/clients=1 | 41,686,656 B | 58,118,144 B | 34,287,401 B |
| update/sequential/value=128/clients=1/batch=10 | 3,642,354 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 3,600,679 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 3,596,929 B | 4,194,304 B | 3,440,691 B |
| insert/random/value=128/clients=1 | 7,050,177 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 6,493,351 B | 8,388,608 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 9,071,419 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 9,070,585 B | 8,388,608 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 6,218,742 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 6,698,810 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 4,301,396 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 7,558,182 B | 8,388,608 B | 3,443,301 B |
| insert/sequential/value=128/clients=1 | 7,436,833 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 8,799,850 B | 8,388,608 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 6,221,385 B | 8,388,608 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,259,852 B | 8,388,608 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 3,770,957 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 3,663,232 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 3,650,132 B | 4,194,304 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 3,515,105 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 3,505,405 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 3,503,405 B | 4,194,304 B | 1,806,401 B |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 3,804,279 B | 2,469,888 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 22,667,941 B | 19,394,560 B | 11,443,301 B |
| update/random/shrink=1024-32/clients=1 | 41,751,464 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,695,324 B | 35,549,184 B | 11,443,301 B |
| update/random/value=32/clients=1 | 1,418,880 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 3,544,704 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 3,849,579 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 3,814,429 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 3,726,104 B | 6,082,560 B | 3,440,691 B |
| update/random/value=128/clients=8 | 3,674,504 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 3,814,429 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 3,674,504 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 3,814,429 B | 4,194,304 B | 3,443,301 B |
| update/random/value=1024/clients=1 | 41,678,464 B | 35,549,184 B | 11,759,401 B |
| update/random/value=3072/clients=1 | 41,686,656 B | 58,118,144 B | 34,287,401 B |
| update/sequential/value=128/clients=1/batch=10 | 3,642,354 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 3,600,679 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 3,596,929 B | 4,194,304 B | 3,440,691 B |
| insert/random/value=128/clients=1 | 7,050,177 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 6,493,351 B | 5,910,528 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 9,071,419 B | 6,303,744 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 9,070,585 B | 7,663,616 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 7,243,678 B | 4,268,032 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 8,913,392 B | 6,287,360 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 7,262,924 B | 4,272,128 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 8,934,924 B | 6,279,168 B | 3,443,301 B |
| insert/sequential/value=128/clients=1 | 7,436,833 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 8,799,850 B | 6,971,392 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 6,221,385 B | 6,971,392 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,259,852 B | 6,971,392 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 3,770,957 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 3,770,957 B | 4,431,872 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 3,770,957 B | 4,923,392 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 3,515,105 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 3,515,105 B | 4,517,888 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 3,515,105 B | 4,202,496 B | 1,806,401 B |
