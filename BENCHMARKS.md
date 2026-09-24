# KVLite benchmarks

This report compares KVLite with bbolt and Redis at the public call boundary. It contains the median of 15 measured runs from the complete `large --workloads=all --storage=volume` profile.

| Item | Value |
| --- | --- |
| Run time | `2026-09-23T23:54:21Z` |
| Elapsed time | 1 hour 55 minutes 16.53 seconds |
| KVLite | `4848f2db2c41a96026a912e0634a581d278d24cd` from a clean working tree |
| bbolt | `v1.4.3` |
| Redis | `8.8.0` |
| Platform | Linux `arm64`, four CPUs in containers |
| Profile | `large --workloads=all --storage=volume` |
| Warm-up | One complete unrecorded round |
| Measurement | 15 measured rounds |

## Results at a glance

A higher throughput value is better. A lower latency or life-cycle value is better. This section shows important cases where KVLite had the best median.

| Category | Mode and workload | KVLite | bbolt | Redis | Highlight |
| --- | --- | ---: | ---: | ---: | --- |
| Point read | Read-only, random, 128-byte value, eight clients | 3,473,763 keys/s | 839,698 keys/s | 178,184 keys/s | **KVLite: 4.14x bbolt; 19.50x Redis** |
| Point-read p99 | Read-only, eight clients | 1.75 µs | 7.50 µs | 181.09 µs | **KVLite: 4.29x advantage over bbolt; 103.48x over Redis** |
| Point update | Durable, random, 128-byte value, one client, batch of 100 | 83,640 keys/s | 17,253 keys/s | 56,312 keys/s | **KVLite: 4.85x bbolt; 1.49x Redis** |
| Point insert | Durable, random, 128-byte value, eight clients | 7,365 keys/s | 512 keys/s | 5,273 keys/s | **KVLite: 14.40x bbolt; 1.40x Redis** |
| Mixed work | Durable, 95% reads, eight clients | 109,519 operations/s | 10,505 operations/s | 16,825 operations/s | **KVLite: 10.43x bbolt; 6.51x Redis** |
| Mixed work | No commit sync, 95% reads, eight clients | 825,681 operations/s | 329,072 operations/s | 154,136 operations/s | **KVLite: 2.51x bbolt; 5.36x Redis** |
| Clean open | Life cycle | 69.79 µs | 2.86 ms | Not comparable | **KVLite: 41.04x advantage** |

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
- KVLite: `4848f2db2c41a96026a912e0634a581d278d24cd` from a clean working tree
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

- KVLite led 65 of 94 durable cases. bbolt led 11. Redis led 18.
- KVLite led 36 of 52 no-commit-sync cases. bbolt led 2. Redis led 14.
- Each count uses one primary metric for each comparable case. It excludes storage size and duplicate throughput metrics.
- KVLite led every point-read throughput case.
- KVLite led the durable 128-byte same-size point update cases at one, eight, and 32 clients. It also led several single-client update batches. Redis led the concurrent random update batches.
- KVLite led durable random point inserts at eight and 32 clients. Redis led most random insert batches.
- KVLite led durable acknowledged mixed work at all tested client counts and read shares.
- KVLite led most no-commit-sync acknowledged mixed work and single-key update cases.
- KVLite and bbolt each led some ordered-operation cases. The 10% enumeration case and the 100% ordered-range case were within 1%.
- KVLite opened a clean database 41.04x faster than bbolt.

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
| read/random/hits=0/misses=above/value=128/clients=1 | 3,990,330 keys/s | 1,548,278 keys/s | 118,076 keys/s |
| read/random/hits=0/misses=below/value=128/clients=1 | 3,877,169 keys/s | 1,565,983 keys/s | 115,486 keys/s |
| read/random/hits=0/misses=between/value=128/clients=1 | 3,121,700 keys/s | 1,490,391 keys/s | 117,536 keys/s |
| read/random/hits=50/value=128/clients=1 | 2,436,097 keys/s | 1,289,086 keys/s | 124,849 keys/s |
| read/random/hits=100/value=128/clients=8 | 3,473,763 keys/s | 839,698 keys/s | 178,184 keys/s |
| read/random/hits=100/value=128/clients=32 | 2,705,506 keys/s | 874,326 keys/s | 221,519 keys/s |
| read/random/value=32/clients=1 | 2,170,196 keys/s | 1,184,187 keys/s | 137,292 keys/s |
| read/random/value=128/clients=1 | 1,899,861 keys/s | 1,225,314 keys/s | 127,829 keys/s |
| read/random/value=1024/clients=1 | 1,139,870 keys/s | 1,016,913 keys/s | 119,114 keys/s |
| read/random/value=3072/clients=1 | 947,457 keys/s | 891,007 keys/s | 100,028 keys/s |
| read/sequential/hits=100/value=128/clients=1 | 2,535,719 keys/s | 1,238,843 keys/s | 134,150 keys/s |
| update/random/grow=32-1024/clients=1 | 555 keys/s | 552 keys/s | 727 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 21,355 keys/s | 11,780 keys/s | 50,259 keys/s |
| update/random/shrink=1024-32/clients=1 | 768 keys/s | 570 keys/s | 1,054 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 45,489 keys/s | 11,493 keys/s | 55,464 keys/s |
| update/random/value=32/clients=1 | 541 keys/s | 629 keys/s | 780 keys/s |
| update/random/value=128/clients=1 | 922 keys/s | 674 keys/s | 593 keys/s |
| update/random/value=128/clients=1/batch=10 | 12,042 keys/s | 4,290 keys/s | 10,775 keys/s |
| update/random/value=128/clients=1/batch=100 | 83,640 keys/s | 17,253 keys/s | 56,312 keys/s |
| update/random/value=128/clients=1/batch=1000 | 324,307 keys/s | 86,973 keys/s | 344,788 keys/s |
| update/random/value=128/clients=8 | 10,945 keys/s | 495 keys/s | 8,132 keys/s |
| update/random/value=128/clients=8/batch=100 | 180,307 keys/s | 17,578 keys/s | 206,963 keys/s |
| update/random/value=128/clients=32 | 30,544 keys/s | 1,897 keys/s | 17,986 keys/s |
| update/random/value=128/clients=32/batch=100 | 322,943 keys/s | 17,481 keys/s | 410,558 keys/s |
| update/random/value=1024/clients=1 | 1,057 keys/s | 659 keys/s | 813 keys/s |
| update/random/value=3072/clients=1 | 574 keys/s | 706 keys/s | 993 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 13,980 keys/s | 6,338 keys/s | 7,132 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 109,003 keys/s | 39,141 keys/s | 67,755 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 456,063 keys/s | 273,067 keys/s | 351,738 keys/s |
| insert/random/value=128/clients=1 | 1,176 keys/s | 681 keys/s | 1,440 keys/s |
| insert/random/value=128/clients=1/batch=10 | 8,998 keys/s | 2,064 keys/s | 8,426 keys/s |
| insert/random/value=128/clients=1/batch=100 | 53,633 keys/s | 15,678 keys/s | 64,264 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 230,475 keys/s | 144,529 keys/s | 341,629 keys/s |
| insert/random/value=128/clients=8 | 7,365 keys/s | 512 keys/s | 5,273 keys/s |
| insert/random/value=128/clients=8/batch=100 | 128,839 keys/s | 18,224 keys/s | 208,474 keys/s |
| insert/random/value=128/clients=32 | 21,052 keys/s | 2,001 keys/s | 14,963 keys/s |
| insert/random/value=128/clients=32/batch=100 | 250,331 keys/s | 18,906 keys/s | 390,185 keys/s |
| insert/sequential/value=128/clients=1 | 858 keys/s | 751 keys/s | 1,496 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 9,620 keys/s | 4,867 keys/s | 8,227 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 86,939 keys/s | 28,112 keys/s | 60,600 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 432,260 keys/s | 208,162 keys/s | 347,901 keys/s |
| mixed/read=50/value=128/clients=1 | 3,010 operations/s | 1,460 operations/s | 1,197 operations/s |
| mixed/read=50/value=128/clients=8 | 16,431 operations/s | 1,003 operations/s | 4,093 operations/s |
| mixed/read=50/value=128/clients=32 | 44,399 operations/s | 3,707 operations/s | 15,653 operations/s |
| mixed/read=95/value=128/clients=1 | 29,570 operations/s | 12,517 operations/s | 14,209 operations/s |
| mixed/read=95/value=128/clients=8 | 109,519 operations/s | 10,505 operations/s | 16,825 operations/s |
| mixed/read=95/value=128/clients=32 | 247,226 operations/s | 36,188 operations/s | 25,201 operations/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 96,657 keys/s | 58,214 keys/s | 83,347 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 116,755 keys/s | 182,864 keys/s | 445,284 keys/s |
| update/random/shrink=1024-32/clients=1 | 161,581 keys/s | 63,937 keys/s | 96,325 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 258,608 keys/s | 201,325 keys/s | 1,065,257 keys/s |
| update/random/value=32/clients=1 | 286,062 keys/s | 45,662 keys/s | 91,739 keys/s |
| update/random/value=128/clients=1 | 233,450 keys/s | 59,865 keys/s | 95,616 keys/s |
| update/random/value=128/clients=1/batch=10 | 581,920 keys/s | 155,019 keys/s | 426,901 keys/s |
| update/random/value=128/clients=1/batch=100 | 640,322 keys/s | 286,701 keys/s | 951,836 keys/s |
| update/random/value=128/clients=1/batch=1000 | 813,254 keys/s | 453,208 keys/s | 974,243 keys/s |
| update/random/value=128/clients=8 | 213,047 keys/s | 45,406 keys/s | 154,891 keys/s |
| update/random/value=128/clients=8/batch=100 | 541,186 keys/s | 278,938 keys/s | 989,143 keys/s |
| update/random/value=128/clients=32 | 200,132 keys/s | 43,692 keys/s | 171,578 keys/s |
| update/random/value=128/clients=32/batch=100 | 519,372 keys/s | 302,142 keys/s | 927,748 keys/s |
| update/random/value=1024/clients=1 | 118,449 keys/s | 62,464 keys/s | 82,490 keys/s |
| update/random/value=3072/clients=1 | 111,587 keys/s | 56,818 keys/s | 70,278 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 1,252,931 keys/s | 516,805 keys/s | 457,121 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 1,623,154 keys/s | 1,607,809 keys/s | 1,014,182 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 1,707,886 keys/s | 1,818,055 keys/s | 1,061,371 keys/s |
| insert/random/value=128/clients=1 | 214,980 keys/s | 50,369 keys/s | 97,265 keys/s |
| insert/random/value=128/clients=1/batch=10 | 227,836 keys/s | 134,058 keys/s | 447,268 keys/s |
| insert/random/value=128/clients=1/batch=100 | 317,730 keys/s | 245,415 keys/s | 932,773 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 619,624 keys/s | 518,874 keys/s | 945,204 keys/s |
| insert/random/value=128/clients=8 | 121,730 keys/s | 38,821 keys/s | 143,063 keys/s |
| insert/random/value=128/clients=8/batch=100 | 280,365 keys/s | 199,808 keys/s | 735,184 keys/s |
| insert/random/value=128/clients=32 | 127,908 keys/s | 35,471 keys/s | 157,728 keys/s |
| insert/random/value=128/clients=32/batch=100 | 257,195 keys/s | 245,496 keys/s | 920,646 keys/s |
| insert/sequential/value=128/clients=1 | 204,131 keys/s | 50,776 keys/s | 100,431 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 612,954 keys/s | 349,762 keys/s | 430,969 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 1,809,538 keys/s | 1,183,266 keys/s | 905,390 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 2,555,965 keys/s | 1,792,455 keys/s | 991,835 keys/s |
| mixed/read=50/value=128/clients=1 | 480,492 operations/s | 118,237 operations/s | 112,942 operations/s |
| mixed/read=50/value=128/clients=8 | 351,934 operations/s | 77,883 operations/s | 152,580 operations/s |
| mixed/read=50/value=128/clients=32 | 340,065 operations/s | 69,224 operations/s | 171,185 operations/s |
| mixed/read=95/value=128/clients=1 | 1,147,993 operations/s | 539,694 operations/s | 130,935 operations/s |
| mixed/read=95/value=128/clients=8 | 825,681 operations/s | 329,072 operations/s | 154,136 operations/s |
| mixed/read=95/value=128/clients=32 | 718,551 operations/s | 296,898 operations/s | 188,471 operations/s |

### Transactions

#### Read-only

| Keys per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 1,295,489 keys/s | 855,248 keys/s | 129,646 keys/s |
| 10 | 1,767,939 keys/s | 1,513,769 keys/s | 760,886 keys/s |
| 100 | 1,790,478 keys/s | 1,731,049 keys/s | 1,548,891 keys/s |
| 1,000 | 1,724,241 keys/s | 1,641,336 keys/s | 1,846,692 keys/s |

#### Durable mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 9,692 operations/s | 5,829 operations/s | 9,163 operations/s |
| 95% | 100 | 101,150 operations/s | 57,024 operations/s | 117,871 operations/s |
| 95% | 1,000 | 533,708 operations/s | 263,451 operations/s | 419,806 operations/s |
| 50% | 10 | 11,337 operations/s | 4,856 operations/s | 8,602 operations/s |
| 50% | 100 | 95,871 operations/s | 21,465 operations/s | 76,854 operations/s |
| 50% | 1,000 | 344,079 operations/s | 103,676 operations/s | 402,902 operations/s |

#### No-commit-sync mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 878,167 operations/s | 450,000 operations/s | 536,125 operations/s |
| 95% | 100 | 1,153,792 operations/s | 952,969 operations/s | 1,071,250 operations/s |
| 95% | 1,000 | 1,240,916 operations/s | 1,169,668 operations/s | 1,119,879 operations/s |
| 50% | 10 | 739,915 operations/s | 237,005 operations/s | 462,462 operations/s |
| 50% | 100 | 906,971 operations/s | 429,632 operations/s | 822,207 operations/s |
| 50% | 1,000 | 993,937 operations/s | 614,305 operations/s | 957,195 operations/s |

### Enumeration

The 0% case returns no entries. This report omits its entries-per-second value because that rate does not apply.

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| selectivity=1 | 7,510,270 entries/s | 6,832,300 entries/s | 112,202 entries/s |
| selectivity=10 | 7,003,586 entries/s | 6,966,185 entries/s | 566,654 entries/s |
| selectivity=100 | 2,984,553 entries/s | 6,704,035 entries/s | 1,190,098 entries/s |

### Ordered operations

Redis does not provide the required ordered-cursor API. The 0% range case returns no entries. This report omits its entries-per-second value.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| full-forward | 7,648,856 entries/s | 8,252,119 entries/s |
| full-reverse | 6,980,140 entries/s | 7,773,348 entries/s |
| range/selectivity=1 | 6,982,616 entries/s | 7,690,503 entries/s |
| range/selectivity=10 | 7,373,826 entries/s | 7,913,922 entries/s |
| range/selectivity=100 | 7,741,820 entries/s | 7,767,135 entries/s |
| seek-and-read=1 | 860,991 entries/s | 940,838 entries/s |
| seek-and-read=10 | 5,125,976 entries/s | 5,087,262 entries/s |
| seek-and-read=100 | 6,641,910 entries/s | 7,495,139 entries/s |
| seek-and-read=1000 | 6,959,533 entries/s | 7,708,531 entries/s |

### Access distribution

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| records=10000/hot-80-20 | 1,942,080 reads/s | 843,741 reads/s | 134,430 reads/s |
| records=10000/uniform | 1,832,158 reads/s | 941,189 reads/s | 130,912 reads/s |
| records=100000/hot-80-20 | 1,315,402 reads/s | 1,031,034 reads/s | 130,435 reads/s |
| records=100000/uniform | 1,157,560 reads/s | 907,281 reads/s | 127,141 reads/s |

### Collections

KVLite and bbolt use native buckets. Redis uses logical key prefixes.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 50,811 keys/s | 35,828 keys/s | 49,155 keys/s |
| write/collections=1/depth=3 | 63,806 keys/s | 35,807 keys/s | 49,476 keys/s |
| write/collections=100/depth=1 | 56,452 keys/s | 37,808 keys/s | 49,234 keys/s |
| write/collections=100/depth=3 | 60,310 keys/s | 39,872 keys/s | 49,933 keys/s |
| read/collections=1/depth=1 | 1,295,818 reads/s | 742,162 reads/s | 129,128 reads/s |
| read/collections=1/depth=3 | 730,984 reads/s | 543,157 reads/s | 130,020 reads/s |
| read/collections=100/depth=1 | 1,038,574 reads/s | 705,352 reads/s | 134,261 reads/s |
| read/collections=100/depth=3 | 616,794 reads/s | 556,563 reads/s | 131,724 reads/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 1,336,190 keys/s | 872,548 keys/s | 619,678 keys/s |
| write/collections=1/depth=3 | 1,410,812 keys/s | 919,757 keys/s | 533,350 keys/s |
| write/collections=100/depth=1 | 1,671,080 keys/s | 1,611,754 keys/s | 666,039 keys/s |
| write/collections=100/depth=3 | 1,585,873 keys/s | 1,719,907 keys/s | 624,661 keys/s |

### Latency

The large profile reports one, eight, and 32 clients.

#### Durable

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Read | 1 | KVLite | 458 ns | 625 ns | 792 ns | 2.12 ms |
| Read | 1 | bbolt | 541 ns | 1.08 µs | 1.92 µs | 3.02 ms |
| Read | 1 | redis | 6.88 µs | 8.63 µs | 16.42 µs | 4.80 ms |
| Read | 8 | KVLite | 625 ns | 1.04 µs | 1.75 µs | 26.90 ms |
| Read | 8 | bbolt | 666 ns | 1.67 µs | 7.50 µs | 23.71 ms |
| Read | 8 | redis | 30.67 µs | 97.59 µs | 181.09 µs | 8.10 ms |
| Read | 32 | KVLite | 584 ns | 959 ns | 1.79 µs | 45.89 ms |
| Read | 32 | bbolt | 667 ns | 5.42 µs | 188.70 µs | 35.04 ms |
| Read | 32 | redis | 83.42 µs | 345.76 µs | 676.66 µs | 12.30 ms |
| Update | 1 | KVLite | 701.97 µs | 2.12 ms | 3.72 ms | 305.25 ms |
| Update | 1 | bbolt | 1.63 ms | 6.58 ms | 9.32 ms | 340.57 ms |
| Update | 1 | redis | 1.10 ms | 3.10 ms | 4.61 ms | 271.42 ms |
| Update | 8 | KVLite | 767.72 µs | 1.39 ms | 2.55 ms | 20.54 ms |
| Update | 8 | bbolt | 15.66 ms | 19.04 ms | 23.38 ms | 37.69 ms |
| Update | 8 | redis | 1.23 ms | 3.28 ms | 4.40 ms | 15.46 ms |
| Update | 32 | KVLite | 914.06 µs | 1.94 ms | 3.03 ms | 7.36 ms |
| Update | 32 | bbolt | 16.57 ms | 20.00 ms | 22.56 ms | 28.24 ms |
| Update | 32 | redis | 1.35 ms | 3.30 ms | 4.13 ms | 6.96 ms |
| Mixed | 1 | KVLite | 917 ns | 417.26 µs | 782.55 µs | 26.21 ms |
| Mixed | 1 | bbolt | 917 ns | 845.89 µs | 1.45 ms | 71.57 ms |
| Mixed | 1 | redis | 8.67 µs | 512.75 µs | 1.45 ms | 110.01 ms |
| Mixed | 8 | KVLite | 750 ns | 688.60 µs | 1.05 ms | 18.26 ms |
| Mixed | 8 | bbolt | 1.29 µs | 4.80 ms | 16.56 ms | 34.26 ms |
| Mixed | 8 | redis | 84.96 µs | 2.16 ms | 3.69 ms | 264.18 ms |
| Mixed | 32 | KVLite | 667 ns | 887.05 µs | 1.69 ms | 16.00 ms |
| Mixed | 32 | bbolt | 1.33 µs | 7.37 ms | 17.23 ms | 23.88 ms |
| Mixed | 32 | redis | 1.40 ms | 2.97 ms | 4.77 ms | 201.97 ms |

#### No commit sync

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Update | 1 | KVLite | 2.33 µs | 5.08 µs | 8.96 µs | 587.53 µs |
| Update | 1 | bbolt | 11.38 µs | 20.25 µs | 30.58 µs | 3.25 ms |
| Update | 1 | redis | 9.50 µs | 12.79 µs | 20.88 µs | 607.82 µs |
| Update | 8 | KVLite | 2.33 µs | 5.38 µs | 14.12 µs | 8.06 ms |
| Update | 8 | bbolt | 11.54 µs | 23.83 µs | 4.22 ms | 9.91 ms |
| Update | 8 | redis | 34.04 µs | 82.58 µs | 153.04 µs | 3.66 ms |
| Update | 32 | KVLite | 2.33 µs | 5.71 µs | 2.99 ms | 10.23 ms |
| Update | 32 | bbolt | 11.54 µs | 3.35 ms | 6.96 ms | 11.53 ms |
| Update | 32 | redis | 132.34 µs | 306.88 µs | 494.88 µs | 4.36 ms |
| Mixed | 1 | KVLite | 583 ns | 2.04 µs | 3.58 µs | 519.12 µs |
| Mixed | 1 | bbolt | 666 ns | 10.38 µs | 15.67 µs | 2.32 ms |
| Mixed | 1 | redis | 6.92 µs | 9.96 µs | 20.17 µs | 3.18 ms |
| Mixed | 8 | KVLite | 583 ns | 2.25 µs | 8.83 µs | 10.61 ms |
| Mixed | 8 | bbolt | 750 ns | 12.42 µs | 32.54 µs | 17.50 ms |
| Mixed | 8 | redis | 31.92 µs | 97.13 µs | 194.50 µs | 6.10 ms |
| Mixed | 32 | KVLite | 542 ns | 2.29 µs | 118.47 µs | 17.58 ms |
| Mixed | 32 | bbolt | 709 ns | 13.62 µs | 1.79 ms | 21.47 ms |
| Mixed | 32 | redis | 92.25 µs | 339.93 µs | 611.59 µs | 6.52 ms |

### Life cycle

Redis does not have the same embedded life-cycle boundary.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| close-after-writes | 2.19 ms | 26.42 µs |
| create-load-close | 42.15 ms | 63.63 ms |
| open-clean | 69.79 µs | 2.86 ms |
| recover-after-process-kill | 54.82 ms | 54.10 ms |

### Persistent bytes

`persistent-B` is the KVLite database plus WAL, the bbolt database, or the Redis AOF. These files have different maintenance and compaction rules.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
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
| update/random/value=32/clients=1 | 1,414,776 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 3,540,600 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 3,845,475 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 3,810,325 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 3,722,000 B | 8,388,608 B | 3,440,691 B |
| update/random/value=128/clients=8 | 3,600,750 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 3,741,700 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 3,592,225 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 3,659,025 B | 4,194,304 B | 3,443,301 B |
| update/random/value=1024/clients=1 | 41,674,360 B | 35,549,184 B | 11,759,401 B |
| update/random/value=3072/clients=1 | 41,682,552 B | 58,118,144 B | 34,287,401 B |
| update/sequential/value=128/clients=1/batch=10 | 3,638,250 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 3,596,575 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 3,592,825 B | 4,194,304 B | 3,440,691 B |
| insert/random/value=128/clients=1 | 6,813,404 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 8,546,083 B | 8,388,608 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 8,762,411 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 9,056,868 B | 8,388,608 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 5,627,998 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 7,285,868 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 7,867,571 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 6,155,073 B | 8,388,608 B | 3,443,301 B |
| insert/sequential/value=128/clients=1 | 7,093,022 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 8,768,334 B | 8,388,608 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,807,299 B | 8,388,608 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,216,290 B | 8,388,608 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 3,766,853 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 3,661,378 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 3,647,278 B | 4,194,304 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 3,511,001 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 3,501,351 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 3,499,376 B | 4,194,304 B | 1,806,401 B |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 2,902,236 B | 2,469,888 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 24,389,244 B | 19,394,560 B | 11,443,301 B |
| update/random/shrink=1024-32/clients=1 | 41,749,360 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,711,220 B | 35,549,184 B | 11,443,301 B |
| update/random/value=32/clients=1 | 1,414,776 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 3,540,600 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 3,845,475 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 3,810,325 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 3,722,000 B | 6,082,560 B | 3,440,691 B |
| update/random/value=128/clients=8 | 3,670,400 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 3,810,325 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 3,670,400 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 3,810,325 B | 4,194,304 B | 3,443,301 B |
| update/random/value=1024/clients=1 | 41,674,360 B | 35,549,184 B | 11,759,401 B |
| update/random/value=3072/clients=1 | 41,682,552 B | 58,118,144 B | 34,287,401 B |
| update/sequential/value=128/clients=1/batch=10 | 3,638,250 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 3,596,575 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 3,592,825 B | 4,194,304 B | 3,440,691 B |
| insert/random/value=128/clients=1 | 6,813,404 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 8,546,083 B | 5,910,528 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 8,762,411 B | 6,303,744 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 9,056,868 B | 7,663,616 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 6,532,016 B | 4,263,936 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 8,590,357 B | 6,287,360 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 6,532,110 B | 4,268,032 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 8,578,005 B | 6,279,168 B | 3,443,301 B |
| insert/sequential/value=128/clients=1 | 7,093,022 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 8,768,334 B | 6,971,392 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 5,807,299 B | 6,971,392 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,216,290 B | 6,971,392 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 3,766,853 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 3,766,853 B | 4,202,496 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 3,766,853 B | 4,333,568 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 3,511,001 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 3,511,001 B | 4,198,400 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 3,511,001 B | 4,734,976 B | 1,806,401 B |
