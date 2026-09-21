# KVLite benchmarks

This report compares KVLite with bbolt and Redis at the caller API boundary. It presents the median of 10 fixed-work runs from what is now the large benchmark profile.

| Item | Value |
| --- | --- |
| Run time | `2026-09-15T13:49:48Z` |
| KVLite | `b14b20e319af7ed14b2236e4a01a2229aa926413` |
| bbolt | `v1.4.3` |
| Redis | `8.8.0` |
| Platform | Linux `arm64`, four CPUs in containers |

## Results at a glance

Except for latency and life-cycle rows, a higher value is better. For latency and life-cycle rows, a lower value is better. A winner includes every engine within 5% of the best median.

| Category | Mode | Representative workload | Winner | KVLite | bbolt | Redis |
| --- | --- | --- | --- | ---: | ---: | ---: |
| Point read | Read-only | Random, 128-byte value, one client | KVLite | 1,935,623 keys/s | 1,083,717 keys/s | 131,798 keys/s |
| Point insert | Durable | Random, 128-byte value, one client | Redis | 1,204 keys/s | 656 keys/s | 1,350 keys/s |
| Point update | Durable | Random, 128-byte value, one client | Redis | 497 keys/s | 510 keys/s | 620 keys/s |
| Batch insert | Durable | Random, 1,000 keys, 128-byte values | Redis | 251,310 keys/s | 166,938 keys/s | 303,309 keys/s |
| Batch update | Durable | Random, 1,000 keys, 128-byte values | Redis | 187,919 keys/s | 89,204 keys/s | 302,654 keys/s |
| Concurrent mixed work | Durable | 50% reads, 32 clients | KVLite | 31,344 operations/s | 3,800 operations/s | 23,122 operations/s |
| Point update | No commit sync | Random, 128-byte value, one client | KVLite | 182,670 keys/s | 64,873 keys/s | 90,652 keys/s |
| Batch insert | No commit sync | Sequential, 1,000 keys, 128-byte values | KVLite | 2,582,776 keys/s | 1,969,572 keys/s | 1,057,600 keys/s |
| Read transaction | Read-only | 1,000 keys per transaction | KVLite / Redis | 1,735,452 keys/s | 1,599,836 keys/s | 1,701,144 keys/s |
| Mixed transaction | Durable | 50% reads, 1,000 operations | Redis | 232,182 operations/s | 107,765 operations/s | 369,685 operations/s |
| Enumeration | Read-only | Full data set | bbolt | 2,654,568 entries/s | 6,651,070 entries/s | 970,483 entries/s |
| Ordered iteration | Read-only | Full forward iteration | KVLite | 7,794,076 entries/s | 7,192,822 entries/s | Not supported |
| Collections | Durable | 100 collections at depth 3 | KVLite | 62,174 keys/s | 46,322 keys/s | 55,852 keys/s |
| Clean open | Life cycle | Open an existing clean database | KVLite | 105.11 µs | 2.23 ms | Not comparable |
| Close after writes | Life cycle | Close after fixed writes | bbolt | 2.15 ms | 30.10 µs | Not comparable |

The 5% rule is a display rule. It is not a statistical confidence test. The generated result set does not contain enough variation data for a significance claim.

## How to read the results

- `ack-keys/s` counts keys whose API operation completed. A batch counts each key in the batch.
- `keys/s`, `reads/s`, `entries/s`, and `operations/s` name the logical work completed each second.
- The main latency tables compare p99 latency. The latency-detail tables also show p50, p95, and the maximum.
- A dash or `Not supported` means that the engine does not provide a matching operation.
- `Not comparable` means that the operation does not have the same application boundary for that engine.

## What the summary represents

The summary uses fixed cases from each workload class:

- Point operations use random access, 128-byte values, and one client.
- Concurrent mixed work uses 32 clients and an equal read and write split.
- Batch and transaction rows use the largest standard group of 1,000 operations.
- Enumeration uses the complete data set.
- Ordered iteration uses the full forward pass.
- The collection row uses 100 collections at the greatest tested depth.
- Life-cycle rows use their named fixed workloads.

The complete tables below show every measured case. They are the source for performance comparisons. The summary does not replace them.

## Fairness and durability

The suite compares the engines at the caller API boundary. It gives each engine the same fixed operation schedule and data set. It uses one transaction for each grouped operation. It validates the stored key count and values after each core case.

| Mode | KVLite | bbolt | Redis |
| --- | --- | --- | --- |
| Durable | `SyncFull` | `NoSync=false`, `NoGrowSync=false`, `NoFreelistSync=false` | AOF with `appendfsync always` |
| No commit sync | `SyncNone` | `NoSync=true`, `NoGrowSync=true`, `NoFreelistSync=false` | AOF with `appendfsync no` |

A successful durable write waits for the durability boundary of its engine. KVLite, bbolt, and Redis can each group concurrent durable writes. Their group limits and internal designs are not equal.

Redis includes a Go client, a server, and a network path. KVLite and bbolt are embedded libraries. The suite keeps this caller-visible cost because it measures the API used by an application. Redis does not provide an ordered cursor or an embedded open and close operation, so those cells have no Redis score.

See the [benchmark suite guide](benchmarks/kvbench/README.md) for the full fairness contract, matched API boundaries, validation rules, and durability details.

## Test environment

- CPU set: `0-3`
- Go: `go1.26.7 linux/arm64`
- KVLite: `b14b20e319af7ed14b2236e4a01a2229aa926413`
- bbolt: `v1.4.3`
- Redis: `8.8.0`, `jemalloc-5.3.0`
- Container system: OrbStack with Linux kernel `7.0.14-orbstack-00380-ga7e0a2dc9535`
- Storage driver: `overlay2`
- Host CPUs visible to the container system: 14
- CPUs assigned to each measured process and its Redis server: four

Other containers were active on the host. CPU assignment reduced direct CPU competition. Other host work could still affect the results.

## Method

The runner used fixed work and one process per engine. It changed engine and durability order between rounds. Each reported result is the median of 10 runs. Fixture loads used durable mode.

The timed sections included the selected API operations and their required acknowledgements. Automatic checkpoint work can occur during a timed sequence. A later API operation waits for that work. The final automatic checkpoint can finish after the timer stops. The timed sections excluded setup, final validation, close, and deferred sync unless a life-cycle case names that work. The latency tables report the median p50, p95, p99, and maximum values from the 10 runs.

The run used what is now the large profile. It did not include the one-million-key cases. All 60 measured engine runs passed their runtime and data checks.

## Interpretation

These statements apply only to this run and its tested workloads.

### KVLite results

- KVLite had the highest result in every acknowledged point-read case. In the representative 128-byte random read, it processed 1.79 times as many keys as bbolt and 14.69 times as many as Redis.
- KVLite had the highest result in every tested scale and access-distribution case.
- KVLite led the representative durable concurrent 50% read workload. It processed 31,344 operations/s. Redis processed 23,122 operations/s. bbolt processed 3,800 operations/s.
- KVLite led the no-commit-sync sequential batch insert case at every tested batch size. At 1,000 keys, it processed 1.31 times as many keys as bbolt and 2.44 times as many as Redis.
- KVLite led most ordered forward and range cases. It processed 7,794,076 entries/s in the full forward pass. bbolt processed 7,192,822 entries/s.
- KVLite opened the clean database in 105.11 µs. bbolt used 2.23 ms.

### Redis results

- Redis led the representative durable random point insert and point update cases.
- Redis led the representative durable random 1,000-key insert and update batches. In the update case, Redis processed 302,654 keys/s. KVLite processed 187,919 keys/s. bbolt processed 89,204 keys/s.
- Redis led the durable 50% read mixed transaction at 100 and 1,000 operations per transaction.
- Redis had the lowest durable update p99 latency at one, eight, and 32 clients.
- Redis led most random batch cases in no-commit-sync mode.

### bbolt results

- bbolt led enumeration at 10% and 100% selectivity. In the complete enumeration case, it processed 2.51 times as many entries as KVLite and 6.85 times as many as Redis.
- bbolt led full reverse iteration and the seek-and-read case that returned 10 entries.
- bbolt led the no-commit-sync case with 100 collections at depth 1.
- bbolt closed after fixed writes in 30.10 µs. KVLite used 2.15 ms. KVLite performs final checkpoint work during close.

## Limits

- This is one run on one container host. It is not a result from bare-metal Linux.
- Other containers were active during the run.
- The report contains medians, but it does not contain enough variation data for statistical significance tests.
- The 5% leader band does not prove that two results are equal.
- The run did not include the optional one-million-key cases.
- The current suite does not contain deletion or delete-churn cases.
- The read tests use a warm operating-system cache. The suite does not claim cold-cache performance.
- The suite does not rank combined CPU use or peak resident memory across the embedded and client-server designs.
- The suite does not rank persistent storage size. Redis reports its AOF file. bbolt reports its database file. KVLite reports its database file and WAL. These files have different maintenance and compaction rules.
- Redis has no result for ordered cursor or embedded life-cycle cases.

## Reproduce

Run the same suite from the repository root:

```sh
cd benchmarks/kvbench
KVBENCH_RESULTS_DIR=/tmp/kvlite-kvbench-large-20260915 \
./run-docker.sh large
```

The runner uses pinned Go and Redis container images. The Go module pins bbolt and the Redis client. Change the results directory for a new run.


## Full results

All 60 measured engine runs passed. Pure read results appear only once because the sync setting does not affect a read. Redis does not provide the ordered cursor API. Redis also does not have an embedded database life cycle. The current suite does not contain deletion benchmarks.

### Acknowledged operations

#### Durable

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| insert/random/value=128/clients=1 | 1,204 ack-keys/s | 656 ack-keys/s | 1,350 ack-keys/s | Redis |
| insert/random/value=128/clients=1/batch=10 | 9,519 ack-keys/s | 3,701 ack-keys/s | 8,898 ack-keys/s | KVLite |
| insert/random/value=128/clients=1/batch=100 | 53,562 ack-keys/s | 23,921 ack-keys/s | 60,166 ack-keys/s | Redis |
| insert/random/value=128/clients=1/batch=1000 | 251,310 ack-keys/s | 166,938 ack-keys/s | 303,309 ack-keys/s | Redis |
| insert/random/value=128/clients=32 | 18,574 ack-keys/s | 2,002 ack-keys/s | 24,058 ack-keys/s | Redis |
| insert/random/value=128/clients=32/batch=100 | 238,528 ack-keys/s | 21,190 ack-keys/s | 415,800 ack-keys/s | Redis |
| insert/random/value=128/clients=8 | 6,985 ack-keys/s | 522 ack-keys/s | 8,418 ack-keys/s | Redis |
| insert/random/value=128/clients=8/batch=100 | 147,780 ack-keys/s | 21,344 ack-keys/s | 192,513 ack-keys/s | Redis |
| insert/sequential/value=128/clients=1 | 1,242 ack-keys/s | 670 ack-keys/s | 1,336 ack-keys/s | Redis |
| insert/sequential/value=128/clients=1/batch=10 | 10,766 ack-keys/s | 6,386 ack-keys/s | 12,762 ack-keys/s | Redis |
| insert/sequential/value=128/clients=1/batch=100 | 98,614 ack-keys/s | 58,916 ack-keys/s | 80,344 ack-keys/s | KVLite |
| insert/sequential/value=128/clients=1/batch=1000 | 515,842 ack-keys/s | 247,831 ack-keys/s | 317,147 ack-keys/s | KVLite |
| mixed/read=50/value=128/clients=1 | 2,467 ack-keys/s | 1,298 ack-keys/s | 2,567 ack-keys/s | KVLite / Redis |
| mixed/read=50/value=128/clients=32 | 31,344 ack-keys/s | 3,800 ack-keys/s | 23,122 ack-keys/s | KVLite |
| mixed/read=50/value=128/clients=8 | 12,601 ack-keys/s | 1,014 ack-keys/s | 6,556 ack-keys/s | KVLite |
| mixed/read=95/value=128/clients=1 | 22,704 ack-keys/s | 12,164 ack-keys/s | 18,988 ack-keys/s | KVLite |
| mixed/read=95/value=128/clients=32 | 192,738 ack-keys/s | 35,789 ack-keys/s | 24,973 ack-keys/s | KVLite |
| mixed/read=95/value=128/clients=8 | 84,958 ack-keys/s | 10,424 ack-keys/s | 19,778 ack-keys/s | KVLite |
| read/random/hits=0/misses=above/value=128/clients=1 | 4,020,906 ack-keys/s | 1,454,938 ack-keys/s | 115,589 ack-keys/s | KVLite |
| read/random/hits=0/misses=below/value=128/clients=1 | 3,406,958 ack-keys/s | 1,657,772 ack-keys/s | 115,664 ack-keys/s | KVLite |
| read/random/hits=0/misses=between/value=128/clients=1 | 3,029,577 ack-keys/s | 1,473,410 ack-keys/s | 115,951 ack-keys/s | KVLite |
| read/random/hits=100/value=128/clients=32 | 3,122,208 ack-keys/s | 805,566 ack-keys/s | 196,172 ack-keys/s | KVLite |
| read/random/hits=100/value=128/clients=8 | 3,354,972 ack-keys/s | 857,806 ack-keys/s | 171,246 ack-keys/s | KVLite |
| read/random/hits=50/value=128/clients=1 | 2,310,701 ack-keys/s | 1,251,244 ack-keys/s | 122,792 ack-keys/s | KVLite |
| read/random/value=1024/clients=1 | 1,116,580 ack-keys/s | 1,036,032 ack-keys/s | 123,608 ack-keys/s | KVLite |
| read/random/value=128/clients=1 | 1,935,623 ack-keys/s | 1,083,717 ack-keys/s | 131,798 ack-keys/s | KVLite |
| read/random/value=3072/clients=1 | 947,850 ack-keys/s | 865,941 ack-keys/s | 99,137 ack-keys/s | KVLite |
| read/random/value=32/clients=1 | 2,221,372 ack-keys/s | 1,154,974 ack-keys/s | 134,743 ack-keys/s | KVLite |
| read/sequential/hits=100/value=128/clients=1 | 2,525,373 ack-keys/s | 1,170,269 ack-keys/s | 133,442 ack-keys/s | KVLite |
| update/random/grow=32-1024/clients=1 | 658 ack-keys/s | 624 ack-keys/s | 1,236 ack-keys/s | Redis |
| update/random/grow=32-1024/clients=1/batch=100 | 23,402 ack-keys/s | 12,834 ack-keys/s | 51,652 ack-keys/s | Redis |
| update/random/shrink=1024-32/clients=1 | 1,000 ack-keys/s | 655 ack-keys/s | 1,400 ack-keys/s | Redis |
| update/random/shrink=1024-32/clients=1/batch=100 | 45,245 ack-keys/s | 11,282 ack-keys/s | 56,572 ack-keys/s | Redis |
| update/random/value=1024/clients=1 | 554 ack-keys/s | 730 ack-keys/s | 941 ack-keys/s | Redis |
| update/random/value=128/clients=1 | 497 ack-keys/s | 510 ack-keys/s | 620 ack-keys/s | Redis |
| update/random/value=128/clients=1/batch=10 | 9,937 ack-keys/s | 3,379 ack-keys/s | 13,340 ack-keys/s | Redis |
| update/random/value=128/clients=1/batch=100 | 56,000 ack-keys/s | 19,734 ack-keys/s | 68,640 ack-keys/s | Redis |
| update/random/value=128/clients=1/batch=1000 | 187,919 ack-keys/s | 89,204 ack-keys/s | 302,654 ack-keys/s | Redis |
| update/random/value=128/clients=32 | 20,823 ack-keys/s | 1,898 ack-keys/s | 23,809 ack-keys/s | Redis |
| update/random/value=128/clients=32/batch=100 | 243,134 ack-keys/s | 18,349 ack-keys/s | 438,650 ack-keys/s | Redis |
| update/random/value=128/clients=8 | 7,338 ack-keys/s | 508 ack-keys/s | 7,548 ack-keys/s | KVLite / Redis |
| update/random/value=128/clients=8/batch=100 | 129,667 ack-keys/s | 17,754 ack-keys/s | 182,048 ack-keys/s | Redis |
| update/random/value=3072/clients=1 | 544 ack-keys/s | 725 ack-keys/s | 1,093 ack-keys/s | Redis |
| update/random/value=32/clients=1 | 517 ack-keys/s | 281 ack-keys/s | 562 ack-keys/s | Redis |
| update/sequential/value=128/clients=1/batch=10 | 11,984 ack-keys/s | 6,390 ack-keys/s | 11,670 ack-keys/s | KVLite / Redis |
| update/sequential/value=128/clients=1/batch=100 | 93,516 ack-keys/s | 53,856 ack-keys/s | 88,773 ack-keys/s | KVLite |
| update/sequential/value=128/clients=1/batch=1000 | 480,036 ack-keys/s | 282,468 ack-keys/s | 315,736 ack-keys/s | KVLite |

#### No commit sync

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| insert/random/value=128/clients=1 | 217,692 ack-keys/s | 58,910 ack-keys/s | 100,163 ack-keys/s | KVLite |
| insert/random/value=128/clients=1/batch=10 | 227,342 ack-keys/s | 141,886 ack-keys/s | 450,610 ack-keys/s | Redis |
| insert/random/value=128/clients=1/batch=100 | 276,904 ack-keys/s | 241,314 ack-keys/s | 925,144 ack-keys/s | Redis |
| insert/random/value=128/clients=1/batch=1000 | 691,361 ack-keys/s | 533,602 ack-keys/s | 1,016,544 ack-keys/s | Redis |
| insert/random/value=128/clients=32 | 125,516 ack-keys/s | 35,940 ack-keys/s | 182,810 ack-keys/s | Redis |
| insert/random/value=128/clients=32/batch=100 | 255,202 ack-keys/s | 242,663 ack-keys/s | 974,376 ack-keys/s | Redis |
| insert/random/value=128/clients=8 | 117,380 ack-keys/s | 39,254 ack-keys/s | 148,242 ack-keys/s | Redis |
| insert/random/value=128/clients=8/batch=100 | 270,800 ack-keys/s | 208,116 ack-keys/s | 923,555 ack-keys/s | Redis |
| insert/sequential/value=128/clients=1 | 215,332 ack-keys/s | 51,712 ack-keys/s | 97,528 ack-keys/s | KVLite |
| insert/sequential/value=128/clients=1/batch=10 | 607,869 ack-keys/s | 331,031 ack-keys/s | 439,804 ack-keys/s | KVLite |
| insert/sequential/value=128/clients=1/batch=100 | 1,546,452 ack-keys/s | 1,148,556 ack-keys/s | 974,213 ack-keys/s | KVLite |
| insert/sequential/value=128/clients=1/batch=1000 | 2,582,776 ack-keys/s | 1,969,572 ack-keys/s | 1,057,600 ack-keys/s | KVLite |
| mixed/read=50/value=128/clients=1 | 335,298 ack-keys/s | 117,196 ack-keys/s | 111,356 ack-keys/s | KVLite |
| mixed/read=50/value=128/clients=32 | 255,913 ack-keys/s | 68,260 ack-keys/s | 154,682 ack-keys/s | KVLite |
| mixed/read=50/value=128/clients=8 | 253,778 ack-keys/s | 68,759 ack-keys/s | 153,449 ack-keys/s | KVLite |
| mixed/read=95/value=128/clients=1 | 1,038,079 ack-keys/s | 451,065 ack-keys/s | 125,622 ack-keys/s | KVLite |
| mixed/read=95/value=128/clients=32 | 830,662 ack-keys/s | 289,230 ack-keys/s | 177,511 ack-keys/s | KVLite |
| mixed/read=95/value=128/clients=8 | 867,544 ack-keys/s | 320,781 ack-keys/s | 152,460 ack-keys/s | KVLite |
| update/random/grow=32-1024/clients=1 | 98,750 ack-keys/s | 59,384 ack-keys/s | 83,565 ack-keys/s | KVLite |
| update/random/grow=32-1024/clients=1/batch=100 | 119,838 ack-keys/s | 176,482 ack-keys/s | 485,660 ack-keys/s | Redis |
| update/random/shrink=1024-32/clients=1 | 141,778 ack-keys/s | 59,733 ack-keys/s | 91,522 ack-keys/s | KVLite |
| update/random/shrink=1024-32/clients=1/batch=100 | 265,456 ack-keys/s | 199,820 ack-keys/s | 1,062,719 ack-keys/s | Redis |
| update/random/value=1024/clients=1 | 112,517 ack-keys/s | 62,890 ack-keys/s | 81,256 ack-keys/s | KVLite |
| update/random/value=128/clients=1 | 182,670 ack-keys/s | 64,873 ack-keys/s | 90,652 ack-keys/s | KVLite |
| update/random/value=128/clients=1/batch=10 | 321,821 ack-keys/s | 146,933 ack-keys/s | 450,270 ack-keys/s | Redis |
| update/random/value=128/clients=1/batch=100 | 341,708 ack-keys/s | 264,664 ack-keys/s | 936,034 ack-keys/s | Redis |
| update/random/value=128/clients=1/batch=1000 | 465,123 ack-keys/s | 387,264 ack-keys/s | 1,062,394 ack-keys/s | Redis |
| update/random/value=128/clients=32 | 164,064 ack-keys/s | 44,031 ack-keys/s | 154,832 ack-keys/s | KVLite |
| update/random/value=128/clients=32/batch=100 | 301,273 ack-keys/s | 274,960 ack-keys/s | 925,870 ack-keys/s | Redis |
| update/random/value=128/clients=8 | 167,661 ack-keys/s | 45,644 ack-keys/s | 140,907 ack-keys/s | KVLite |
| update/random/value=128/clients=8/batch=100 | 345,186 ack-keys/s | 271,856 ack-keys/s | 805,652 ack-keys/s | Redis |
| update/random/value=3072/clients=1 | 110,814 ack-keys/s | 48,892 ack-keys/s | 65,961 ack-keys/s | KVLite |
| update/random/value=32/clients=1 | 215,975 ack-keys/s | 53,664 ack-keys/s | 96,121 ack-keys/s | KVLite |
| update/sequential/value=128/clients=1/batch=10 | 1,148,053 ack-keys/s | 527,172 ack-keys/s | 465,093 ack-keys/s | KVLite |
| update/sequential/value=128/clients=1/batch=100 | 1,856,194 ack-keys/s | 1,592,552 ack-keys/s | 999,480 ack-keys/s | KVLite |
| update/sequential/value=128/clients=1/batch=1000 | 1,969,330 ack-keys/s | 1,788,272 ack-keys/s | 1,052,138 ack-keys/s | KVLite |

### Read transactions

#### Durable

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| keys-per-transaction=1 | 1,325,632 keys/s | 917,388 keys/s | 126,446 keys/s | KVLite |
| keys-per-transaction=10 | 1,648,313 keys/s | 1,540,930 keys/s | 746,074 keys/s | KVLite |
| keys-per-transaction=100 | 1,701,468 keys/s | 1,526,144 keys/s | 1,657,340 keys/s | KVLite / Redis |
| keys-per-transaction=1000 | 1,735,452 keys/s | 1,599,836 keys/s | 1,701,144 keys/s | KVLite / Redis |

### Mixed transactions

#### Durable

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| read=50/operations-per-transaction=10 | 7,264 operations/s | 4,088 operations/s | 5,066 operations/s | KVLite |
| read=50/operations-per-transaction=100 | 52,689 operations/s | 23,725 operations/s | 70,318 operations/s | Redis |
| read=50/operations-per-transaction=1000 | 232,182 operations/s | 107,765 operations/s | 369,685 operations/s | Redis |
| read=95/operations-per-transaction=10 | 10,158 operations/s | 6,368 operations/s | 7,924 operations/s | KVLite |
| read=95/operations-per-transaction=100 | 81,746 operations/s | 50,229 operations/s | 99,902 operations/s | Redis |
| read=95/operations-per-transaction=1000 | 483,119 operations/s | 255,126 operations/s | 434,152 operations/s | KVLite |

#### No commit sync

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| read=50/operations-per-transaction=10 | 419,038 operations/s | 228,116 operations/s | 486,544 operations/s | Redis |
| read=50/operations-per-transaction=100 | 541,881 operations/s | 399,340 operations/s | 809,853 operations/s | Redis |
| read=50/operations-per-transaction=1000 | 669,784 operations/s | 568,812 operations/s | 1,018,158 operations/s | Redis |
| read=95/operations-per-transaction=10 | 689,654 operations/s | 452,491 operations/s | 467,071 operations/s | KVLite |
| read=95/operations-per-transaction=100 | 1,036,872 operations/s | 878,148 operations/s | 1,063,382 operations/s | KVLite / Redis |
| read=95/operations-per-transaction=1000 | 1,157,333 operations/s | 1,202,607 operations/s | 1,083,456 operations/s | KVLite / bbolt |

### Enumeration

#### Durable

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| selectivity=0 | 272.61 µs | 496.88 µs | 523.32 ms | KVLite |
| selectivity=1 | 8,059,001 entries/s | 7,695,012 entries/s | 111,384 entries/s | KVLite / bbolt |
| selectivity=10 | 6,850,240 entries/s | 7,923,575 entries/s | 636,942 entries/s | bbolt |
| selectivity=100 | 2,654,568 entries/s | 6,651,070 entries/s | 970,483 entries/s | bbolt |

### Ordered operations

#### Durable

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| full-forward | 7,794,076 entries/s | 7,192,822 entries/s | — | KVLite |
| full-reverse | 6,776,686 entries/s | 7,542,828 entries/s | — | bbolt |
| range/selectivity=0 | 290.60 µs | 501.80 µs | — | KVLite |
| range/selectivity=1 | 7,500,789 entries/s | 7,114,186 entries/s | — | KVLite |
| range/selectivity=10 | 7,057,447 entries/s | 7,222,014 entries/s | — | KVLite / bbolt |
| range/selectivity=100 | 7,589,647 entries/s | 7,189,928 entries/s | — | KVLite |
| seek-and-read=1 | 905,770 entries/s | 742,738 entries/s | — | KVLite |
| seek-and-read=10 | 4,806,486 entries/s | 5,263,008 entries/s | — | bbolt |
| seek-and-read=100 | 7,122,580 entries/s | 6,692,487 entries/s | — | KVLite |
| seek-and-read=1000 | 7,171,811 entries/s | 7,225,606 entries/s | — | KVLite / bbolt |

### Scale and access distribution

#### Durable

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| records=10000/hot-80-20 | 2,034,213 reads/s | 885,202 reads/s | 132,496 reads/s | KVLite |
| records=10000/uniform | 1,947,610 reads/s | 840,108 reads/s | 129,807 reads/s | KVLite |
| records=100000/hot-80-20 | 1,290,362 reads/s | 976,018 reads/s | 125,556 reads/s | KVLite |
| records=100000/uniform | 1,030,736 reads/s | 887,542 reads/s | 126,470 reads/s | KVLite |

### Latency

#### Durable

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| mixed/clients=1 | 1.27 ms | 1.43 ms | 1.03 ms | Redis |
| mixed/clients=32 | 2.21 ms | 16.35 ms | 3.01 ms | KVLite |
| mixed/clients=8 | 1.87 ms | 15.94 ms | 1.87 ms | KVLite / Redis |
| read/clients=1 | 1.33 µs | 2.79 µs | 20.56 µs | KVLite |
| read/clients=32 | 1.85 µs | 47.65 µs | 626.21 µs | KVLite |
| read/clients=8 | 1.60 µs | 4.15 µs | 185.36 µs | KVLite |
| update/clients=1 | 4.76 ms | 8.62 ms | 4.06 ms | Redis |
| update/clients=32 | 6.10 ms | 21.34 ms | 3.86 ms | Redis |
| update/clients=8 | 4.40 ms | 21.29 ms | 2.77 ms | Redis |

#### No commit sync

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| mixed/clients=1 | 6.25 µs | 17.81 µs | 17.92 µs | KVLite |
| mixed/clients=32 | 174.17 µs | 197.94 µs | 634.83 µs | KVLite |
| mixed/clients=8 | 12.54 µs | 31.10 µs | 236.31 µs | KVLite |
| update/clients=1 | 11.35 µs | 29.81 µs | 20.58 µs | KVLite |
| update/clients=32 | 4.37 ms | 6.79 ms | 537.57 µs | Redis |
| update/clients=8 | 43.27 µs | 3.72 ms | 153.50 µs | KVLite |

### Collections

#### Durable

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| collections=1/depth=1 | 62,754 keys/s | 47,995 keys/s | 48,408 keys/s | KVLite |
| collections=1/depth=3 | 69,208 keys/s | 45,230 keys/s | 60,898 keys/s | KVLite |
| collections=100/depth=1 | 59,962 keys/s | 46,569 keys/s | 64,398 keys/s | Redis |
| collections=100/depth=3 | 62,174 keys/s | 46,322 keys/s | 55,852 keys/s | KVLite |
| read/collections=1/depth=1 | 1,160,068 reads/s | 716,320 reads/s | 129,204 reads/s | KVLite |
| read/collections=1/depth=3 | 701,602 reads/s | 510,266 reads/s | 126,912 reads/s | KVLite |
| read/collections=100/depth=1 | 1,027,773 reads/s | 702,146 reads/s | 131,850 reads/s | KVLite |
| read/collections=100/depth=3 | 647,768 reads/s | 556,844 reads/s | 130,972 reads/s | KVLite |

#### No commit sync

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| collections=1/depth=1 | 1,216,144 keys/s | 963,016 keys/s | 647,928 keys/s | KVLite |
| collections=1/depth=3 | 1,176,529 keys/s | 856,259 keys/s | 569,514 keys/s | KVLite |
| collections=100/depth=1 | 1,294,895 keys/s | 1,577,984 keys/s | 791,558 keys/s | bbolt |
| collections=100/depth=3 | 1,652,914 keys/s | 1,613,906 keys/s | 604,151 keys/s | KVLite / bbolt |

### Life cycle

#### Durable

| Case | KVLite | bbolt | Redis | Leader |
| --- | ---: | ---: | ---: | --- |
| close-after-writes | 2.15 ms | 30.10 µs | — | bbolt |
| create-load-close | 38.22 ms | 43.31 ms | — | KVLite |
| open-clean | 105.11 µs | 2.23 ms | — | KVLite |
| recover-after-process-kill | 54.73 ms | 55.64 ms | — | KVLite / bbolt |

### Latency detail

#### Durable

##### Mixed, one client

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 1.00 µs | 27.73 µs | 1.27 ms | 133.05 ms | 10 |
| bbolt | 958.00 ns | 62.27 µs | 1.43 ms | 6.86 ms | 10 |
| Redis | 8.77 µs | 482.90 µs | 1.03 ms | 4.73 ms | 10 |

##### Mixed, 32 clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 708.00 ns | 1.18 ms | 2.21 ms | 10.17 ms | 10 |
| bbolt | 1.29 µs | 3.12 ms | 16.35 ms | 18.80 ms | 10 |
| Redis | 1.23 ms | 2.51 ms | 3.01 ms | 5.88 ms | 10 |

##### Mixed, eight clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 854.50 ns | 994.97 µs | 1.87 ms | 6.15 ms | 10 |
| bbolt | 1.38 µs | 806.75 µs | 15.94 ms | 19.93 ms | 10 |
| Redis | 81.80 µs | 1.21 ms | 1.87 ms | 5.62 ms | 10 |

##### Read, one client

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 500.00 ns | 791.50 ns | 1.33 µs | 26.92 µs | 10 |
| bbolt | 562.50 ns | 1.73 µs | 2.79 µs | 318.83 µs | 10 |
| Redis | 7.02 µs | 10.44 µs | 20.56 µs | 366.94 µs | 10 |

##### Read, 32 clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 541.50 ns | 792.00 ns | 1.85 µs | 2.39 ms | 10 |
| bbolt | 625.00 ns | 2.10 µs | 47.65 µs | 7.19 ms | 10 |
| Redis | 132.96 µs | 354.34 µs | 626.21 µs | 3.44 ms | 10 |

##### Read, eight clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 521.00 ns | 853.50 ns | 1.60 µs | 2.28 ms | 10 |
| bbolt | 625.00 ns | 1.90 µs | 4.15 µs | 6.77 ms | 10 |
| Redis | 33.60 µs | 90.31 µs | 185.36 µs | 3.69 ms | 10 |

##### Update, one client

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 1.16 ms | 3.32 ms | 4.76 ms | 293.24 ms | 10 |
| bbolt | 1.53 ms | 6.31 ms | 8.62 ms | 340.62 ms | 10 |
| Redis | 768.18 µs | 2.75 ms | 4.06 ms | 294.01 ms | 10 |

##### Update, 32 clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 1.31 ms | 3.20 ms | 6.10 ms | 11.63 ms | 10 |
| bbolt | 16.79 ms | 19.60 ms | 21.34 ms | 24.73 ms | 10 |
| Redis | 1.33 ms | 2.43 ms | 3.86 ms | 7.46 ms | 10 |

##### Update, eight clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 1.21 ms | 2.68 ms | 4.40 ms | 143.12 ms | 10 |
| bbolt | 15.25 ms | 18.32 ms | 21.29 ms | 35.71 ms | 10 |
| Redis | 951.48 µs | 1.91 ms | 2.77 ms | 18.23 ms | 10 |

#### No commit sync

##### Mixed, one client

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 583.00 ns | 3.04 µs | 6.25 µs | 42.19 µs | 10 |
| bbolt | 729.50 ns | 10.60 µs | 17.81 µs | 618.08 µs | 10 |
| Redis | 6.98 µs | 10.35 µs | 17.92 µs | 276.30 µs | 10 |

##### Mixed, 32 clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 584.00 ns | 5.04 µs | 174.17 µs | 4.06 ms | 10 |
| bbolt | 812.50 ns | 12.81 µs | 197.94 µs | 12.69 ms | 10 |
| Redis | 132.08 µs | 329.67 µs | 634.83 µs | 4.43 ms | 10 |

##### Mixed, eight clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 583.00 ns | 4.00 µs | 12.54 µs | 4.08 ms | 10 |
| bbolt | 750.00 ns | 11.71 µs | 31.10 µs | 9.13 ms | 10 |
| Redis | 35.46 µs | 98.77 µs | 236.31 µs | 3.91 ms | 10 |

##### Update, one client

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 3.35 µs | 6.08 µs | 11.35 µs | 2.28 ms | 10 |
| bbolt | 11.19 µs | 19.04 µs | 29.81 µs | 2.72 ms | 10 |
| Redis | 9.52 µs | 13.58 µs | 20.58 µs | 550.47 µs | 10 |

##### Update, 32 clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 3.48 µs | 8.42 µs | 4.37 ms | 10.65 ms | 10 |
| bbolt | 11.90 µs | 3.76 ms | 6.79 ms | 11.52 ms | 10 |
| Redis | 113.17 µs | 313.51 µs | 537.57 µs | 6.84 ms | 10 |

##### Update, eight clients

| Engine | p50 | p95 | p99 | Maximum | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| KVLite | 3.46 µs | 6.90 µs | 43.27 µs | 9.17 ms | 10 |
| bbolt | 11.81 µs | 26.06 µs | 3.72 ms | 10.43 ms | 10 |
| Redis | 32.94 µs | 85.52 µs | 153.50 µs | 3.64 ms | 10 |
