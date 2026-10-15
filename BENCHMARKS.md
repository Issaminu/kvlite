# KVLite benchmarks

This report compares KVLite with bbolt and Redis at the public call boundary. It contains the median of 15 measured runs at the complete `large --workloads=all --storage=volume` scale. The same run then profiled every KVLite case.

| Item | Value |
| --- | --- |
| Run time | `2026-10-15T00:35:46Z` |
| Timing duration | 2 hours 25 minutes 29 seconds |
| Profile capture | 34 minutes 49 seconds, KVLite cases only |
| KVLite | `ca4fdae52912a21177ba3a7c3f4010d8b896869e` from a clean working tree |
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
| Point read | Read-only, random, 128-byte value, eight clients | 4,277,018 keys/s | 973,946 keys/s | 187,709 keys/s | **KVLite: 4.39x bbolt; 22.79x Redis** |
| Point-read p99 | Read-only, eight clients | 958 ns | 6.21 µs | 161.96 µs | **KVLite: 6.48x advantage over bbolt; 169.06x over Redis** |
| Point update | Durable, random, 128-byte value, one client, batch of 100 | 137,750 keys/s | 21,954 keys/s | 48,732 keys/s | **KVLite: 6.27x bbolt; 2.83x Redis** |
| Mixed work | Durable, 95% reads, eight clients | 150,664 operations/s | 8,669 operations/s | 11,409 operations/s | **KVLite: 17.38x bbolt; 13.21x Redis** |
| Mixed work | No commit sync, 95% reads, eight clients | 1,464,241 operations/s | 317,602 operations/s | 171,890 operations/s | **KVLite: 4.61x bbolt; 8.52x Redis** |
| Key delete | No commit sync, random, one client | 286,336 keys/s | 69,125 keys/s | 105,981 keys/s | **KVLite: 4.14x bbolt; 2.70x Redis** |
| Large-key read | Read-only, random, 32 KiB key, one client | 80,187 keys/s | 67,238 keys/s | 29,334 keys/s | **KVLite: 1.19x bbolt; 2.73x Redis** |
| Clean open | Life cycle | 68.67 µs | 2.67 ms | Not comparable | **KVLite: 38.88x advantage over bbolt** |

These ratios apply only to the named workloads and public call boundaries. KVLite did not lead every case. bbolt read 64 KiB values 1.33x faster and 1 MiB values 1.54x faster. Redis led most batch inserts and half of the deletion, reuse, and reopen cases. The full tables show every case.

## Scope

This run measured every workload group and every case variant at the large scale. It covers point operations, deletion, transactions, enumeration, ordered operations, access distributions, latency, collections, storage size, and embedded life-cycle operations.

The run includes large-entry cases. These cases use 64 KiB values, 1 MiB values, or 32 KiB keys. A large entry needs continuation pages in KVLite. The previous report did not include these cases.

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

The harness opens KVLite with the default `DataPageChecksums=false`. KVLite therefore does not calculate or check checksums for leaf, branch, and continuation pages in this run. Metadata pages, allocation pages, and WAL records keep their checksums. The previous report used commit `c0fff00`. That version always wrote data-page checksums at checkpoint and checked them when it decoded a page. Its mapped point lookups did not check them. Some KVLite write, scan, and large-entry gains since that report come from this default and not from faster code. With data-page checksums on, KVLite point reads and scans are 2.5 to 4.9 times slower than with the default.

## Test environment

- CPU set: `0-3`
- Go: `go1.26.7 linux/arm64`
- KVLite: `ca4fdae52912a21177ba3a7c3f4010d8b896869e` from a clean working tree
- bbolt: `v1.4.3`
- Redis client: `go-redis v9.22.0`
- Redis server: `8.8.0`, `jemalloc-5.3.0`
- Container system: OrbStack with Linux kernel `7.0.14-orbstack-00380-ga7e0a2dc9535`
- Storage driver: `overlay2`
- Host: Apple M4 Max; 14 CPUs visible to the container system
- CPUs assigned to each measured process and its Redis server: four
- Go container memory limit: 12,614,234,112 bytes
- Benchmark data storage: temporary Docker volume

The Docker volume path includes the OrbStack virtual machine and the host storage path. It does not prove bare-metal device performance. Other containers were active on the host. CPU assignment reduced direct CPU competition. Other host work could still affect the results.

## Method

The runner first completed its correctness tests. It then completed one unrecorded warm-up round. It created a new database fixture for each benchmark process. It did not reuse a measured database from the warm-up round.

The runner recorded 15 fixed-work rounds. It changed the engine order and durability-mode order between rounds. Pure reads ran once per round because commit-sync settings do not affect them.

The large scale uses 10,000 records for its main cases. It uses 100,000 point reads, 1,000 single-client point writes, 3,200 concurrent point writes, and 10,000 mixed operations. Large-entry cases use 512 records, 5,000 reads, and 1,000 writes. The 1 MiB value case uses 64 records and 1,000 reads. Read-latency cases use 1,000,000 recorded operations and 10,000 unrecorded fixture warm-up reads. Update-latency cases use 10,000 operations. Mixed-latency cases use 100,000 operations.

Setup, fixture loading, final stored-data checks, and close operations were outside the core timers. The named life-cycle cases measure open, close, and recovery work separately. The runner ran each life-cycle case in its own process. The previous report measured these cases in a process that had already run other cases.

After the 15 timed rounds, the runner profiled every KVLite case again with 15 repeats. It recorded CPU profiles, allocation profiles, and execution traces. The profile pass does not change the timing values in this report. KVLite rows also report WAL commits, WAL bytes, write calls, and storage bytes written per operation.

## Changes since the previous report

The previous report measured `c0fff00` on 2026-10-03. This section compares the medians of the two runs. bbolt and Redis code did not change between the runs, so their changes show host drift. As a geometric mean over 227 metrics, KVLite times went down by 17.5% and bbolt times went down by 8.7%.

| Workload group | KVLite time ratio | bbolt time ratio |
| --- | ---: | ---: |
| Small point reads | 0.85 | 0.98 |
| Read transactions | 0.86 | 1.02 |
| Access-distribution reads | 0.88 | 0.96 |
| Ordered operations | 0.91 | 1.02 |
| Durable key deletes | 0.63 | 0.92 |
| No-commit-sync key deletes | 0.71 | 0.95 |
| Durable bucket deletion | 0.49 | 0.93 |
| No-commit-sync bucket deletion | 0.47 | 1.43 |
| Durable latency p99 | 0.44 | 0.89 |
| No-commit-sync latency p99 | 0.73 | 1.09 |
| No-commit-sync point updates | 0.81 | 0.97 |
| No-commit-sync point inserts | 0.87 | 0.97 |
| No-commit-sync mixed work | 0.80 | 1.00 |
| Durable point updates | 0.82 | 0.77 |
| Durable point inserts | 0.71 | 0.80 |

A ratio below 1 means a lower time. Each ratio is the geometric mean of the main metric of each case in the group. Durable write ratios follow the bbolt ratios, so storage sync latency explains most of their change.

The 12 commits after `973586d` changed the read path. A full run of `973586d` on 2026-10-14 gave a baseline, with bbolt as the drift control. Paired runs in one container then checked each case that looked slower. They showed these KVLite changes:

- Point reads of absent keys are 18% to 19% faster.
- Point reads of present keys are 10% to 13% faster.
- Read transactions are 9% to 10% faster.
- Full scans have the same machine code in their entry loops. A 3% difference in some runs came from code placement.
- Delete, collection, page-reuse, reopen, and life-cycle cases did not change outside the noise of this host.

The clean-open time increased from 48.04 µs to 68.67 µs because each life-cycle case now runs in a new process. Paired runs of the old and new code in one process gave the same open time.

## Interpretation

These statements apply only to this run and its tested workloads.

- KVLite led 82 of 118 durable cases. bbolt led nine. Redis led 27.
- KVLite led 46 of 71 no-commit-sync cases. bbolt led four. Redis led 21.
- Each count uses one main metric for each case: the median time per operation, or p99 for latency cases. It excludes storage-size metrics.
- KVLite led 40 of 43 read cases, including point reads, read transactions, scans, ordered reads, access distributions, collection reads, and read latency. bbolt led 64 KiB value reads, 1 MiB value reads, and `seek-and-read=1000`.
- KVLite led 13 of 21 durable point-update cases and five of 13 durable point-insert cases. Redis led the other cases.
- KVLite led 15 of 21 no-commit-sync point-update cases and five of 13 no-commit-sync point-insert cases. Redis led the other cases.
- KVLite led durable acknowledged mixed work in six of seven cases. bbolt led the case with 64 KiB values.
- bbolt led three of four durable collection-write cases. The KVLite times of these cases did not change in paired runs.
- KVLite led 13 of 28 deletion, reuse, and reopen cases across both modes. Redis led 14. bbolt led one.
- KVLite opened a clean database 38.88x faster than bbolt. bbolt closed after writes 165x faster, because KVLite checkpoints its WAL during close.

## Limits

- This is one run on one container host. It is not a bare-metal Linux result.
- Docker volume storage includes the OrbStack virtual machine and host storage path. It does not isolate physical-device sync latency.
- Durable write times depend on storage sync latency. That latency changed by up to four times between rounds of the same binary.
- The `recover-after-process-kill` samples have two groups for both engines: 2 to 5 ms and 55 to 65 ms. Linux can release the file lock of the killed helper process some milliseconds after the harness waits for that process. The open then finds the file locked, and both engines try again after 50 ms. The median of this case depends on how many samples are in the fast group.
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
| read/random/hits=0/misses=above/value=128/clients=1 | 5,050,299 keys/s | 1,775,639 keys/s | 123,621 keys/s |
| read/random/hits=0/misses=below/value=128/clients=1 | 4,853,624 keys/s | 1,823,011 keys/s | 122,653 keys/s |
| read/random/hits=0/misses=between/value=128/clients=1 | 4,407,697 keys/s | 1,712,827 keys/s | 122,473 keys/s |
| read/random/hits=50/value=128/clients=1 | 3,429,747 keys/s | 1,550,836 keys/s | 131,216 keys/s |
| read/random/hits=100/value=128/clients=8 | 4,277,018 keys/s | 973,946 keys/s | 187,709 keys/s |
| read/random/hits=100/value=128/clients=32 | 3,514,990 keys/s | 912,905 keys/s | 256,263 keys/s |
| read/random/key=32768/value=128/clients=1 | 80,187 keys/s | 67,238 keys/s | 29,334 keys/s |
| read/random/value=32/clients=1 | 3,086,039 keys/s | 1,307,788 keys/s | 143,213 keys/s |
| read/random/value=128/clients=1 | 2,825,242 keys/s | 1,406,338 keys/s | 140,495 keys/s |
| read/random/value=1024/clients=1 | 1,716,480 keys/s | 1,335,999 keys/s | 127,859 keys/s |
| read/random/value=3072/clients=1 | 1,344,284 keys/s | 1,158,054 keys/s | 109,235 keys/s |
| read/random/value=65536/clients=1 | 155,523 keys/s | 207,504 keys/s | 35,723 keys/s |
| read/random/value=1048576/clients=1 | 15,724 keys/s | 24,284 keys/s | 5,715 keys/s |
| read/sequential/hits=100/value=128/clients=1 | 3,679,180 keys/s | 1,427,289 keys/s | 141,364 keys/s |
| update/random/grow=32-1024/clients=1 | 1,445 keys/s | 791 keys/s | 1,552 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 25,892 keys/s | 13,472 keys/s | 40,464 keys/s |
| update/random/grow=128-65536/clients=1 | 1,141 keys/s | 769 keys/s | 1,161 keys/s |
| update/random/shrink=1024-32/clients=1 | 1,770 keys/s | 879 keys/s | 1,952 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 47,185 keys/s | 12,891 keys/s | 50,816 keys/s |
| update/random/shrink=65536-128/clients=1 | 1,389 keys/s | 892 keys/s | 1,689 keys/s |
| update/random/value=32/clients=1 | 1,677 keys/s | 852 keys/s | 1,688 keys/s |
| update/random/value=128/clients=1 | 1,875 keys/s | 926 keys/s | 1,643 keys/s |
| update/random/value=128/clients=1/batch=10 | 17,635 keys/s | 4,964 keys/s | 15,199 keys/s |
| update/random/value=128/clients=1/batch=100 | 137,750 keys/s | 21,954 keys/s | 48,732 keys/s |
| update/random/value=128/clients=1/batch=1000 | 470,267 keys/s | 104,640 keys/s | 278,921 keys/s |
| update/random/value=128/clients=8 | 13,124 keys/s | 404 keys/s | 11,372 keys/s |
| update/random/value=128/clients=8/batch=100 | 256,697 keys/s | 21,047 keys/s | 209,892 keys/s |
| update/random/value=128/clients=32 | 41,477 keys/s | 1,543 keys/s | 31,778 keys/s |
| update/random/value=128/clients=32/batch=100 | 422,663 keys/s | 21,162 keys/s | 381,284 keys/s |
| update/random/value=1024/clients=1 | 1,671 keys/s | 922 keys/s | 1,585 keys/s |
| update/random/value=3072/clients=1 | 1,729 keys/s | 796 keys/s | 1,387 keys/s |
| update/random/value=65536/clients=1 | 1,037 keys/s | 810 keys/s | 1,135 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 18,732 keys/s | 8,331 keys/s | 14,805 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 163,636 keys/s | 59,575 keys/s | 46,665 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 860,729 keys/s | 269,083 keys/s | 321,535 keys/s |
| insert/random/value=128/clients=1 | 1,644 keys/s | 931 keys/s | 1,890 keys/s |
| insert/random/value=128/clients=1/batch=10 | 13,614 keys/s | 4,849 keys/s | 17,173 keys/s |
| insert/random/value=128/clients=1/batch=100 | 82,044 keys/s | 25,493 keys/s | 42,805 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 278,174 keys/s | 179,775 keys/s | 303,573 keys/s |
| insert/random/value=128/clients=8 | 10,191 keys/s | 428 keys/s | 5,838 keys/s |
| insert/random/value=128/clients=8/batch=100 | 173,388 keys/s | 26,242 keys/s | 214,381 keys/s |
| insert/random/value=128/clients=32 | 31,310 keys/s | 1,579 keys/s | 14,925 keys/s |
| insert/random/value=128/clients=32/batch=100 | 366,066 keys/s | 26,624 keys/s | 398,217 keys/s |
| insert/random/value=65536/clients=1 | 601 keys/s | 802 keys/s | 1,178 keys/s |
| insert/sequential/value=128/clients=1 | 1,679 keys/s | 924 keys/s | 1,795 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 14,410 keys/s | 9,259 keys/s | 16,136 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 136,838 keys/s | 78,286 keys/s | 42,373 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 760,317 keys/s | 247,046 keys/s | 319,448 keys/s |
| mixed/read=50/value=128/clients=1 | 3,756 operations/s | 1,744 operations/s | 3,546 operations/s |
| mixed/read=50/value=128/clients=8 | 24,434 operations/s | 798 operations/s | 11,848 operations/s |
| mixed/read=50/value=128/clients=32 | 65,530 operations/s | 3,091 operations/s | 31,326 operations/s |
| mixed/read=95/value=128/clients=1 | 35,359 operations/s | 13,676 operations/s | 10,519 operations/s |
| mixed/read=95/value=128/clients=8 | 150,664 operations/s | 8,669 operations/s | 11,409 operations/s |
| mixed/read=95/value=128/clients=32 | 374,761 operations/s | 29,898 operations/s | 18,063 operations/s |
| mixed/read=95/value=65536/clients=1 | 8,580 operations/s | 9,727 operations/s | 6,485 operations/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 116,615 keys/s | 69,869 keys/s | 71,270 keys/s |
| update/random/grow=32-1024/clients=1/batch=100 | 176,844 keys/s | 220,397 keys/s | 418,882 keys/s |
| update/random/grow=128-65536/clients=1 | 16,912 keys/s | 16,478 keys/s | 21,503 keys/s |
| update/random/shrink=1024-32/clients=1 | 236,316 keys/s | 66,639 keys/s | 92,084 keys/s |
| update/random/shrink=1024-32/clients=1/batch=100 | 489,025 keys/s | 255,432 keys/s | 1,123,510 keys/s |
| update/random/shrink=65536-128/clients=1 | 98,821 keys/s | 29,695 keys/s | 98,784 keys/s |
| update/random/value=32/clients=1 | 358,838 keys/s | 52,236 keys/s | 91,962 keys/s |
| update/random/value=128/clients=1 | 307,274 keys/s | 72,658 keys/s | 89,211 keys/s |
| update/random/value=128/clients=1/batch=10 | 706,226 keys/s | 170,335 keys/s | 395,716 keys/s |
| update/random/value=128/clients=1/batch=100 | 958,306 keys/s | 358,246 keys/s | 842,400 keys/s |
| update/random/value=128/clients=1/batch=1000 | 1,274,410 keys/s | 539,033 keys/s | 894,574 keys/s |
| update/random/value=128/clients=8 | 215,394 keys/s | 46,374 keys/s | 157,623 keys/s |
| update/random/value=128/clients=8/batch=100 | 588,461 keys/s | 311,245 keys/s | 1,010,119 keys/s |
| update/random/value=128/clients=32 | 242,623 keys/s | 47,438 keys/s | 176,296 keys/s |
| update/random/value=128/clients=32/batch=100 | 659,748 keys/s | 322,357 keys/s | 816,633 keys/s |
| update/random/value=1024/clients=1 | 209,709 keys/s | 72,505 keys/s | 85,419 keys/s |
| update/random/value=3072/clients=1 | 224,308 keys/s | 65,066 keys/s | 75,115 keys/s |
| update/random/value=65536/clients=1 | 15,236 keys/s | 21,287 keys/s | 21,960 keys/s |
| update/sequential/value=128/clients=1/batch=10 | 1,807,594 keys/s | 517,147 keys/s | 490,547 keys/s |
| update/sequential/value=128/clients=1/batch=100 | 2,861,159 keys/s | 1,763,120 keys/s | 885,169 keys/s |
| update/sequential/value=128/clients=1/batch=1000 | 3,309,986 keys/s | 2,406,405 keys/s | 909,568 keys/s |
| insert/random/value=128/clients=1 | 228,548 keys/s | 60,581 keys/s | 91,566 keys/s |
| insert/random/value=128/clients=1/batch=10 | 279,135 keys/s | 144,846 keys/s | 426,134 keys/s |
| insert/random/value=128/clients=1/batch=100 | 389,093 keys/s | 297,227 keys/s | 922,806 keys/s |
| insert/random/value=128/clients=1/batch=1000 | 889,336 keys/s | 761,635 keys/s | 988,447 keys/s |
| insert/random/value=128/clients=8 | 118,173 keys/s | 36,026 keys/s | 147,877 keys/s |
| insert/random/value=128/clients=8/batch=100 | 280,231 keys/s | 243,800 keys/s | 909,282 keys/s |
| insert/random/value=128/clients=32 | 130,488 keys/s | 34,477 keys/s | 175,896 keys/s |
| insert/random/value=128/clients=32/batch=100 | 347,783 keys/s | 258,647 keys/s | 918,231 keys/s |
| insert/random/value=65536/clients=1 | 11,650 keys/s | 12,613 keys/s | 21,802 keys/s |
| insert/sequential/value=128/clients=1 | 219,838 keys/s | 57,604 keys/s | 96,130 keys/s |
| insert/sequential/value=128/clients=1/batch=10 | 652,605 keys/s | 409,062 keys/s | 413,183 keys/s |
| insert/sequential/value=128/clients=1/batch=100 | 2,058,966 keys/s | 1,479,618 keys/s | 907,143 keys/s |
| insert/sequential/value=128/clients=1/batch=1000 | 3,822,183 keys/s | 2,157,126 keys/s | 973,519 keys/s |
| mixed/read=50/value=128/clients=1 | 613,201 operations/s | 133,355 operations/s | 119,204 operations/s |
| mixed/read=50/value=128/clients=8 | 438,785 operations/s | 78,013 operations/s | 144,698 operations/s |
| mixed/read=50/value=128/clients=32 | 410,035 operations/s | 70,610 operations/s | 176,850 operations/s |
| mixed/read=95/value=128/clients=1 | 1,550,096 operations/s | 590,898 operations/s | 135,672 operations/s |
| mixed/read=95/value=128/clients=8 | 1,464,241 operations/s | 317,602 operations/s | 171,890 operations/s |
| mixed/read=95/value=128/clients=32 | 1,481,660 operations/s | 308,622 operations/s | 211,939 operations/s |
| mixed/read=95/value=65536/clients=1 | 118,374 operations/s | 145,702 operations/s | 36,361 operations/s |

### Key deletion: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 1,065 ack-keys/s | 745 ack-keys/s | 1,773 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 7,282,407 B | 4,194,304 B | 2,080,401 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 112,359 ack-keys/s | 37,506 ack-keys/s | 146,561 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 7,031,335 B | 4,194,304 B | 1,951,901 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 521,319 ack-keys/s | 204,193 ack-keys/s | 836,918 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,373,884 B | 8,388,608 B | 1,950,561 B |
| `existing/random/clients=8` | `ack-keys/s` | 11,340 ack-keys/s | 403 ack-keys/s | 11,037 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 5,958,145 B | 4,194,304 B | 2,080,401 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 1,009 ack-keys/s | 488 ack-keys/s | 1,429 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 37,794,883 B | 51,425,280 B | 33,587,340 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 1,501 ack-keys/s | 810 ack-keys/s | 1,750 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 3,532,416 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 146,161 ack-keys/s | 84,588 ack-keys/s | 76,714 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 4,297,532 B | 4,194,304 B | 1,951,901 B |
| `missing/random/clients=1` | `ack-keys/s` | 345,915 ack-keys/s | 842 ack-keys/s | 135,142 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,485,696 B | 4,194,304 B | 1,720,401 B |

### Key deletion: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `existing/random/clients=1` | `ack-keys/s` | 286,336 ack-keys/s | 69,125 ack-keys/s | 105,981 ack-keys/s |
| `existing/random/clients=1` | `persistent-B` | 7,282,407 B | 4,194,304 B | 2,080,401 B |
| `existing/random/clients=1/batch=100` | `ack-keys/s` | 773,457 ack-keys/s | 427,602 ack-keys/s | 2,619,018 ack-keys/s |
| `existing/random/clients=1/batch=100` | `persistent-B` | 7,031,335 B | 4,194,304 B | 1,951,901 B |
| `existing/random/clients=1/batch=1000` | `ack-keys/s` | 1,490,163 ack-keys/s | 769,120 ack-keys/s | 3,580,595 ack-keys/s |
| `existing/random/clients=1/batch=1000` | `persistent-B` | 5,373,884 B | 6,045,696 B | 1,950,561 B |
| `existing/random/clients=8` | `ack-keys/s` | 208,658 ack-keys/s | 48,740 ack-keys/s | 174,565 ack-keys/s |
| `existing/random/clients=8` | `persistent-B` | 7,260,168 B | 4,194,304 B | 2,080,401 B |
| `existing/random/value=65536/clients=1` | `ack-keys/s` | 13,766 ack-keys/s | 14,256 ack-keys/s | 95,639 ack-keys/s |
| `existing/random/value=65536/clients=1` | `persistent-B` | 37,729,493 B | 51,425,280 B | 33,587,340 B |
| `existing/sequential/clients=1` | `ack-keys/s` | 339,593 ack-keys/s | 67,289 ack-keys/s | 106,396 ack-keys/s |
| `existing/sequential/clients=1` | `persistent-B` | 3,532,416 B | 4,194,304 B | 2,080,401 B |
| `existing/sequential/clients=1/batch=100` | `ack-keys/s` | 3,176,902 ack-keys/s | 2,074,621 ack-keys/s | 2,535,066 ack-keys/s |
| `existing/sequential/clients=1/batch=100` | `persistent-B` | 4,297,532 B | 4,194,304 B | 1,951,901 B |
| `missing/random/clients=1` | `ack-keys/s` | 3,018,869 ack-keys/s | 213,815 ack-keys/s | 135,822 ack-keys/s |
| `missing/random/clients=1` | `persistent-B` | 3,485,696 B | 4,194,304 B | 1,720,401 B |

### Bucket deletion: durable

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 1,311 buckets/s | 877 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 4,138,291 B | 4,194,304 B |

### Bucket deletion: no-commit-sync

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `top-level/buckets=100/keys-per-bucket=100` | `buckets/s` | 68,071 buckets/s | 45,275 buckets/s |
| `top-level/buckets=100/keys-per-bucket=100` | `persistent-B` | 4,138,291 B | 4,194,304 B |

### Read after delete: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 595.94 µs | 1.26 ms | 742.14 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 1.33 µs | 1.54 µs | 15.71 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 1.75 µs | 2.92 µs | 30.96 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 2.12 µs | 7.38 µs | 44.13 µs |

### Read after delete: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `post-ack-miss` | `delete-ack-ns` | 3.22 µs | 24.12 µs | 8.88 µs |
| `post-ack-miss` | `post-ack-miss-p50-ns` | 375 ns | 750 ns | 8.08 µs |
| `post-ack-miss` | `post-ack-miss-p95-ns` | 500 ns | 2.58 µs | 10.21 µs |
| `post-ack-miss` | `post-ack-miss-p99-ns` | 667 ns | 3.83 µs | 15.71 µs |

### Page reuse: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 60,903 cycle-keys/s | 20,699 cycle-keys/s | 122,706 cycle-keys/s |
| `random-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `random-half/batch=100` | `post-delete-persistent-B` | 14,736,866 B | 16,777,216 B | 5,676,151 B |
| `random-half/batch=100` | `active-post-reinsert-persistent-B` | 17,384,701 B | 16,777,216 B | 8,457,601 B |
| `random-half/batch=100` | `stable-post-reinsert-persistent-B` | 13,881,344 B | 16,777,216 B | — |
| `random-half/batch=100` | `reuse-active-growth-B` | 3,503,357 B | 0 B | 2,897,200 B |
| `random-half/batch=100` | `reuse-stable-growth-B` | 0 B | 0 B | — |
| `random-half/batch=100` | `reinsert-pages-reused` | 1,035 | — | — |

### Page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `random-half/batch=100` | `cycle-keys/s` | 334,944 cycle-keys/s | 306,861 cycle-keys/s | 1,051,555 cycle-keys/s |
| `random-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `random-half/batch=100` | `post-delete-persistent-B` | 14,736,866 B | 16,777,216 B | 5,676,151 B |
| `random-half/batch=100` | `active-post-reinsert-persistent-B` | 17,384,701 B | 16,777,216 B | 8,457,601 B |
| `random-half/batch=100` | `stable-post-reinsert-persistent-B` | 13,881,344 B | 16,777,216 B | — |
| `random-half/batch=100` | `reuse-active-growth-B` | 3,503,357 B | 0 B | 2,897,200 B |
| `random-half/batch=100` | `reuse-stable-growth-B` | 0 B | 0 B | — |
| `random-half/batch=100` | `reinsert-pages-reused` | 1,035 | — | — |

### Repeated page reuse: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 32,383 cycle-keys/s | 21,348 cycle-keys/s | 65,327 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 14,547,108 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 8,519,680 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,679 | — | — |

### Repeated page reuse: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `ten-cycles/batch=100` | `cycle-keys/s` | 459,668 cycle-keys/s | 428,046 cycle-keys/s | 1,273,928 cycle-keys/s |
| `ten-cycles/batch=100` | `loaded-persistent-B` | 8,556,544 B | 16,777,216 B | 5,560,401 B |
| `ten-cycles/batch=100` | `peak-persistent-B` | 14,547,108 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `active-final-persistent-B` | 8,519,680 B | 16,777,216 B | 34,532,401 B |
| `ten-cycles/batch=100` | `stable-final-persistent-B` | 8,519,680 B | 16,777,216 B | — |
| `ten-cycles/batch=100` | `stable-growth-B` | -36,864 B | 0 B | — |
| `ten-cycles/batch=100` | `pages-reused` | 7,679 | — | — |

### Tail reclamation: durable

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 52,218 ack-keys/s | 56,569 ack-keys/s | 81,870 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,334,779 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 2.37 ms | 86.45 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 77.71 µs | 1.39 ms | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Tail reclamation: no-commit-sync

| Case | Metric | KVLite | bbolt | Redis |
| --- | --- | ---: | ---: | ---: |
| `upper-half/batch=100` | `ack-keys/s` | 1,492,922 ack-keys/s | 1,486,709 ack-keys/s | 2,443,442 ack-keys/s |
| `upper-half/batch=100` | `loaded-persistent-B` | 13,881,344 B | 16,777,216 B | 5,560,401 B |
| `upper-half/batch=100` | `post-delete-persistent-B` | 14,334,779 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `final-persistent-B` | 6,955,008 B | 16,777,216 B | 5,676,151 B |
| `upper-half/batch=100` | `close-ns` | 451.93 µs | 70.50 µs | — |
| `upper-half/batch=100` | `reopen-ns` | 44.63 µs | 53.38 µs | — |
| `upper-half/batch=100` | `tail-pages-reclaimed` | 1,691 | — | — |

### Reopen after deletion: durable

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `alternating-half` | `reopen-ns` | 67.12 µs | 1.34 ms |

### Reopen after deletion: no-commit-sync

| Case | Metric | KVLite | bbolt |
| --- | --- | ---: | ---: |
| `alternating-half` | `reopen-ns` | 58.30 µs | 38.21 µs |

### Transactions

#### Read-only

| Keys per transaction | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| 1 | 1,726,677 keys/s | 1,035,096 keys/s | 130,267 keys/s |
| 10 | 2,432,127 keys/s | 1,812,898 keys/s | 776,536 keys/s |
| 100 | 2,503,697 keys/s | 1,900,195 keys/s | 1,719,103 keys/s |
| 1,000 | 2,531,522 keys/s | 1,929,630 keys/s | 2,181,922 keys/s |

#### Durable mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 14,710 operations/s | 7,634 operations/s | 15,030 operations/s |
| 95% | 100 | 143,641 operations/s | 61,699 operations/s | 147,360 operations/s |
| 95% | 1,000 | 803,622 operations/s | 273,648 operations/s | 587,287 operations/s |
| 50% | 10 | 15,347 operations/s | 3,991 operations/s | 14,177 operations/s |
| 50% | 100 | 126,726 operations/s | 24,452 operations/s | 110,026 operations/s |
| 50% | 1,000 | 620,718 operations/s | 125,973 operations/s | 349,539 operations/s |

#### No-commit-sync mixed transactions

| Read share | Operations per transaction | KVLite | bbolt | Redis |
| ---: | ---: | ---: | ---: | ---: |
| 95% | 10 | 1,081,756 operations/s | 510,450 operations/s | 505,119 operations/s |
| 95% | 100 | 1,484,169 operations/s | 1,216,539 operations/s | 1,042,217 operations/s |
| 95% | 1,000 | 1,746,009 operations/s | 1,430,314 operations/s | 1,022,753 operations/s |
| 50% | 10 | 890,483 operations/s | 268,270 operations/s | 416,037 operations/s |
| 50% | 100 | 1,206,286 operations/s | 531,878 operations/s | 812,147 operations/s |
| 50% | 1,000 | 1,489,326 operations/s | 738,394 operations/s | 843,978 operations/s |

### Enumeration

The 0% case returns no entries. This report omits its entries-per-second value because that rate does not apply.

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| selectivity=1 | 101,594,382 entries/s | 62,481,002 entries/s | 134,159 entries/s |
| selectivity=10 | 130,334,748 entries/s | 104,744,167 entries/s | 774,860 entries/s |
| selectivity=100 | 205,842,620 entries/s | 99,087,583 entries/s | 1,544,848 entries/s |

### Ordered operations

Redis does not provide the required ordered-cursor API. The 0% range case returns no entries. This report omits its entries-per-second value.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| full-forward | 218,091,363 entries/s | 97,900,839 entries/s |
| full-reverse | 230,933,571 entries/s | 110,240,894 entries/s |
| range/selectivity=1 | 110,281,240 entries/s | 67,765,596 entries/s |
| range/selectivity=10 | 129,550,517 entries/s | 93,603,260 entries/s |
| range/selectivity=100 | 211,851,597 entries/s | 97,577,196 entries/s |
| seek-and-read=1 | 2,286,322 entries/s | 910,520 entries/s |
| seek-and-read=10 | 18,985,398 entries/s | 8,497,850 entries/s |
| seek-and-read=100 | 64,360,335 entries/s | 54,228,476 entries/s |
| seek-and-read=1000 | 86,058,038 entries/s | 89,319,065 entries/s |

### Access distribution

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| records=10000/hot-80-20 | 2,926,531 reads/s | 982,292 reads/s | 143,118 reads/s |
| records=10000/uniform | 2,643,454 reads/s | 905,684 reads/s | 142,214 reads/s |
| records=100000/hot-80-20 | 1,806,636 reads/s | 1,331,192 reads/s | 135,128 reads/s |
| records=100000/uniform | 1,677,817 reads/s | 1,121,346 reads/s | 131,761 reads/s |

### Collections

KVLite and bbolt use native buckets. Redis uses logical key prefixes.

#### Durable

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 46,696 keys/s | 63,438 keys/s | 40,478 keys/s |
| write/collections=1/depth=3 | 53,232 keys/s | 54,627 keys/s | 46,138 keys/s |
| write/collections=100/depth=1 | 53,856 keys/s | 49,801 keys/s | 49,639 keys/s |
| write/collections=100/depth=3 | 54,453 keys/s | 59,870 keys/s | 45,956 keys/s |
| read/collections=1/depth=1 | 1,739,367 reads/s | 775,790 reads/s | 136,964 reads/s |
| read/collections=1/depth=3 | 1,310,795 reads/s | 595,080 reads/s | 134,317 reads/s |
| read/collections=100/depth=1 | 1,462,759 reads/s | 781,285 reads/s | 136,797 reads/s |
| read/collections=100/depth=3 | 1,051,774 reads/s | 637,554 reads/s | 132,401 reads/s |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| write/collections=1/depth=1 | 1,192,493 keys/s | 1,174,746 keys/s | 777,510 keys/s |
| write/collections=1/depth=3 | 1,309,813 keys/s | 982,477 keys/s | 599,745 keys/s |
| write/collections=100/depth=1 | 1,812,131 keys/s | 1,870,631 keys/s | 709,592 keys/s |
| write/collections=100/depth=3 | 1,719,184 keys/s | 2,043,809 keys/s | 559,101 keys/s |

### Latency

The large scale reports one, eight, and 32 clients.

#### Durable

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Read | 1 | KVLite | 333 ns | 375 ns | 542 ns | 405.08 µs |
| Read | 1 | bbolt | 417 ns | 708 ns | 1.38 µs | 1.23 ms |
| Read | 1 | Redis | 6.67 µs | 8.12 µs | 12.08 µs | 4.56 ms |
| Read | 8 | KVLite | 500 ns | 667 ns | 958 ns | 22.60 ms |
| Read | 8 | bbolt | 541 ns | 1.46 µs | 6.21 µs | 23.13 ms |
| Read | 8 | Redis | 27.96 µs | 90.67 µs | 161.96 µs | 9.34 ms |
| Read | 32 | KVLite | 500 ns | 667 ns | 916 ns | 61.10 ms |
| Read | 32 | bbolt | 625 ns | 30.21 µs | 178.50 µs | 34.94 ms |
| Read | 32 | Redis | 74.88 µs | 304.50 µs | 591.02 µs | 10.02 ms |
| Update | 1 | KVLite | 610.69 µs | 1.09 ms | 1.55 ms | 18.38 ms |
| Update | 1 | bbolt | 1.10 ms | 1.45 ms | 3.28 ms | 17.89 ms |
| Update | 1 | Redis | 585.55 µs | 775.63 µs | 939.34 µs | 11.96 ms |
| Update | 8 | KVLite | 597.50 µs | 909.80 µs | 1.34 ms | 6.34 ms |
| Update | 8 | bbolt | 20.48 ms | 23.99 ms | 26.70 ms | 40.85 ms |
| Update | 8 | Redis | 653.06 µs | 1.16 ms | 1.39 ms | 5.67 ms |
| Update | 32 | KVLite | 743.67 µs | 1.11 ms | 1.60 ms | 4.05 ms |
| Update | 32 | bbolt | 20.45 ms | 24.67 ms | 26.59 ms | 30.06 ms |
| Update | 32 | Redis | 967.30 µs | 1.43 ms | 1.85 ms | 3.74 ms |
| Mixed | 1 | KVLite | 708 ns | 392.29 µs | 680.38 µs | 8.82 ms |
| Mixed | 1 | bbolt | 709 ns | 208.92 µs | 1.21 ms | 9.61 ms |
| Mixed | 1 | Redis | 10.62 µs | 484.67 µs | 1.86 ms | 17.35 ms |
| Mixed | 8 | KVLite | 583 ns | 616.34 µs | 810.30 µs | 5.96 ms |
| Mixed | 8 | bbolt | 2.46 µs | 5.71 ms | 20.83 ms | 31.13 ms |
| Mixed | 8 | Redis | 96.33 µs | 2.15 ms | 3.13 ms | 18.59 ms |
| Mixed | 32 | KVLite | 458 ns | 740.13 µs | 1.22 ms | 10.93 ms |
| Mixed | 32 | bbolt | 2.54 µs | 7.75 ms | 21.61 ms | 28.17 ms |
| Mixed | 32 | Redis | 1.70 ms | 3.46 ms | 4.29 ms | 16.61 ms |

#### No commit sync

| Operation | Clients | Engine | p50 | p95 | p99 | Maximum |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Update | 1 | KVLite | 2.08 µs | 4.08 µs | 6.50 µs | 348.39 µs |
| Update | 1 | bbolt | 10.71 µs | 18.08 µs | 25.25 µs | 4.47 ms |
| Update | 1 | Redis | 9.46 µs | 12.96 µs | 17.62 µs | 619.01 µs |
| Update | 8 | KVLite | 2.12 µs | 4.42 µs | 9.00 µs | 7.67 ms |
| Update | 8 | bbolt | 11.00 µs | 20.63 µs | 3.89 ms | 9.72 ms |
| Update | 8 | Redis | 32.50 µs | 78.33 µs | 120.29 µs | 4.78 ms |
| Update | 32 | KVLite | 2.08 µs | 4.46 µs | 2.09 ms | 9.73 ms |
| Update | 32 | bbolt | 11.00 µs | 3.46 ms | 7.21 ms | 12.01 ms |
| Update | 32 | Redis | 115.33 µs | 336.27 µs | 489.54 µs | 5.23 ms |
| Mixed | 1 | KVLite | 458 ns | 1.75 µs | 2.58 µs | 262.45 µs |
| Mixed | 1 | bbolt | 625 ns | 9.29 µs | 12.42 µs | 2.26 ms |
| Mixed | 1 | Redis | 6.71 µs | 9.04 µs | 13.04 µs | 3.00 ms |
| Mixed | 8 | KVLite | 459 ns | 1.96 µs | 4.75 µs | 9.12 ms |
| Mixed | 8 | bbolt | 708 ns | 11.62 µs | 30.67 µs | 16.32 ms |
| Mixed | 8 | Redis | 29.33 µs | 91.04 µs | 178.16 µs | 6.56 ms |
| Mixed | 32 | KVLite | 459 ns | 2.00 µs | 15.46 µs | 16.47 ms |
| Mixed | 32 | bbolt | 708 ns | 14.83 µs | 1.84 ms | 20.94 ms |
| Mixed | 32 | Redis | 84.71 µs | 320.33 µs | 538.67 µs | 6.36 ms |

### Life cycle

Redis does not have the same embedded life-cycle boundary.

| Case | KVLite | bbolt |
| --- | ---: | ---: |
| close-after-writes | 2.12 ms | 12.83 µs |
| create-load-close | 42.76 ms | 62.01 ms |
| open-clean | 68.67 µs | 2.67 ms |
| recover-after-process-kill | 60.83 ms | 3.12 ms |

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
| update/random/grow=32-1024/clients=1 | 3,705,641 B | 4,194,304 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 24,427,107 B | 33,640,448 B | 11,443,301 B |
| update/random/grow=128-65536/clients=1 | 39,480,772 B | 50,573,312 B | 65,670,204 B |
| update/random/shrink=1024-32/clients=1 | 41,751,464 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,695,324 B | 35,549,184 B | 11,443,301 B |
| update/random/shrink=65536-128/clients=1 | 37,953,000 B | 51,425,280 B | 33,750,124 B |
| update/random/value=32/clients=1 | 1,418,880 B | 2,097,152 B | 825,401 B |
| update/random/value=128/clients=1 | 3,544,704 B | 4,194,304 B | 1,892,401 B |
| update/random/value=128/clients=1/batch=10 | 3,849,579 B | 4,194,304 B | 3,469,401 B |
| update/random/value=128/clients=1/batch=100 | 3,814,429 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=1/batch=1000 | 3,726,104 B | 8,388,608 B | 3,440,691 B |
| update/random/value=128/clients=8 | 3,604,404 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=8/batch=100 | 3,741,779 B | 4,194,304 B | 3,443,301 B |
| update/random/value=128/clients=32 | 3,595,854 B | 4,194,304 B | 2,270,801 B |
| update/random/value=128/clients=32/batch=100 | 3,646,329 B | 4,194,304 B | 3,443,301 B |
| update/random/value=1024/clients=1 | 41,678,464 B | 35,549,184 B | 11,759,401 B |
| update/random/value=3072/clients=1 | 41,686,656 B | 58,118,144 B | 34,287,401 B |
| update/random/value=65536/clients=1 | 36,232,424 B | 51,425,280 B | 99,160,124 B |
| update/sequential/value=128/clients=1/batch=10 | 3,642,354 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 3,600,679 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 3,596,929 B | 4,194,304 B | 3,440,691 B |
| insert/random/value=128/clients=1 | 7,050,633 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 6,493,423 B | 8,388,608 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 9,071,515 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 9,063,627 B | 8,388,608 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 6,211,688 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 6,698,400 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 4,286,677 B | 8,388,608 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 7,550,830 B | 8,388,608 B | 3,443,301 B |
| insert/random/value=65536/clients=1 | 75,235,680 B | 85,176,320 B | 99,160,124 B |
| insert/sequential/value=128/clients=1 | 7,437,497 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 7,923,868 B | 8,388,608 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 6,100,075 B | 8,388,608 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,247,142 B | 8,388,608 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 3,770,957 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 3,663,032 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 3,650,257 B | 4,194,304 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 3,515,105 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 3,505,305 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 3,503,355 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=65536/clients=1 | 35,833,664 B | 51,425,280 B | 49,973,624 B |

#### No commit sync

| Case | KVLite | bbolt | Redis |
| --- | ---: | ---: | ---: |
| update/random/grow=32-1024/clients=1 | 3,705,641 B | 2,469,888 B | 1,819,401 B |
| update/random/grow=32-1024/clients=1/batch=100 | 24,427,107 B | 19,394,560 B | 11,443,301 B |
| update/random/grow=128-65536/clients=1 | 39,480,772 B | 38,391,808 B | 65,670,204 B |
| update/random/shrink=1024-32/clients=1 | 41,751,464 B | 35,549,184 B | 10,765,401 B |
| update/random/shrink=1024-32/clients=1/batch=100 | 42,695,324 B | 35,549,184 B | 11,443,301 B |
| update/random/shrink=65536-128/clients=1 | 37,952,854 B | 51,425,280 B | 33,750,124 B |
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
| update/random/value=65536/clients=1 | 36,232,424 B | 51,425,280 B | 99,160,124 B |
| update/sequential/value=128/clients=1/batch=10 | 3,642,354 B | 4,194,304 B | 3,469,401 B |
| update/sequential/value=128/clients=1/batch=100 | 3,600,679 B | 4,194,304 B | 3,443,301 B |
| update/sequential/value=128/clients=1/batch=1000 | 3,596,929 B | 4,194,304 B | 3,440,691 B |
| insert/random/value=128/clients=1 | 7,050,633 B | 4,194,304 B | 1,892,401 B |
| insert/random/value=128/clients=1/batch=10 | 6,493,423 B | 5,910,528 B | 3,469,401 B |
| insert/random/value=128/clients=1/batch=100 | 9,071,515 B | 6,303,744 B | 3,443,301 B |
| insert/random/value=128/clients=1/batch=1000 | 9,063,627 B | 7,663,616 B | 3,440,691 B |
| insert/random/value=128/clients=8 | 7,257,499 B | 4,268,032 B | 2,270,801 B |
| insert/random/value=128/clients=8/batch=100 | 8,882,817 B | 6,287,360 B | 3,443,301 B |
| insert/random/value=128/clients=32 | 7,261,214 B | 4,268,032 B | 2,270,801 B |
| insert/random/value=128/clients=32/batch=100 | 8,923,219 B | 6,270,976 B | 3,443,301 B |
| insert/random/value=65536/clients=1 | 75,235,680 B | 73,461,760 B | 99,160,124 B |
| insert/sequential/value=128/clients=1 | 7,437,497 B | 4,194,304 B | 1,892,401 B |
| insert/sequential/value=128/clients=1/batch=10 | 7,923,868 B | 6,971,392 B | 3,469,401 B |
| insert/sequential/value=128/clients=1/batch=100 | 6,100,075 B | 6,971,392 B | 3,443,301 B |
| insert/sequential/value=128/clients=1/batch=1000 | 5,247,142 B | 6,971,392 B | 3,440,691 B |
| mixed/read=50/value=128/clients=1 | 3,770,957 B | 4,194,304 B | 2,580,401 B |
| mixed/read=50/value=128/clients=8 | 3,770,957 B | 4,222,976 B | 2,580,401 B |
| mixed/read=50/value=128/clients=32 | 3,770,957 B | 4,284,416 B | 2,580,401 B |
| mixed/read=95/value=128/clients=1 | 3,515,105 B | 4,194,304 B | 1,806,401 B |
| mixed/read=95/value=128/clients=8 | 3,515,105 B | 4,321,280 B | 1,806,401 B |
| mixed/read=95/value=128/clients=32 | 3,515,105 B | 4,608,000 B | 1,806,401 B |
| mixed/read=95/value=65536/clients=1 | 35,833,664 B | 51,425,280 B | 49,973,624 B |
