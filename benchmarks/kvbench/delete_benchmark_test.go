package kvbench

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

type deleteBenchmarkCase struct {
	name       string
	records    int
	operations int
	order      keyOrder
	clients    int
	batchSize  int
	missing    bool
}

func deleteBenchmarkCases() []deleteBenchmarkCase {
	records := profileSizedValue(10_000, 3_000, 1_000)
	cases := []deleteBenchmarkCase{
		{name: "existing/random/clients=1", records: records, operations: records, order: keyOrderRandom, clients: 1, batchSize: 1},
		{name: "existing/random/clients=8", records: records, operations: records, order: keyOrderRandom, clients: 8, batchSize: 1},
		{name: "missing/random/clients=1", records: records, operations: records, order: keyOrderRandom, clients: 1, batchSize: 1, missing: true},
		{name: "existing/sequential/clients=1/batch=100", records: records, operations: records / 100, order: keyOrderSequential, clients: 1, batchSize: 100},
	}
	if os.Getenv("KVBENCH_WORKLOAD") == "focused" {
		return cases[:1]
	}
	if !lightProfile() && !mediumProfile() {
		cases = append(cases,
			deleteBenchmarkCase{name: "existing/sequential/clients=1", records: records, operations: records, order: keyOrderSequential, clients: 1, batchSize: 1},
			deleteBenchmarkCase{name: "existing/random/clients=1/batch=100", records: records, operations: records / 100, order: keyOrderRandom, clients: 1, batchSize: 100},
			deleteBenchmarkCase{name: "existing/random/clients=1/batch=1000", records: records, operations: records / 1_000, order: keyOrderRandom, clients: 1, batchSize: 1_000},
		)
	}
	return cases
}

func BenchmarkDeleteOperations(b *testing.B) {
	if !workloadEnabled(benchmarkDelete) {
		b.Skip("delete workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	for _, benchmarkCase := range deleteBenchmarkCases() {
		b.Run(string(environment.mode)+"/"+benchmarkCase.name+"/"+string(environment.kind), func(b *testing.B) {
			setup, err := makePairs(benchmarkCase.records, 128, 1, keyOrderSequential, 1)
			if err != nil {
				b.Fatal(err)
			}
			workload := uint64(1)
			if benchmarkCase.missing {
				workload = 2
			}
			operations, err := makePairs(benchmarkCase.records, 128, workload, benchmarkCase.order, 1)
			if err != nil {
				b.Fatal(err)
			}
			engine, _ := prepareDeleteBenchmarkEngine(b, environment, benchmarkCase.clients, setup)
			b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
			expected := expectedPairs(setup)

			transactions := benchmarkCase.operations
			keysDeleted := transactions * benchmarkCase.batchSize
			if err := runDeleteTransactions(b, engine, operations, transactions, benchmarkCase.batchSize, benchmarkCase.clients); err != nil {
				b.Fatal(err)
			}
			if !benchmarkCase.missing {
				removeExpectedPairs(expected, operations[:keysDeleted])
			}
			validateExpectedState(b, engine, expected)
			b.ReportMetric(float64(keysDeleted)/b.Elapsed().Seconds(), "ack-keys/s")
			b.ReportMetric(float64(transactions)/b.Elapsed().Seconds(), "transactions/s")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(transactions), "ns/op")
			reportDeleteStorage(b, engine)
		})
	}
}

func BenchmarkDeleteBuckets(b *testing.B) {
	if !workloadEnabled(benchmarkDelete) || os.Getenv("KVBENCH_WORKLOAD") == "focused" {
		b.Skip("bucket delete workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	if environment.kind == EngineRedis {
		b.Skip("Redis does not have native buckets")
	}
	const bucketCount = 100
	keysPerBucket := profileSizedValue(100, 30, 10)
	pairs, err := makePairs(keysPerBucket, 128, 1, keyOrderSequential, 1)
	if err != nil {
		b.Fatal(err)
	}
	b.Run(fmt.Sprintf("%s/top-level/buckets=%d/keys-per-bucket=%d/%s", environment.mode, bucketCount, keysPerBucket, environment.kind), func(b *testing.B) {
		options := engineOpenOptions{Kind: environment.kind, Mode: DurabilityDurable, DataDir: b.TempDir(), ClientCount: 1}
		engine, err := openEngine(b.Context(), options)
		if err != nil {
			b.Fatal(err)
		}
		buckets := engine.(bucketDeleteEngine)
		names := make([][]byte, bucketCount)
		for index := range names {
			names[index] = fmt.Appendf(nil, "delete-bucket-%03d", index)
			if err := buckets.PrepareBucket(b.Context(), names[index], pairs); err != nil {
				b.Fatal(err)
			}
		}
		if err := engine.Close(); err != nil {
			b.Fatal(err)
		}
		options.Mode = environment.mode
		engine, err = openEngine(b.Context(), options)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
		buckets = engine.(bucketDeleteEngine)
		b.ResetTimer()
		for _, name := range names {
			if err := buckets.DeleteBucket(b.Context(), name); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		for _, name := range names {
			exists, err := buckets.BucketExists(b.Context(), name)
			if err != nil || exists {
				b.Fatalf("bucket %q remains after delete: exists=%t, error=%v", name, exists, err)
			}
		}
		if err := engine.Validate(b.Context()); err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(bucketCount)/b.Elapsed().Seconds(), "buckets/s")
		b.ReportMetric(float64(bucketCount*keysPerBucket)/b.Elapsed().Seconds(), "keys/s")
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/bucketCount, "ns/op")
		reportDeleteStorage(b, engine)
	})
}

func runDeleteTransactions(b *testing.B, engine Engine, pairs []Pair, transactions, batchSize, clients int) error {
	keysByTransaction := make([][][]byte, transactions)
	for transaction := range transactions {
		start := transaction * batchSize
		keys := make([][]byte, batchSize)
		for offset := range batchSize {
			keys[offset] = pairs[start+offset].Key
		}
		keysByTransaction[transaction] = keys
	}
	run := func(first, last int) error {
		for transaction := first; transaction < last; transaction++ {
			keys := keysByTransaction[transaction]
			if batchSize == 1 {
				if err := engine.Delete(b.Context(), keys[0]); err != nil {
					return err
				}
				continue
			}
			if err := engine.DeleteBatch(b.Context(), keys); err != nil {
				return err
			}
		}
		return nil
	}
	if clients == 1 {
		b.ResetTimer()
		err := run(0, transactions)
		b.StopTimer()
		return err
	}

	start := make(chan struct{})
	errorsByClient := make(chan error, clients)
	var ready sync.WaitGroup
	ready.Add(clients)
	for client := range clients {
		first := transactions * client / clients
		last := transactions * (client + 1) / clients
		go func() {
			ready.Done()
			<-start
			errorsByClient <- run(first, last)
		}()
	}
	ready.Wait()
	b.ResetTimer()
	close(start)
	var firstError error
	for range clients {
		if err := <-errorsByClient; err != nil && firstError == nil {
			firstError = err
		}
	}
	b.StopTimer()
	return firstError
}

func BenchmarkDeleteVisibility(b *testing.B) {
	if !workloadEnabled(benchmarkDelete) {
		b.Skip("delete workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	operations := profileSizedValue(10_000, 3_000, 1_000)
	setup, err := makePairs(operations, 128, 1, keyOrderRandom, 1)
	if err != nil {
		b.Fatal(err)
	}
	b.Run(string(environment.mode)+"/post-ack-miss/"+string(environment.kind), func(b *testing.B) {
		engine, _ := prepareDeleteBenchmarkEngine(b, environment, 1, setup)
		b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
		readDurations := make([]time.Duration, operations)
		var deleteDuration time.Duration
		b.ResetTimer()
		for index := range operations {
			start := time.Now()
			if err := engine.Delete(b.Context(), setup[index].Key); err != nil {
				b.Fatal(err)
			}
			deleteDuration += time.Since(start)
			start = time.Now()
			_, err := engine.Get(b.Context(), setup[index].Key)
			readDurations[index] = time.Since(start)
			if !errors.Is(err, ErrKeyNotFound) {
				b.Fatalf("post-delete read returned %v, want ErrKeyNotFound", err)
			}
		}
		b.StopTimer()
		validateExpectedState(b, engine, nil)
		sort.Slice(readDurations, func(left, right int) bool { return readDurations[left] < readDurations[right] })
		b.ReportMetric(float64(deleteDuration.Nanoseconds())/float64(operations), "delete-ack-ns")
		b.ReportMetric(float64(readDurations[operations/2].Nanoseconds()), "post-ack-miss-p50-ns")
		b.ReportMetric(float64(readDurations[operations*95/100].Nanoseconds()), "post-ack-miss-p95-ns")
		b.ReportMetric(float64(readDurations[operations*99/100].Nanoseconds()), "post-ack-miss-p99-ns")
	})
}

func BenchmarkPageReuse(b *testing.B) {
	if !workloadEnabled(benchmarkDelete) {
		b.Skip("delete workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	records := profileSizedValue(10_000, 3_000, 1_000)
	setup, err := makePairs(records, 512, 1, keyOrderSequential, 1)
	if err != nil {
		b.Fatal(err)
	}
	deletes, err := makePairs(records, 512, 1, keyOrderRandom, 1)
	if err != nil {
		b.Fatal(err)
	}
	inserts, err := makePairs(records/2, 512, 2, keyOrderRandom, 1)
	if err != nil {
		b.Fatal(err)
	}
	b.Run(string(environment.mode)+"/random-half/batch=100/"+string(environment.kind), func(b *testing.B) {
		engine, options := prepareDeleteBenchmarkEngine(b, environment, 1, setup)
		b.Cleanup(func() {
			if engine != nil {
				closeBenchmarkEngine(b, engine)
			}
		})
		loaded := mustStorageStats(b, engine)
		deletePairs := deletes[:len(inserts)]
		expected := expectedPairs(setup)
		removeExpectedPairs(expected, deletePairs)
		addExpectedPairs(expected, inserts)
		b.ResetTimer()
		if err := deleteKeysInBatches(b, engine, deletePairs, 100); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		postDelete := mustStorageStats(b, engine)
		b.StartTimer()
		if err := loadPairs(b.Context(), engine, inserts, 100); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		active := mustStorageStats(b, engine)
		validateExpectedState(b, engine, expected)
		stable := active
		if environment.kind != EngineRedis {
			if err := engine.Close(); err != nil {
				b.Fatal(err)
			}
			engine = nil
			options.RedisFlushDB = false
			reopened, err := openEngine(b.Context(), options)
			if err != nil {
				b.Fatal(err)
			}
			engine = reopened
			stable = mustStorageStats(b, engine)
			validateExpectedState(b, engine, expected)
		}
		b.ReportMetric(float64(len(inserts)*2)/b.Elapsed().Seconds(), "cycle-keys/s")
		b.ReportMetric(float64((len(inserts)/100)*2)/b.Elapsed().Seconds(), "transactions/s")
		b.ReportMetric(float64(persistentBytes(loaded)), "loaded-persistent-B")
		b.ReportMetric(float64(persistentBytes(postDelete)), "post-delete-persistent-B")
		b.ReportMetric(float64(persistentBytes(active)), "active-post-reinsert-persistent-B")
		b.ReportMetric(float64(persistentBytes(active)-persistentBytes(loaded)), "reuse-active-growth-B")
		if environment.kind != EngineRedis {
			b.ReportMetric(float64(persistentBytes(stable)), "stable-post-reinsert-persistent-B")
			b.ReportMetric(float64(persistentBytes(stable)-persistentBytes(loaded)), "reuse-stable-growth-B")
		}
		if loaded.hasAllocatedBytes && postDelete.hasAllocatedBytes && active.hasAllocatedBytes {
			b.ReportMetric(float64(loaded.allocatedBytes), "loaded-os-allocated-B")
			b.ReportMetric(float64(postDelete.allocatedBytes), "post-delete-os-allocated-B")
			b.ReportMetric(float64(active.allocatedBytes), "active-post-reinsert-os-allocated-B")
			if environment.kind != EngineRedis && stable.hasAllocatedBytes {
				b.ReportMetric(float64(stable.allocatedBytes), "stable-post-reinsert-os-allocated-B")
			}
		}
		if active.hasAllocationStats {
			b.ReportMetric(float64(active.pagesReused-postDelete.pagesReused), "reinsert-pages-reused")
			b.ReportMetric(float64(active.reusablePages), "reusable-pages")
		}
	})
}

func BenchmarkPageReusePlateau(b *testing.B) {
	if !workloadEnabled(benchmarkDelete) {
		b.Skip("delete workloads are not selected")
	}
	const cycles = 10
	environment := readBenchmarkEnvironment(b)
	records := profileSizedValue(10_000, 3_000, 1_000)
	current, err := makePairs(records, 512, 1, keyOrderRandom, 1)
	if err != nil {
		b.Fatal(err)
	}
	insertsByCycle := make([][]Pair, cycles)
	for cycle := range insertsByCycle {
		insertsByCycle[cycle], err = makePairs(records/2, 512, uint64(cycle+2), keyOrderRandom, 1)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.Run(string(environment.mode)+"/ten-cycles/batch=100/"+string(environment.kind), func(b *testing.B) {
		engine, options := prepareDeleteBenchmarkEngine(b, environment, 1, current)
		b.Cleanup(func() {
			if engine != nil {
				closeBenchmarkEngine(b, engine)
			}
		})
		expected := expectedPairs(current)
		loaded := mustStorageStats(b, engine)
		peakPersistent := persistentBytes(loaded)
		b.ResetTimer()
		for cycle := range cycles {
			deleteCount := len(current) / 2
			deleted := current[:deleteCount]
			inserted := insertsByCycle[cycle]
			if err := deleteKeysInBatches(b, engine, deleted, 100); err != nil {
				b.Fatal(err)
			}
			if err := loadPairs(b.Context(), engine, inserted, 100); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			removeExpectedPairs(expected, deleted)
			addExpectedPairs(expected, inserted)
			current = append(current[:0], current[deleteCount:]...)
			current = append(current, inserted...)
			stats := mustStorageStats(b, engine)
			peakPersistent = max(peakPersistent, persistentBytes(stats))
			b.StartTimer()
		}
		b.StopTimer()
		active := mustStorageStats(b, engine)
		validateExpectedState(b, engine, expected)
		stable := active
		if environment.kind != EngineRedis {
			if err := engine.Close(); err != nil {
				b.Fatal(err)
			}
			engine = nil
			options.RedisFlushDB = false
			reopened, err := openEngine(b.Context(), options)
			if err != nil {
				b.Fatal(err)
			}
			engine = reopened
			stable = mustStorageStats(b, engine)
			validateExpectedState(b, engine, expected)
		}

		keysChanged := cycles * records
		transactions := cycles * 2 * ((records / 2) / 100)
		b.ReportMetric(float64(keysChanged)/b.Elapsed().Seconds(), "cycle-keys/s")
		b.ReportMetric(float64(transactions)/b.Elapsed().Seconds(), "transactions/s")
		b.ReportMetric(float64(persistentBytes(loaded)), "loaded-persistent-B")
		b.ReportMetric(float64(peakPersistent), "peak-persistent-B")
		b.ReportMetric(float64(persistentBytes(active)), "active-final-persistent-B")
		if environment.kind != EngineRedis {
			b.ReportMetric(float64(persistentBytes(stable)), "stable-final-persistent-B")
			b.ReportMetric(float64(persistentBytes(stable)-persistentBytes(loaded)), "stable-growth-B")
		}
		if active.hasAllocationStats {
			b.ReportMetric(float64(active.pagesReused-loaded.pagesReused), "pages-reused")
			b.ReportMetric(float64(active.reusablePages), "reusable-pages")
		}
	})
}

func BenchmarkTailReclamation(b *testing.B) {
	if !workloadEnabled(benchmarkDelete) {
		b.Skip("delete workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	records := profileSizedValue(10_000, 3_000, 1_000)
	setup, err := makePairs(records, 512, 1, keyOrderSequential, 1)
	if err != nil {
		b.Fatal(err)
	}
	b.Run(string(environment.mode)+"/upper-half/batch=100/"+string(environment.kind), func(b *testing.B) {
		engine, options := prepareDeleteBenchmarkEngine(b, environment, 1, setup)
		b.Cleanup(func() {
			if engine != nil {
				closeBenchmarkEngine(b, engine)
			}
		})
		loaded := mustStorageStats(b, engine)
		expected := expectedPairs(setup[:records/2])
		b.ResetTimer()
		if err := deleteKeysInBatches(b, engine, setup[records/2:], 100); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		postDelete := mustStorageStats(b, engine)
		var closeDuration time.Duration
		var reopenDuration time.Duration
		if environment.kind != EngineRedis {
			start := time.Now()
			if err := engine.Close(); err != nil {
				b.Fatal(err)
			}
			closeDuration = time.Since(start)
			engine = nil
			options.RedisFlushDB = false
			start = time.Now()
			reopened, err := openEngine(b.Context(), options)
			if err != nil {
				b.Fatal(err)
			}
			reopenDuration = time.Since(start)
			engine = reopened
		}
		final := mustStorageStats(b, engine)
		validateExpectedState(b, engine, expected)
		b.ReportMetric(float64(records/2)/b.Elapsed().Seconds(), "ack-keys/s")
		b.ReportMetric(float64(persistentBytes(loaded)), "loaded-persistent-B")
		b.ReportMetric(float64(persistentBytes(postDelete)), "post-delete-persistent-B")
		b.ReportMetric(float64(persistentBytes(final)), "final-persistent-B")
		reportByteChange(b, persistentBytes(loaded), persistentBytes(final), "persistent")
		if loaded.hasAllocatedBytes && final.hasAllocatedBytes {
			reportByteChange(b, loaded.allocatedBytes, final.allocatedBytes, "os")
		}
		if closeDuration > 0 {
			b.ReportMetric(float64(closeDuration.Nanoseconds()), "close-ns")
			b.ReportMetric(float64(reopenDuration.Nanoseconds()), "reopen-ns")
		}
		if postDelete.hasAllocationStats {
			b.ReportMetric(float64(postDelete.tailPagesReclaimed), "tail-pages-reclaimed")
		}
	})
}

func BenchmarkReopenAfterDelete(b *testing.B) {
	if !workloadEnabled(benchmarkDelete) {
		b.Skip("delete workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	if environment.kind == EngineRedis {
		b.Skip("a Redis server restart is not comparable to an embedded database reopen")
	}
	records := profileSizedValue(10_000, 3_000, 1_000)
	setup, err := makePairs(records, 128, 1, keyOrderSequential, 1)
	if err != nil {
		b.Fatal(err)
	}
	b.Run(string(environment.mode)+"/alternating-half/"+string(environment.kind), func(b *testing.B) {
		engine, options := prepareDeleteBenchmarkEngine(b, environment, 1, setup)
		b.Cleanup(func() {
			if engine != nil {
				closeBenchmarkEngine(b, engine)
			}
		})
		keys := make([][]byte, 0, records/2)
		expected := expectedPairs(setup)
		for index := 0; index < records; index += 2 {
			keys = append(keys, setup[index].Key)
			delete(expected, string(setup[index].Key))
		}
		if err := engine.DeleteBatch(b.Context(), keys); err != nil {
			b.Fatal(err)
		}
		if err := engine.Close(); err != nil {
			b.Fatal(err)
		}
		engine = nil
		b.ResetTimer()
		engine, err = openEngine(b.Context(), options)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		validateExpectedState(b, engine, expected)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds()), "reopen-ns")
	})
}

func prepareDeleteBenchmarkEngine(b *testing.B, environment benchmarkEnvironment, clients int, setup []Pair) (Engine, engineOpenOptions) {
	b.Helper()
	options := engineOpenOptions{
		Kind:         environment.kind,
		Mode:         DurabilityDurable,
		DataDir:      b.TempDir(),
		RedisAddr:    environment.redisAddress,
		RedisFlushDB: os.Getenv("KVBENCH_REDIS_FLUSHDB") == "1",
		ClientCount:  clients,
	}
	engine, err := openEngine(b.Context(), options)
	if err != nil {
		b.Fatal(err)
	}
	if err := loadPairs(b.Context(), engine, setup, 1_000); err != nil {
		b.Fatal(errors.Join(err, engine.Close()))
	}
	options.Mode = environment.mode
	if redisEngine, ok := engine.(*redisEngine); ok {
		if err := redisEngine.setDurability(b.Context(), environment.mode); err != nil {
			b.Fatal(errors.Join(err, engine.Close()))
		}
		if err := engine.Validate(b.Context()); err != nil {
			b.Fatal(err)
		}
		return engine, options
	}
	if err := engine.Close(); err != nil {
		b.Fatal(err)
	}
	engine, err = openEngine(b.Context(), options)
	if err != nil {
		b.Fatal(err)
	}
	if err := engine.Validate(b.Context()); err != nil {
		b.Fatal(err)
	}
	return engine, options
}

func expectedPairs(pairs []Pair) map[string][]byte {
	expected := make(map[string][]byte, len(pairs))
	addExpectedPairs(expected, pairs)
	return expected
}

func addExpectedPairs(expected map[string][]byte, pairs []Pair) {
	for _, pair := range pairs {
		expected[string(pair.Key)] = pair.Value
	}
}

func removeExpectedPairs(expected map[string][]byte, pairs []Pair) {
	for _, pair := range pairs {
		delete(expected, string(pair.Key))
	}
}

func validateExpectedState(b *testing.B, engine Engine, expected map[string][]byte) {
	b.Helper()
	if err := engine.Validate(b.Context()); err != nil {
		b.Fatal(err)
	}
	seen := make(map[string]struct{}, len(expected))
	err := engine.ScanPrefix(b.Context(), nil, func(key, value []byte) error {
		want, ok := expected[string(key)]
		if !ok {
			return fmt.Errorf("unexpected key %q", key)
		}
		if !bytes.Equal(value, want) {
			return fmt.Errorf("value for key %q is %q, want %q", key, value, want)
		}
		if _, duplicate := seen[string(key)]; duplicate {
			return fmt.Errorf("duplicate key %q", key)
		}
		seen[string(key)] = struct{}{}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	if len(seen) != len(expected) {
		b.Fatalf("database has %d expected keys, want %d", len(seen), len(expected))
	}
}

func reportByteChange(b *testing.B, before, after int64, prefix string) {
	b.Helper()
	change := after - before
	b.ReportMetric(float64(change), prefix+"-change-B")
	b.ReportMetric(float64(max(-change, 0)), prefix+"-reclaimed-B")
	b.ReportMetric(float64(max(change, 0)), prefix+"-growth-B")
}

func deleteKeysInBatches(b *testing.B, engine Engine, pairs []Pair, batchSize int) error {
	keys := make([][]byte, batchSize)
	for start := 0; start < len(pairs); start += batchSize {
		end := min(start+batchSize, len(pairs))
		batch := keys[:end-start]
		for index := start; index < end; index++ {
			batch[index-start] = pairs[index].Key
		}
		if err := engine.DeleteBatch(b.Context(), batch); err != nil {
			return fmt.Errorf("delete batch at %d: %w", start, err)
		}
	}
	return nil
}

func mustStorageStats(b *testing.B, engine Engine) storageStats {
	b.Helper()
	stats, err := engine.StorageStats(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	return stats
}

func persistentBytes(stats storageStats) int64 {
	return stats.primaryBytes + stats.logBytes
}

func reportDeleteStorage(b *testing.B, engine Engine) {
	b.Helper()
	stats := mustStorageStats(b, engine)
	b.ReportMetric(float64(stats.primaryBytes), "primary-B")
	b.ReportMetric(float64(stats.logBytes), "log-B")
	b.ReportMetric(float64(persistentBytes(stats)), "persistent-B")
	if stats.hasAllocatedBytes {
		b.ReportMetric(float64(stats.allocatedBytes), "os-allocated-B")
	}
	if stats.hasKVLiteWALStats {
		b.ReportMetric(float64(stats.walBytesWritten), "wal-written-B")
		b.ReportMetric(float64(stats.checkpointCount), "checkpoints")
	}
	if stats.hasAllocationStats {
		b.ReportMetric(float64(stats.allocatedPages), "allocated-pages")
		b.ReportMetric(float64(stats.reusablePages), "reusable-pages")
		b.ReportMetric(float64(stats.pagesRetired), "pages-retired")
	}
}
