package kvbench

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

type benchmarkEnvironment struct {
	kind         EngineKind
	mode         DurabilityMode
	redisAddress string
}

func readBenchmarkEnvironment(b *testing.B) benchmarkEnvironment {
	b.Helper()
	if os.Getenv("KVBENCH_FIXED_WORK") != "1" {
		b.Skip("use run-docker.sh so every engine receives the same fixed work")
	}
	if b.N != 1 {
		b.Fatalf("benchmark calibration changed b.N to %d; run with -benchtime=1x", b.N)
	}
	environment := benchmarkEnvironment{
		kind:         EngineKind(os.Getenv("KVBENCH_ENGINE")),
		mode:         DurabilityMode(os.Getenv("KVBENCH_DURABILITY")),
		redisAddress: os.Getenv("KVBENCH_REDIS_ADDR"),
	}
	if environment.mode == "" {
		environment.mode = DurabilityDurable
	}
	if environment.mode != DurabilityDurable && environment.mode != DurabilityNoCommitSync {
		b.Fatalf("KVBENCH_DURABILITY is %q", environment.mode)
	}
	if environment.kind != EngineKVLite && environment.kind != EngineBBolt && environment.kind != EngineRedis {
		b.Fatalf("KVBENCH_ENGINE is %q", environment.kind)
	}
	if environment.kind == EngineRedis && environment.redisAddress == "" {
		b.Fatal("KVBENCH_REDIS_ADDR is empty")
	}
	return environment
}

func BenchmarkReadTransactions(b *testing.B) {
	if !workloadEnabled(benchmarkRead) {
		b.Skip("read workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	if environment.mode == DurabilityNoCommitSync {
		b.Skip("read results do not depend on commit sync mode")
	}
	records := profileSizedValue(10_000, 3_000, 1_000)
	operations := profileSizedValue(100_000, 10_000, 1_000)
	setup, err := makePairs(records, 128, 1, keyOrderSequential, 1)
	if err != nil {
		b.Fatal(err)
	}
	pairs, err := makePairs(records, 128, 1, keyOrderRandom, 1)
	if err != nil {
		b.Fatal(err)
	}
	batchSizes := []int{1, 10, 100, 1_000}
	if lightProfile() {
		batchSizes = []int{100}
	} else if mediumProfile() {
		batchSizes = []int{1, 100, 1_000}
	}
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("keys-per-transaction=%d/%s", batchSize, environment.kind), func(b *testing.B) {
			engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, 1, setup)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
			keys := make([][]byte, batchSize)
			transactions := operations / batchSize
			var checksum uint64
			b.SetBytes(int64(transactions * batchSize * 128))
			b.ResetTimer()
			for transaction := range transactions {
				for offset := range batchSize {
					keys[offset] = pairs[(transaction*batchSize+offset)%len(pairs)].Key
				}
				values, err := engine.GetBatch(b.Context(), keys)
				if err != nil {
					b.Fatal(err)
				}
				for index, value := range values {
					checksum = consumePair(checksum, keys[index], value)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(transactions*batchSize)/b.Elapsed().Seconds(), "keys/s")
			b.ReportMetric(checksumMetric(checksum), "checksum")
		})
	}
}

func BenchmarkMixedTransactions(b *testing.B) {
	if !workloadEnabled(benchmarkMixed) {
		b.Skip("mixed workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	records := profileSizedValue(10_000, 3_000, 1_000)
	operations := profileSizedValue(10_000, 3_000, 1_000)
	setup, err := makePairs(records, 128, 1, keyOrderSequential, 1)
	if err != nil {
		b.Fatal(err)
	}
	reads, err := makePairs(records, 128, 1, keyOrderRandom, 1)
	if err != nil {
		b.Fatal(err)
	}
	updates, err := makePairs(records, 128, 1, keyOrderRandom, 2)
	if err != nil {
		b.Fatal(err)
	}
	for index := range updates {
		updates[index].Value[0] ^= 0xff
	}
	readPercents := []int{95, 50}
	transactionSizes := []int{10, 100, 1_000}
	if lightProfile() {
		readPercents = []int{95}
		transactionSizes = []int{100}
	} else if mediumProfile() {
		transactionSizes = []int{10, 100}
	}
	for _, readPercent := range readPercents {
		for _, transactionSize := range transactionSizes {
			name := fmt.Sprintf("read=%d/operations-per-transaction=%d/%s", readPercent, transactionSize, environment.kind)
			b.Run(name, func(b *testing.B) {
				engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, 1, setup)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
				readCount := transactionSize * readPercent / 100
				writeCount := transactionSize - readCount
				keys := make([][]byte, readCount)
				writes := make([]Pair, writeCount)
				transactions := max(1, operations/transactionSize)
				var checksum uint64
				b.ResetTimer()
				for transaction := range transactions {
					base := transaction * transactionSize
					for index := range keys {
						keys[index] = reads[(base+index)%len(reads)].Key
					}
					for index := range writes {
						writes[index] = updates[(base+readCount+index)%len(updates)]
					}
					values, err := engine.MixedBatch(b.Context(), keys, writes)
					if err != nil {
						b.Fatal(err)
					}
					for index, value := range values {
						checksum = consumePair(checksum, keys[index], value)
					}
				}
				b.StopTimer()
				for transaction := range transactions {
					base := transaction * transactionSize
					for index := range writeCount {
						pair := updates[(base+readCount+index)%len(updates)]
						value, err := engine.Get(b.Context(), pair.Key)
						if err != nil || !bytes.Equal(value, pair.Value) {
							b.Fatalf("stored value for key %x does not match: %v", pair.Key, err)
						}
					}
				}
				b.ReportMetric(float64(transactions*transactionSize)/b.Elapsed().Seconds(), "operations/s")
				b.ReportMetric(checksumMetric(checksum), "checksum")
			})
		}
	}
}

func BenchmarkEnumeration(b *testing.B) {
	if !workloadEnabled(benchmarkRead) {
		b.Skip("read workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	if environment.mode == DurabilityNoCommitSync {
		b.Skip("enumeration results do not depend on commit sync mode")
	}
	records := profileValue(10_000, 1_000)
	pairs := makeTextPairs(records, 128)
	tests := []struct {
		name       string
		prefix     []byte
		want       int
		iterations int
	}{
		{name: "selectivity=0", prefix: []byte("none/"), want: 0, iterations: 1_000},
		{name: "selectivity=1", prefix: []byte("item/000000"), want: 100, iterations: 10_000},
		{name: "selectivity=10", prefix: []byte("item/00000"), want: 1_000, iterations: 1_000},
		{name: "selectivity=100", want: 10_000, iterations: 1_000},
	}
	if lightProfile() {
		tests = tests[3:]
		tests[0].want = records
	}
	for _, test := range tests {
		b.Run(test.name+"/"+string(environment.kind), func(b *testing.B) {
			engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, 1, pairs)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
			var checksum uint64
			b.ResetTimer()
			for range test.iterations {
				count := 0
				err := engine.ScanPrefix(b.Context(), test.prefix, func(key, value []byte) error {
					checksum = consumeScannedPair(checksum, key, value)
					count++
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
				if count != test.want {
					b.Fatalf("enumerated %d entries, want %d", count, test.want)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(test.want*test.iterations)/b.Elapsed().Seconds(), "entries/s")
			b.ReportMetric(checksumMetric(checksum), "checksum")
		})
	}
}

func BenchmarkOrderedOperations(b *testing.B) {
	const (
		seekMeasurementRepeats     = 100
		scanMeasurementRepeats     = 1_000
		fullScanMeasurementRepeats = 5_000
	)

	if !workloadEnabled(benchmarkRead) {
		b.Skip("read workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	if environment.mode == DurabilityNoCommitSync {
		b.Skip("ordered read results do not depend on commit sync mode")
	}
	if environment.kind == EngineRedis {
		b.Skip("Redis does not provide the ordered cursor contract used by this suite")
	}
	records := profileValue(10_000, 1_000)
	pairs := makeTextPairs(records, 128)
	engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, 1, pairs)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
	ordered, ok := engine.(orderedEngine)
	if !ok {
		b.Fatalf("%s does not implement ordered visits", environment.kind)
	}
	limits := []int{1, 10, 100, 1_000}
	if lightProfile() {
		limits = []int{10}
	} else if mediumProfile() {
		limits = []int{1, 100, 1_000}
	}
	for _, limit := range limits {
		iterations := records / limit * seekMeasurementRepeats
		name := fmt.Sprintf("seek-and-read=%d/%s", limit, environment.kind)
		b.Run(name, func(b *testing.B) {
			var checksum uint64
			visited := 0
			b.ResetTimer()
			for iteration := range iterations {
				start := pairs[distributedIndex(iteration, len(pairs)-limit+1, false)].Key
				if err := ordered.VisitOrdered(b.Context(), start, nil, false, limit, func(key, value []byte) error {
					checksum = consumeScannedPair(checksum, key, value)
					visited++
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if visited != iterations*limit {
				b.Fatalf("visited %d entries, want %d", visited, iterations*limit)
			}
			b.ReportMetric(float64(iterations*limit)/b.Elapsed().Seconds(), "entries/s")
			b.ReportMetric(checksumMetric(checksum), "checksum")
		})
	}
	rangeTests := []struct {
		name       string
		start      []byte
		end        []byte
		want       int
		iterations int
	}{
		{name: "selectivity=0", start: []byte("item/99999999"), end: []byte("item/99999999"), want: 0, iterations: profileValue(1_000, 10) * scanMeasurementRepeats},
		{name: "selectivity=1", start: pairs[0].Key, end: pairs[records/100].Key, want: records / 100, iterations: profileValue(100, 10) * scanMeasurementRepeats},
		{name: "selectivity=10", start: pairs[0].Key, end: pairs[records/10].Key, want: records / 10, iterations: profileValue(10, 2) * scanMeasurementRepeats},
		{name: "selectivity=100", want: records, iterations: scanMeasurementRepeats},
	}
	if lightProfile() {
		rangeTests = rangeTests[2:3]
	}
	for _, test := range rangeTests {
		b.Run("range/"+test.name+"/"+string(environment.kind), func(b *testing.B) {
			var checksum uint64
			b.ResetTimer()
			for range test.iterations {
				visited := 0
				if err := ordered.VisitOrdered(b.Context(), test.start, test.end, false, 0, func(key, value []byte) error {
					checksum = consumeScannedPair(checksum, key, value)
					visited++
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				if visited != test.want {
					b.Fatalf("visited %d entries, want %d", visited, test.want)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(test.want*test.iterations)/b.Elapsed().Seconds(), "entries/s")
			b.ReportMetric(checksumMetric(checksum), "checksum")
		})
	}
	directions := []bool{false, true}
	if lightProfile() {
		directions = []bool{false}
	}
	for _, reverse := range directions {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		b.Run("full-"+name+"/"+string(environment.kind), func(b *testing.B) {
			var checksum uint64
			b.ResetTimer()
			for range fullScanMeasurementRepeats {
				if err := ordered.VisitOrdered(b.Context(), nil, nil, reverse, 0, func(key, value []byte) error {
					checksum = consumeScannedPair(checksum, key, value)
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(records*fullScanMeasurementRepeats)/b.Elapsed().Seconds(), "entries/s")
			b.ReportMetric(checksumMetric(checksum), "checksum")
		})
	}
}

func BenchmarkScaleAndAccessDistribution(b *testing.B) {
	if !workloadEnabled(benchmarkRead) {
		b.Skip("read workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	if environment.mode == DurabilityNoCommitSync {
		b.Skip("read results do not depend on commit sync mode")
	}
	sizes := []int{10_000, 100_000}
	if lightProfile() {
		sizes = []int{1_000}
	} else if mediumProfile() {
		sizes = []int{10_000}
	}
	for _, records := range sizes {
		pairs, err := makePairs(records, 128, 1, keyOrderSequential, 1)
		if err != nil {
			b.Fatal(err)
		}
		for _, hot := range []bool{false, true} {
			name := "uniform"
			if hot {
				name = "hot-80-20"
			}
			b.Run(fmt.Sprintf("records=%d/%s/%s", records, name, environment.kind), func(b *testing.B) {
				engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, 1, pairs)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
				operations := profileSizedValue(100_000, 10_000, 1_000)
				b.SetBytes(int64(operations * 128))
				b.ResetTimer()
				for operation := range operations {
					index := distributedIndex(operation, records, hot)
					if _, err := engine.Get(b.Context(), pairs[index].Key); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(operations)/b.Elapsed().Seconds(), "reads/s")
			})
		}
	}
}

func BenchmarkLatency(b *testing.B) {
	environment := readBenchmarkEnvironment(b)
	records := profileSizedValue(10_000, 3_000, 1_000)
	setup, err := makePairs(records, 128, 1, keyOrderSequential, 1)
	if err != nil {
		b.Fatal(err)
	}
	reads, err := makePairs(records, 128, 1, keyOrderRandom, 1)
	if err != nil {
		b.Fatal(err)
	}
	updates, err := makePairs(records, 128, 1, keyOrderRandom, 2)
	if err != nil {
		b.Fatal(err)
	}
	for index := range updates {
		updates[index].Value[0] ^= 0xff
	}
	for _, operation := range []benchmarkOperation{benchmarkRead, benchmarkUpdate, benchmarkMixed} {
		if !workloadEnabled(operation) {
			continue
		}
		if environment.mode == DurabilityNoCommitSync && operation == benchmarkRead {
			continue
		}
		clientsValues := []int{1, 8, 32}
		if lightProfile() {
			clientsValues = []int{8}
		} else if mediumProfile() {
			clientsValues = []int{8}
		}
		for _, clients := range clientsValues {
			name := fmt.Sprintf("%s/clients=%d/%s", operation, clients, environment.kind)
			b.Run(name, func(b *testing.B) {
				engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, clients, setup)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
				operations := latencyOperationCount(operation)
				durations := make([]time.Duration, operations)
				if warmupOperations := latencyWarmupOperationCount(operation); warmupOperations > 0 {
					b.StopTimer()
					err = runLatencyOperations(b, engine, operation, clients, reads, updates, make([]time.Duration, warmupOperations))
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ResetTimer()
				err = runLatencyOperations(b, engine, operation, clients, reads, updates, durations)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				reportLatency(b, durations)
			})
		}
	}
}

func latencyOperationCount(operation benchmarkOperation) int {
	switch operation {
	case benchmarkRead:
		return profileSizedValue(1_000_000, 100_000, 10_000)
	case benchmarkMixed:
		return profileSizedValue(100_000, 10_000, 1_000)
	case benchmarkUpdate:
		return profileSizedValue(10_000, 1_000, 1_000)
	default:
		panic("unknown latency operation")
	}
}

func latencyWarmupOperationCount(operation benchmarkOperation) int {
	if operation != benchmarkRead {
		return 0
	}
	return profileSizedValue(10_000, 1_000, 100)
}

func makeTextPairs(count, valueBytes int) []Pair {
	pairs := make([]Pair, count)
	for index := range pairs {
		pairs[index] = Pair{
			Key:   fmt.Appendf(nil, "item/%08d", index),
			Value: bytes.Repeat([]byte{byte(index%251 + 1)}, valueBytes),
		}
	}
	return pairs
}

func consumePair(checksum uint64, key, value []byte) uint64 {
	digest := uint64(14_695_981_039_346_656_037)
	for _, character := range key {
		digest = (digest ^ uint64(character)) * 1_099_511_628_211
	}
	digest = (digest ^ 0xff) * 1_099_511_628_211
	for _, character := range value {
		digest = (digest ^ uint64(character)) * 1_099_511_628_211
	}
	return checksum + digest
}

func consumeScannedPair(checksum uint64, key, value []byte) uint64 {
	digest := uint64(len(key))*1_099_511_628_211 + uint64(len(value))
	if len(key) > 0 {
		digest = (digest ^ uint64(key[0])) * 1_099_511_628_211
		digest = (digest ^ uint64(key[len(key)-1])) * 1_099_511_628_211
	}
	if len(value) > 0 {
		digest = (digest ^ uint64(value[0])) * 1_099_511_628_211
		digest = (digest ^ uint64(value[len(value)-1])) * 1_099_511_628_211
	}
	return checksum + digest
}

func checksumMetric(checksum uint64) float64 {
	return float64(checksum & (1<<53 - 1))
}

func distributedIndex(operation, records int, hot bool) int {
	value := uint64(operation) + 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	random := int((value ^ (value >> 31)) % uint64(records))
	if !hot {
		return random
	}
	hotRecords := max(1, records/5)
	if operation%10 < 8 {
		return random % hotRecords
	}
	return hotRecords + random%(records-hotRecords)
}

func runLatencyOperations(b *testing.B, engine Engine, operation benchmarkOperation, clients int, reads, updates []Pair, durations []time.Duration) error {
	b.Helper()
	run := func(first, last int) error {
		for index := first; index < last; index++ {
			start := time.Now()
			var err error
			switch operation {
			case benchmarkRead:
				_, err = engine.Get(b.Context(), reads[index%len(reads)].Key)
			case benchmarkUpdate:
				err = engine.Put(b.Context(), updates[index%len(updates)].Key, updates[index%len(updates)].Value)
			case benchmarkMixed:
				if index*37%100 < 95 {
					_, err = engine.Get(b.Context(), reads[index%len(reads)].Key)
				} else {
					err = engine.Put(b.Context(), updates[index%len(updates)].Key, updates[index%len(updates)].Value)
				}
			default:
				err = fmt.Errorf("unknown latency operation %q", operation)
			}
			durations[index] = time.Since(start)
			if err != nil {
				return err
			}
		}
		return nil
	}
	start := make(chan struct{})
	errorsByClient := make(chan error, clients)
	var ready sync.WaitGroup
	ready.Add(clients)
	for client := range clients {
		first := len(durations) * client / clients
		last := len(durations) * (client + 1) / clients
		go func() {
			ready.Done()
			<-start
			errorsByClient <- run(first, last)
		}()
	}
	ready.Wait()
	close(start)
	var firstError error
	for range clients {
		if err := <-errorsByClient; err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func reportLatency(b *testing.B, durations []time.Duration) {
	b.Helper()
	slices.Sort(durations)
	percentile := func(percent int) float64 {
		index := (len(durations)*percent + 99) / 100
		return float64(durations[max(1, index)-1].Nanoseconds())
	}
	b.ReportMetric(percentile(50), "p50-ns")
	b.ReportMetric(percentile(95), "p95-ns")
	b.ReportMetric(percentile(99), "p99-ns")
	b.ReportMetric(float64(durations[len(durations)-1].Nanoseconds()), "max-ns")
}

func closeBenchmarkEngine(b *testing.B, engine Engine) {
	b.Helper()
	if err := engine.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		b.Error(err)
	}
}
