package kvbench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
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
	if environment.kind != EngineKVLite && environment.kind != EngineBBolt && environment.kind != EngineRedis && environment.kind != EngineLMDB {
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
	records := scaleSizedValue(10_000, 3_000, 1_000)
	operations := scaleSizedValue(100_000, 10_000, 1_000)
	setup, err := makePairs(records, 128, 1, keyOrderSequential, 1)
	if err != nil {
		b.Fatal(err)
	}
	pairs, err := makePairs(records, 128, 1, keyOrderRandom, 1)
	if err != nil {
		b.Fatal(err)
	}
	batchSizes := []int{1, 10, 100, 1_000}
	if lightScale() {
		batchSizes = []int{100}
	} else if mediumScale() {
		batchSizes = []int{1, 100, 1_000}
	}
	for _, batchSize := range batchSizes {
		runScaledBenchmark(b, fmt.Sprintf("keys-per-transaction=%d/%s", batchSize, environment.kind), func(b *testing.B) {
			engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, 1, setup)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
			keys := make([][]byte, batchSize)
			transactions := operations / batchSize
			var checksum uint64
			b.SetBytes(int64(transactions * batchSize * 128))
			for b.Loop() {
				checksum = 0
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
			}
			b.ReportMetric(float64(b.N*transactions*batchSize)/b.Elapsed().Seconds(), "keys/s")
			b.ReportMetric(checksumMetric(checksum), "checksum")
		})
	}
}

func BenchmarkMixedTransactions(b *testing.B) {
	if !workloadEnabled(benchmarkMixed) {
		b.Skip("mixed workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	records := scaleSizedValue(10_000, 3_000, 1_000)
	operations := scaleSizedValue(10_000, 3_000, 1_000)
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
	if lightScale() {
		readPercents = []int{95}
		transactionSizes = []int{100}
	} else if mediumScale() {
		transactionSizes = []int{10, 100}
	}
	for _, readPercent := range readPercents {
		for _, transactionSize := range transactionSizes {
			name := fmt.Sprintf("%s/read=%d/operations-per-transaction=%d/%s", environment.mode, readPercent, transactionSize, environment.kind)
			runScaledBenchmark(b, name, func(b *testing.B) {
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
				iteration := 0
				for b.Loop() {
					if iteration > 0 {
						b.StopTimer()
						closeBenchmarkEngine(b, engine)
						engine, err = prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, 1, setup)
						if err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}
					checksum = 0
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
					iteration++
				}
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
				b.ReportMetric(float64(b.N*transactions*transactionSize)/b.Elapsed().Seconds(), "operations/s")
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
	records := scaleValue(10_000, 1_000)
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
	if lightScale() {
		tests = tests[3:]
		tests[0].want = records
	}
	for _, test := range tests {
		runScaledBenchmark(b, test.name+"/"+string(environment.kind), func(b *testing.B) {
			engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, 1, pairs)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
			var checksum uint64
			for b.Loop() {
				checksum = 0
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
			}
			b.ReportMetric(float64(b.N*test.want*test.iterations)/b.Elapsed().Seconds(), "entries/s")
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
	records := scaleValue(10_000, 1_000)
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
	if lightScale() {
		limits = []int{10}
	} else if mediumScale() {
		limits = []int{1, 100, 1_000}
	}
	for _, limit := range limits {
		iterations := records / limit * seekMeasurementRepeats
		name := fmt.Sprintf("seek-and-read=%d/%s", limit, environment.kind)
		runScaledBenchmark(b, name, func(b *testing.B) {
			var checksum uint64
			visited := 0
			for b.Loop() {
				checksum = 0
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
			}
			if visited != b.N*iterations*limit {
				b.Fatalf("visited %d entries, want %d", visited, b.N*iterations*limit)
			}
			b.ReportMetric(float64(b.N*iterations*limit)/b.Elapsed().Seconds(), "entries/s")
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
		{name: "selectivity=0", start: []byte("item/99999999"), end: []byte("item/99999999"), want: 0, iterations: scaleValue(1_000, 10) * scanMeasurementRepeats},
		{name: "selectivity=1", start: pairs[0].Key, end: pairs[records/100].Key, want: records / 100, iterations: scaleValue(100, 10) * scanMeasurementRepeats},
		{name: "selectivity=10", start: pairs[0].Key, end: pairs[records/10].Key, want: records / 10, iterations: scaleValue(10, 2) * scanMeasurementRepeats},
		{name: "selectivity=100", want: records, iterations: scanMeasurementRepeats},
	}
	if lightScale() {
		rangeTests = rangeTests[2:3]
	}
	for _, test := range rangeTests {
		runScaledBenchmark(b, "range/"+test.name+"/"+string(environment.kind), func(b *testing.B) {
			var checksum uint64
			for b.Loop() {
				checksum = 0
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
			}
			b.ReportMetric(float64(b.N*test.want*test.iterations)/b.Elapsed().Seconds(), "entries/s")
			b.ReportMetric(checksumMetric(checksum), "checksum")
		})
	}
	directions := []bool{false, true}
	if lightScale() {
		directions = []bool{false}
	}
	for _, reverse := range directions {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		runScaledBenchmark(b, "full-"+name+"/"+string(environment.kind), func(b *testing.B) {
			var checksum uint64
			for b.Loop() {
				checksum = 0
				for range fullScanMeasurementRepeats {
					if err := ordered.VisitOrdered(b.Context(), nil, nil, reverse, 0, func(key, value []byte) error {
						checksum = consumeScannedPair(checksum, key, value)
						return nil
					}); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(b.N*records*fullScanMeasurementRepeats)/b.Elapsed().Seconds(), "entries/s")
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
	if lightScale() {
		sizes = []int{1_000}
	} else if mediumScale() {
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
			runScaledBenchmark(b, fmt.Sprintf("records=%d/%s/%s", records, name, environment.kind), func(b *testing.B) {
				engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, 1, pairs)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
				operations := scaleSizedValue(100_000, 10_000, 1_000)
				b.SetBytes(int64(operations * 128))
				for b.Loop() {
					for operation := range operations {
						index := distributedIndex(operation, records, hot)
						if _, err := engine.Get(b.Context(), pairs[index].Key); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.ReportMetric(float64(b.N*operations)/b.Elapsed().Seconds(), "reads/s")
			})
		}
	}
}

func BenchmarkLatency(b *testing.B) {
	if !workloadEnabled(benchmarkRead) && !workloadEnabled(benchmarkUpdate) && !workloadEnabled(benchmarkMixed) {
		b.Skip("latency workloads are not selected")
	}
	environment := readBenchmarkEnvironment(b)
	records := scaleSizedValue(10_000, 3_000, 1_000)
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
		if lightScale() {
			clientsValues = []int{8}
		} else if mediumScale() {
			clientsValues = []int{8}
		}
		for _, clients := range clientsValues {
			name := fmt.Sprintf("%s/%s/clients=%d/%s", environment.mode, operation, clients, environment.kind)
			runScaledBenchmark(b, name, func(b *testing.B) {
				engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, clients, setup)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
				operations := latencyOperationCount(operation)
				durations := make([]time.Duration, operations)
				if warmupOperations := latencyWarmupOperationCount(operation); warmupOperations > 0 {
					err = runLatencyOperations(b, engine, operation, clients, reads, updates, make([]time.Duration, warmupOperations))
					if err != nil {
						b.Fatal(err)
					}
				}
				iteration := 0
				for b.Loop() {
					if iteration > 0 && operation != benchmarkRead {
						b.StopTimer()
						closeBenchmarkEngine(b, engine)
						engine, err = prepareBenchmarkEngine(b, environment.kind, environment.mode, environment.redisAddress, clients, setup)
						if err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}
					err = runLatencyOperations(b, engine, operation, clients, reads, updates, durations)
					if err != nil {
						b.Fatal(err)
					}
					iteration++
				}
				reportLatency(b, durations)
			})
		}
	}
}

func latencyOperationCount(operation benchmarkOperation) int {
	switch operation {
	case benchmarkRead:
		return scaleSizedValue(1_000_000, 100_000, 10_000)
	case benchmarkMixed:
		return scaleSizedValue(100_000, 10_000, 1_000)
	case benchmarkUpdate:
		return scaleSizedValue(10_000, 1_000, 1_000)
	default:
		panic("unknown latency operation")
	}
}

func latencyWarmupOperationCount(operation benchmarkOperation) int {
	if operation != benchmarkRead {
		return 0
	}
	return scaleSizedValue(10_000, 1_000, 100)
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

// batchAPICase describes a fixed single-bucket batch request schedule.
type batchAPICase struct {
	batch, value, hits int
	order              string
	fragmented, sync   bool
}

type syncEngine interface{ Sync(context.Context) error }

// BenchmarkGetBatch measures owned batch results, order, repeats, and missing keys.
func BenchmarkGetBatch(b *testing.B) {
	if !workloadEnabled(benchmarkRead) {
		b.Skip("read workloads are not selected")
	}
	env := readBenchmarkEnvironment(b)
	if env.mode != DurabilityDurable {
		b.Skip("reads do not depend on commit sync")
	}
	var cases []batchAPICase
	for _, batch := range []int{32, 256} {
		for _, order := range []string{"sequential", "shuffled", "random", "repeat"} {
			cases = append(cases, batchAPICase{batch: batch, value: 128, hits: 100, order: order})
			if order == "random" {
				cases = append(cases, batchAPICase{batch: batch, value: 128, hits: 50, order: order})
			}
		}
	}
	for _, order := range []string{"sequential", "random"} {
		cases = append(cases, batchAPICase{batch: 256, value: 128, hits: 0, order: order}, batchAPICase{batch: 32, value: 64 << 10, hits: 100, order: order})
	}
	for _, c := range cases {
		if lightScale() && (c.batch != 32 || c.value != 128 || c.order != "random" || c.hits != 100) {
			continue
		}
		name := fmt.Sprintf("read/value=%d/keys=%d/order=%s/hit=%d/%s", c.value, c.batch, c.order, c.hits, env.kind)
		runScaledBenchmark(b, name, func(b *testing.B) {
			records, reads := scaleSizedValue(8192, 2048, 512), scaleSizedValue(131072, 32768, 4096)
			if c.value > 128 {
				records, reads = scaleSizedValue(512, 128, 32), scaleSizedValue(2048, 512, 128)
			}
			setup, err := makePairs(records, c.value, 1, keyOrderSequential, 42)
			if err != nil {
				b.Fatal(err)
			}
			engine, err := prepareBenchmarkEngine(b, env.kind, env.mode, env.redisAddress, 1, setup)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
			keys := make([][]byte, reads)
			permutation := rand.New(rand.NewSource(42)).Perm(records)
			var wantChecksum uint64
			for i := range keys {
				index := i % records
				if c.order == "random" {
					index = permutation[index]
				}
				if c.order == "repeat" {
					index = permutation[((i/c.batch)*8+i%8)%records]
				}
				keys[i] = setup[index].Key
				value := setup[index].Value
				if c.hits == 0 || c.hits == 50 && i%2 == 0 {
					keys[i] = makeMissingKey("between", index)
					value = nil
				}
				wantChecksum = consumeScannedPair(wantChecksum, keys[i], value)
			}
			if c.order == "shuffled" {
				random := rand.New(rand.NewSource(42))
				for first := 0; first < len(keys); first += c.batch {
					group := keys[first : first+c.batch]
					random.Shuffle(len(group), func(i, j int) { group[i], group[j] = group[j], group[i] })
				}
			}
			// Touch the same requests before timing. Each timed call still returns new copies.
			for first := 0; first < len(keys); first += c.batch {
				if _, err := engine.GetBatch(b.Context(), keys[first:first+c.batch]); err != nil {
					b.Fatal(err)
				}
			}
			var checksum uint64
			b.ReportAllocs()
			b.SetBytes(int64(reads * c.value))
			for b.Loop() {
				checksum = 0
				for first := 0; first < len(keys); first += c.batch {
					requested := keys[first : first+c.batch]
					values, err := engine.GetBatch(b.Context(), requested)
					if err != nil {
						b.Fatal(err)
					}
					if len(values) != len(requested) {
						b.Fatal("batch read returned the wrong result count")
					}
					for i, value := range values {
						if (len(requested[i]) != 16) != (value == nil) {
							b.Fatal("batch read changed missing-key positions")
						}
						checksum = consumeScannedPair(checksum, requested[i], value)
					}
				}
			}
			if checksum != wantChecksum {
				b.Fatalf("batch read checksum: got %d, want %d", checksum, wantChecksum)
			}
			allKeys := make([][]byte, len(setup))
			for i, pair := range setup {
				allKeys[i] = pair.Key
			}
			values, err := engine.GetBatch(b.Context(), allKeys)
			if err != nil {
				b.Fatal(err)
			}
			for i, pair := range setup {
				if !bytes.Equal(values[i], pair.Value) {
					b.Fatalf("batch read changed key %x", pair.Key)
				}
			}
			b.ReportMetric(float64(b.N*reads)/b.Elapsed().Seconds(), "keys/s")
			// Default time and allocation metrics describe one fixed pass; ns/batch describes one API call.
			b.ReportMetric(float64(reads/c.batch), "batches/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*(reads/c.batch)), "ns/batch")
			b.ReportMetric(checksumMetric(checksum), "checksum")
		})
	}
}

// BenchmarkPutBatch measures changing values, duplicates, and inserts into a fragmented file.
func BenchmarkPutBatch(b *testing.B) {
	if !workloadEnabled(benchmarkUpdate) {
		b.Skip("write workloads are not selected")
	}
	env := readBenchmarkEnvironment(b)
	var cases []batchAPICase
	for _, batch := range []int{32, 256} {
		for _, order := range []string{"sequential", "random", "repeat"} {
			cases = append(cases, batchAPICase{batch: batch, value: 128, order: order})
		}
	}
	for _, order := range []string{"sequential", "random"} {
		cases = append(cases, batchAPICase{batch: 32, value: 64 << 10, order: order})
	}
	cases = append(cases, batchAPICase{batch: 32, value: 64 << 10, order: "random", fragmented: true})
	for _, c := range cases {
		if lightScale() && (c.batch != 32 || c.value != 128 || c.order != "random" || c.fragmented) {
			continue
		}
		if mediumScale() && c.order == "repeat" {
			continue
		}
		shape := "replace"
		if c.fragmented {
			shape = "fragmented-insert"
		}
		name := fmt.Sprintf("%s/%s/value=%d/keys=%d/order=%s/%s", env.mode, shape, c.value, c.batch, c.order, env.kind)
		runScaledBenchmark(b, name, func(b *testing.B) { benchmarkBatchPuts(b, env, c) })
	}
}

// BenchmarkSync measures writes followed by an explicit storage sync on a no-commit-sync handle.
// Redis has no matching direct flush call in this adapter, so it has no result.
func BenchmarkSync(b *testing.B) {
	if !workloadEnabled(benchmarkUpdate) {
		b.Skip("write workloads are not selected")
	}
	env := readBenchmarkEnvironment(b)
	if env.mode != DurabilityNoCommitSync || env.kind == EngineRedis {
		b.Skip("explicit sync needs an embedded no-commit-sync handle")
	}
	for _, batch := range []int{32, 256} {
		if lightScale() && batch != 32 {
			continue
		}
		c := batchAPICase{batch: batch, value: 128, order: "random", sync: true}
		name := fmt.Sprintf("%s/write-and-sync/value=128/keys=%d/%s", env.mode, batch, env.kind)
		runScaledBenchmark(b, name, func(b *testing.B) { benchmarkBatchPuts(b, env, c) })
	}
}

func benchmarkBatchPuts(b *testing.B, env benchmarkEnvironment, c batchAPICase) {
	records := scaleSizedValue(8192, 2048, 512)
	if c.value > 128 {
		records = scaleSizedValue(512, 128, 32)
	}
	initial := bytes.Repeat([]byte{1}, c.value)
	setup := make([]Pair, records)
	for i := range setup {
		setup[i] = Pair{Key: makeKey(1, uint64(i)), Value: initial}
	}
	permutation := rand.New(rand.NewSource(42)).Perm(records)
	phases := [2][][]Pair{}
	for phase := range phases {
		value := bytes.Repeat([]byte{byte(2 + phase)}, c.value)
		for first := 0; first < records; first += c.batch {
			writes := make([]Pair, 0, c.batch)
			for offset := range c.batch {
				index := (first + offset) % records
				if c.order == "random" {
					index = permutation[index]
				}
				if c.order == "repeat" {
					index = permutation[((first/c.batch)*8+offset%8)%records]
				}
				key := setup[index].Key
				if c.fragmented {
					key = makeKey(2, uint64(index))
				}
				writes = append(writes, Pair{Key: key, Value: value})
			}
			phases[phase] = append(phases[phase], writes)
		}
	}
	prepare := func() Engine {
		engine, err := prepareBenchmarkEngine(b, env.kind, env.mode, env.redisAddress, 1, setup)
		if err != nil {
			b.Fatal(err)
		}
		// Warm every value, then restore it. This makes both warm-up passes real writes.
		warm := make([]Pair, len(setup))
		for i, pair := range setup {
			warm[i] = Pair{Key: pair.Key, Value: bytes.Repeat([]byte{4}, c.value)}
		}
		if err := loadPairs(b.Context(), engine, warm, c.batch); err != nil {
			b.Fatal(err)
		}
		if err := loadPairs(b.Context(), engine, setup, c.batch); err != nil {
			b.Fatal(err)
		}
		if c.fragmented {
			keys := make([][]byte, 0, len(setup)/2)
			for i := 1; i < len(setup); i += 2 {
				keys = append(keys, setup[i].Key)
			}
			if err := engine.DeleteBatch(b.Context(), keys); err != nil {
				b.Fatal(err)
			}
		}
		if c.sync {
			syncer, ok := engine.(syncEngine)
			if !ok {
				b.Fatal("engine has no explicit sync")
			}
			if err := syncer.Sync(b.Context()); err != nil {
				b.Fatal(err)
			}
		}
		return engine
	}
	engine := prepare()
	b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
	counters := writeCounters{hasWAL: true, hasProcessIO: true}
	iteration := 0
	b.ReportAllocs()
	b.SetBytes(int64(records * c.value))
	for b.Loop() {
		b.StopTimer()
		if iteration > 0 && c.fragmented {
			closeBenchmarkEngine(b, engine)
			engine = prepare()
		}
		before := readWriteCounters(b, engine, env.kind)
		syncer, _ := engine.(syncEngine)
		b.StartTimer()
		for _, writes := range phases[iteration%2] {
			err := engine.PutBatch(b.Context(), writes)
			if err != nil {
				b.Fatal(err)
			}
			if c.sync {
				if err := syncer.Sync(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.StopTimer()
		counters.add(before, readWriteCounters(b, engine, env.kind))
		b.StartTimer()
		iteration++
	}
	if err := engine.Validate(b.Context()); err != nil {
		b.Fatal(err)
	}
	expected := make(map[string][]byte, len(setup)*2)
	for i, pair := range setup {
		if !c.fragmented || i%2 == 0 {
			expected[string(pair.Key)] = pair.Value
		}
	}
	for _, writes := range phases[(iteration-1)%2] {
		for _, write := range writes {
			expected[string(write.Key)] = write.Value
		}
	}
	count, err := engine.Count(b.Context())
	if err != nil || count != len(expected) {
		b.Fatalf("batch count: got %d, want %d, error %v", count, len(expected), err)
	}
	keys := make([][]byte, 0, len(expected))
	for key := range expected {
		keys = append(keys, []byte(key))
	}
	slices.SortFunc(keys, bytes.Compare)
	values, err := engine.GetBatch(b.Context(), keys)
	if err != nil {
		b.Fatal(err)
	}
	for i, key := range keys {
		if !bytes.Equal(values[i], expected[string(key)]) {
			b.Fatalf("wrong final batch value for %x", key)
		}
	}
	calls := float64(b.N * len(phases[0]))
	b.ReportMetric(float64(b.N*records)/b.Elapsed().Seconds(), "ack-keys/s")
	// Default time and allocation metrics describe one fixed pass; counters below describe one API call.
	b.ReportMetric(float64(len(phases[0])), "batches/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/calls, "ns/batch")
	if counters.hasWAL {
		b.ReportMetric(float64(counters.walCommits)/calls, "wal-commits/batch")
		b.ReportMetric(float64(counters.walWrittenBytes)/calls, "wal-B/batch")
	}
	if counters.hasProcessIO {
		b.ReportMetric(float64(counters.writeCalls)/calls, "write-calls/batch")
		b.ReportMetric(float64(counters.storageWrittenBytes)/calls, "storage-written-B/batch")
	}
	reportDeleteStorage(b, engine)
}
