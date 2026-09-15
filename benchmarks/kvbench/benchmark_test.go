package kvbench

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

type benchmarkOperation string

const (
	benchmarkRead  benchmarkOperation = "read"
	benchmarkWrite benchmarkOperation = "write"
	benchmarkMixed benchmarkOperation = "mixed"
)

type benchmarkCase struct {
	name        string
	operation   benchmarkOperation
	operations  int
	records     int
	valueBytes  int
	order       keyOrder
	readPercent int
	clients     int
	batchSize   int
}

func benchmarkCases() []benchmarkCase {
	const (
		records              = 10_000
		pointReadOperations  = 100_000
		pointWriteOperations = 1_000
		concurrentOperations = 3_200
		mixedOperations      = 10_000
	)
	var cases []benchmarkCase
	for _, valueBytes := range []int{32, 128, 1024, 3072} {
		cases = append(cases,
			benchmarkCase{
				name:       fmt.Sprintf("read/random/value=%d/clients=1", valueBytes),
				operation:  benchmarkRead,
				operations: pointReadOperations,
				records:    records,
				valueBytes: valueBytes,
				order:      keyOrderRandom,
				clients:    1,
				batchSize:  1,
			},
			benchmarkCase{
				name:       fmt.Sprintf("write/random/value=%d/clients=1", valueBytes),
				operation:  benchmarkWrite,
				operations: pointWriteOperations,
				records:    records,
				valueBytes: valueBytes,
				order:      keyOrderRandom,
				clients:    1,
				batchSize:  1,
			},
		)
	}
	cases = append(cases,
		benchmarkCase{name: "read/sequential/value=128/clients=1", operation: benchmarkRead, operations: pointReadOperations, records: records, valueBytes: 128, order: keyOrderSequential, clients: 1, batchSize: 1},
		benchmarkCase{name: "read/random/value=128/clients=8", operation: benchmarkRead, operations: pointReadOperations, records: records, valueBytes: 128, order: keyOrderRandom, clients: 8, batchSize: 1},
		benchmarkCase{name: "write/random/value=128/clients=32", operation: benchmarkWrite, operations: concurrentOperations, records: records, valueBytes: 128, order: keyOrderRandom, clients: 32, batchSize: 1},
		benchmarkCase{name: "mixed/read=95/value=128/clients=8", operation: benchmarkMixed, operations: mixedOperations, records: records, valueBytes: 128, order: keyOrderRandom, readPercent: 95, clients: 8, batchSize: 1},
		benchmarkCase{name: "mixed/read=50/value=128/clients=8", operation: benchmarkMixed, operations: mixedOperations, records: records, valueBytes: 128, order: keyOrderRandom, readPercent: 50, clients: 8, batchSize: 1},
	)
	for _, batchSize := range []int{10, 100, 1000} {
		cases = append(cases, benchmarkCase{
			name:       fmt.Sprintf("write/random/value=128/clients=1/batch=%d", batchSize),
			operation:  benchmarkWrite,
			operations: records / batchSize,
			records:    records,
			valueBytes: 128,
			order:      keyOrderRandom,
			clients:    1,
			batchSize:  batchSize,
		})
	}
	return cases
}

func BenchmarkAcknowledgedOperations(b *testing.B) {
	if os.Getenv("KVBENCH_FIXED_WORK") != "1" {
		b.Skip("use run-docker.sh so every engine receives the same fixed work")
	}
	if b.N != 1 {
		b.Fatalf("benchmark calibration changed b.N to %d; run with -benchtime=1x", b.N)
	}
	mode := DurabilityDurable
	if value := os.Getenv("KVBENCH_DURABILITY"); value != "" {
		mode = DurabilityMode(value)
	}
	if mode != DurabilityDurable && mode != DurabilityNoCommitSync {
		b.Fatalf("KVBENCH_DURABILITY is %q", mode)
	}
	redisAddress := os.Getenv("KVBENCH_REDIS_ADDR")
	engine := EngineKind(os.Getenv("KVBENCH_ENGINE"))
	if engine != EngineKVLite && engine != EngineBBolt && engine != EngineRedis {
		b.Fatalf("KVBENCH_ENGINE is %q", engine)
	}
	if engine == EngineRedis && redisAddress == "" {
		b.Fatal("KVBENCH_REDIS_ADDR is empty")
	}
	for _, benchmarkCase := range benchmarkCases() {
		if mode == DurabilityNoCommitSync && benchmarkCase.operation == benchmarkRead {
			continue
		}
		b.Run(string(mode)+"/"+benchmarkCase.name+"/"+string(engine), func(b *testing.B) {
			benchmarkDatabase(b, engine, mode, redisAddress, benchmarkCase)
		})
	}
}

func benchmarkDatabase(b *testing.B, kind EngineKind, mode DurabilityMode, redisAddress string, benchmarkCase benchmarkCase) {
	setupPairs, err := makePairs(benchmarkCase.records, benchmarkCase.valueBytes, 1, keyOrderSequential, 1)
	if err != nil {
		b.Fatal(err)
	}
	operationPairs, err := makePairs(benchmarkCase.records, benchmarkCase.valueBytes, 1, benchmarkCase.order, 1)
	if err != nil {
		b.Fatal(err)
	}
	if benchmarkCase.operation != benchmarkRead {
		for index := range operationPairs {
			operationPairs[index].Value[0] ^= 0xff
		}
	}
	schedule := makeOperationSchedule(benchmarkCase.operations, len(operationPairs), benchmarkCase)
	engine, err := prepareBenchmarkEngine(b, kind, mode, redisAddress, benchmarkCase.clients, setupPairs)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := engine.Close(); err != nil {
			b.Error(err)
		}
	})
	if err := engine.Validate(b.Context()); err != nil {
		b.Fatal(err)
	}
	value, err := engine.Get(b.Context(), setupPairs[0].Key)
	if err != nil || !bytes.Equal(value, setupPairs[0].Value) {
		b.Fatalf("initial read returned %d bytes and error %v", len(value), err)
	}

	b.SetBytes(int64(benchmarkCase.valueBytes * benchmarkCase.batchSize * benchmarkCase.operations))
	err = runBenchmarkOperations(b, engine, operationPairs, benchmarkCase, schedule)
	if err != nil {
		b.Fatal(err)
	}
	if err := engine.Validate(b.Context()); err != nil {
		b.Fatal(err)
	}
	count, err := engine.Count(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	if count != benchmarkCase.records {
		b.Fatalf("database has %d keys, want %d", count, benchmarkCase.records)
	}
	if err := validateStoredPairs(b, engine, setupPairs, operationPairs, benchmarkCase, schedule); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(benchmarkCase.operations*benchmarkCase.batchSize)/b.Elapsed().Seconds(), "ack-keys/s")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(benchmarkCase.operations), "ns/op")
}

func prepareBenchmarkEngine(b *testing.B, kind EngineKind, mode DurabilityMode, redisAddress string, clients int, setupPairs []Pair) (Engine, error) {
	b.Helper()
	options := engineOpenOptions{
		Kind:         kind,
		Mode:         DurabilityDurable,
		DataDir:      b.TempDir(),
		RedisAddr:    redisAddress,
		RedisFlushDB: os.Getenv("KVBENCH_REDIS_FLUSHDB") == "1",
		ClientCount:  clients,
	}
	engine, err := openEngine(b.Context(), options)
	if err != nil {
		return nil, err
	}
	if err := loadPairs(b.Context(), engine, setupPairs, 1_000); err != nil {
		return nil, errors.Join(err, engine.Close())
	}
	if redisEngine, ok := engine.(*redisEngine); ok {
		if err := redisEngine.setDurability(b.Context(), mode); err != nil {
			return nil, errors.Join(err, engine.Close())
		}
		return engine, nil
	}
	if err := engine.Close(); err != nil {
		return nil, err
	}
	options.Mode = mode
	return openEngine(b.Context(), options)
}

type scheduledOperation struct {
	pairIndex int
	write     bool
}

func makeOperationSchedule(count, pairCount int, benchmarkCase benchmarkCase) []scheduledOperation {
	schedule := make([]scheduledOperation, count)
	for operation := range schedule {
		pairIndex := operation % pairCount
		write := benchmarkCase.operation == benchmarkWrite
		if benchmarkCase.operation == benchmarkMixed {
			value := uint64(operation) + 0x9e3779b97f4a7c15
			value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
			value = (value ^ (value >> 27)) * 0x94d049bb133111eb
			pairIndex = int((value ^ (value >> 31)) % uint64(pairCount))
			write = operation*37%100 >= benchmarkCase.readPercent
		}
		if benchmarkCase.batchSize > 1 {
			pairIndex = operation % (pairCount / benchmarkCase.batchSize) * benchmarkCase.batchSize
		}
		schedule[operation] = scheduledOperation{pairIndex: pairIndex, write: write}
	}
	return schedule
}

func validateStoredPairs(b *testing.B, engine Engine, setupPairs, operationPairs []Pair, benchmarkCase benchmarkCase, schedule []scheduledOperation) error {
	b.Helper()
	written := make(map[string][]byte)
	for _, operation := range schedule {
		if !operation.write {
			continue
		}
		for offset := range benchmarkCase.batchSize {
			pair := operationPairs[operation.pairIndex+offset]
			written[string(pair.Key)] = pair.Value
		}
	}
	for _, pair := range setupPairs {
		want := pair.Value
		if value, ok := written[string(pair.Key)]; ok {
			want = value
		}
		got, err := engine.Get(b.Context(), pair.Key)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("stored value for key %x does not match", pair.Key)
		}
	}
	return nil
}

func runBenchmarkOperations(b *testing.B, engine Engine, pairs []Pair, benchmarkCase benchmarkCase, schedule []scheduledOperation) error {
	run := func(start, end int) error {
		for operation := start; operation < end; operation++ {
			scheduled := schedule[operation]
			pair := pairs[scheduled.pairIndex]
			switch benchmarkCase.operation {
			case benchmarkRead:
				if _, err := engine.Get(b.Context(), pair.Key); err != nil {
					return err
				}
			case benchmarkWrite:
				if benchmarkCase.batchSize == 1 {
					if err := engine.Put(b.Context(), pair.Key, pair.Value); err != nil {
						return err
					}
					continue
				}
				if err := engine.PutBatch(b.Context(), pairs[scheduled.pairIndex:scheduled.pairIndex+benchmarkCase.batchSize]); err != nil {
					return err
				}
			case benchmarkMixed:
				if !scheduled.write {
					if _, err := engine.Get(b.Context(), pair.Key); err != nil {
						return err
					}
				} else if err := engine.Put(b.Context(), pair.Key, pair.Value); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown operation %q", benchmarkCase.operation)
			}
		}
		return nil
	}
	if benchmarkCase.clients == 1 {
		b.ResetTimer()
		err := run(0, len(schedule))
		b.StopTimer()
		return err
	}

	start := make(chan struct{})
	errorsByClient := make(chan error, benchmarkCase.clients)
	var ready sync.WaitGroup
	ready.Add(benchmarkCase.clients)
	for client := range benchmarkCase.clients {
		first := len(schedule) * client / benchmarkCase.clients
		last := len(schedule) * (client + 1) / benchmarkCase.clients
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
	for range benchmarkCase.clients {
		if err := <-errorsByClient; err != nil && firstError == nil {
			firstError = err
		}
	}
	b.StopTimer()
	return firstError
}
