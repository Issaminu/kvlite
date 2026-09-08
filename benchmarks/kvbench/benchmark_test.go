package kvbench

import (
	"bytes"
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
	records     int
	valueBytes  int
	order       keyOrder
	readPercent int
	clients     int
	batchSize   int
}

func benchmarkCases() []benchmarkCase {
	const records = 10_000
	var cases []benchmarkCase
	for _, valueBytes := range []int{32, 128, 1024, 3072} {
		cases = append(cases,
			benchmarkCase{
				name:       fmt.Sprintf("read/random/value=%d/clients=1", valueBytes),
				operation:  benchmarkRead,
				records:    records,
				valueBytes: valueBytes,
				order:      keyOrderRandom,
				clients:    1,
				batchSize:  1,
			},
			benchmarkCase{
				name:       fmt.Sprintf("write/random/value=%d/clients=1", valueBytes),
				operation:  benchmarkWrite,
				records:    records,
				valueBytes: valueBytes,
				order:      keyOrderRandom,
				clients:    1,
				batchSize:  1,
			},
		)
	}
	cases = append(cases,
		benchmarkCase{name: "read/sequential/value=128/clients=1", operation: benchmarkRead, records: records, valueBytes: 128, order: keyOrderSequential, clients: 1, batchSize: 1},
		benchmarkCase{name: "read/random/value=128/clients=8", operation: benchmarkRead, records: records, valueBytes: 128, order: keyOrderRandom, clients: 8, batchSize: 1},
		benchmarkCase{name: "write/random/value=128/clients=32", operation: benchmarkWrite, records: records, valueBytes: 128, order: keyOrderRandom, clients: 32, batchSize: 1},
		benchmarkCase{name: "mixed/read=95/value=128/clients=8", operation: benchmarkMixed, records: records, valueBytes: 128, order: keyOrderRandom, readPercent: 95, clients: 8, batchSize: 1},
		benchmarkCase{name: "mixed/read=50/value=128/clients=8", operation: benchmarkMixed, records: records, valueBytes: 128, order: keyOrderRandom, readPercent: 50, clients: 8, batchSize: 1},
	)
	for _, batchSize := range []int{10, 100, 1000} {
		cases = append(cases, benchmarkCase{
			name:       fmt.Sprintf("write/random/value=128/clients=1/batch=%d", batchSize),
			operation:  benchmarkWrite,
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
	mode := DurabilityDurable
	if value := os.Getenv("KVBENCH_DURABILITY"); value != "" {
		mode = DurabilityMode(value)
	}
	if mode != DurabilityDurable && mode != DurabilityNoCommitSync {
		b.Fatalf("KVBENCH_DURABILITY is %q", mode)
	}
	redisAddress := os.Getenv("KVBENCH_REDIS_ADDR")
	engines := []EngineKind{EngineKVLite, EngineBBolt}
	if redisAddress != "" {
		engines = append(engines, EngineRedis)
	}
	for _, benchmarkCase := range benchmarkCases() {
		for _, kind := range engines {
			b.Run(string(mode)+"/"+benchmarkCase.name+"/"+string(kind), func(b *testing.B) {
				benchmarkDatabase(b, kind, mode, redisAddress, benchmarkCase)
			})
		}
	}
}

func benchmarkDatabase(b *testing.B, kind EngineKind, mode DurabilityMode, redisAddress string, benchmarkCase benchmarkCase) {
	pairs, err := makePairs(benchmarkCase.records, benchmarkCase.valueBytes, 1, benchmarkCase.order, 1)
	if err != nil {
		b.Fatal(err)
	}
	engine, err := openEngine(b.Context(), engineOpenOptions{
		Kind:         kind,
		Mode:         mode,
		DataDir:      b.TempDir(),
		RedisAddr:    redisAddress,
		RedisFlushDB: os.Getenv("KVBENCH_REDIS_FLUSHDB") == "1",
		ClientCount:  benchmarkCase.clients,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := engine.Close(); err != nil {
			b.Error(err)
		}
	})
	if err := loadPairs(b.Context(), engine, pairs, 1_000); err != nil {
		b.Fatal(err)
	}
	value, err := engine.Get(b.Context(), pairs[0].Key)
	if err != nil || !bytes.Equal(value, pairs[0].Value) {
		b.Fatalf("initial read returned %d bytes and error %v", len(value), err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(benchmarkCase.valueBytes * benchmarkCase.batchSize))
	err = runBenchmarkOperations(b, engine, pairs, benchmarkCase)
	if err != nil {
		b.Fatal(err)
	}
	count, err := engine.Count(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	if count != benchmarkCase.records {
		b.Fatalf("database has %d keys, want %d", count, benchmarkCase.records)
	}
	b.ReportMetric(float64(b.N)*float64(benchmarkCase.batchSize)/b.Elapsed().Seconds(), "ack-keys/s")
}

func runBenchmarkOperations(b *testing.B, engine Engine, pairs []Pair, benchmarkCase benchmarkCase) error {
	run := func(start, end int) error {
		for operation := start; operation < end; operation++ {
			pair := pairs[operation%len(pairs)]
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
				batch := operation % (len(pairs) / benchmarkCase.batchSize)
				start := batch * benchmarkCase.batchSize
				if err := engine.PutBatch(b.Context(), pairs[start:start+benchmarkCase.batchSize]); err != nil {
					return err
				}
			case benchmarkMixed:
				if operation*37%100 < benchmarkCase.readPercent {
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
		err := run(0, b.N)
		b.StopTimer()
		return err
	}

	start := make(chan struct{})
	errorsByClient := make(chan error, benchmarkCase.clients)
	var ready sync.WaitGroup
	ready.Add(benchmarkCase.clients)
	for client := range benchmarkCase.clients {
		first := b.N * client / benchmarkCase.clients
		last := b.N * (client + 1) / benchmarkCase.clients
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
