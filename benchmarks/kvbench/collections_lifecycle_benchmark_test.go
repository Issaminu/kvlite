package kvbench

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
)

func BenchmarkCollections(b *testing.B) {
	environment := readBenchmarkEnvironment(b)
	for _, count := range []int{1, 100} {
		for _, depth := range []int{1, 3} {
			paths := makeCollectionPaths(count, depth)
			model := "native-buckets"
			if environment.kind == EngineRedis {
				model = "logical-prefixes"
			}
			name := fmt.Sprintf("collections=%d/depth=%d/model=%s/%s", count, depth, model, environment.kind)
			b.Run(name, func(b *testing.B) {
				benchmarkCollectionWrites(b, environment, paths)
			})
			if environment.mode == DurabilityDurable {
				b.Run("read/"+name, func(b *testing.B) {
					benchmarkCollectionReads(b, environment, paths)
				})
			}
		}
	}
}

func benchmarkCollectionWrites(b *testing.B, environment benchmarkEnvironment, paths [][][]byte) {
	engine, err := prepareCollectionEngine(b, environment, paths)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
	collections, ok := engine.(collectionEngine)
	if !ok {
		b.Fatalf("%s does not implement collections", environment.kind)
	}
	const totalKeys = 10_000
	const batchSize = 100
	transactions := make([][]Pair, totalKeys/batchSize)
	for transaction := range transactions {
		pathIndex := transaction % len(paths)
		pairs, err := makePairs(batchSize, 128, uint64(pathIndex+10), keyOrderSequential, 1)
		if err != nil {
			b.Fatal(err)
		}
		cycle := transaction / len(paths)
		for index := range pairs {
			pairs[index].Key = makeKey(uint64(pathIndex+10), uint64(cycle*batchSize+index))
		}
		transactions[transaction] = pairs
	}
	b.SetBytes(totalKeys * 128)
	b.ResetTimer()
	for transaction, pairs := range transactions {
		pathIndex := transaction % len(paths)
		if err := collections.PutCollectionBatch(b.Context(), paths[pathIndex], pairs); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(totalKeys/b.Elapsed().Seconds(), "keys/s")
}

func benchmarkCollectionReads(b *testing.B, environment benchmarkEnvironment, paths [][][]byte) {
	engine, err := prepareCollectionEngine(b, environment, paths)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
	collections, ok := engine.(collectionEngine)
	if !ok {
		b.Fatalf("%s does not implement collections", environment.kind)
	}
	const totalKeys = 10_000
	keysPerCollection := totalKeys / len(paths)
	pairsByCollection := make([][]Pair, len(paths))
	for pathIndex, path := range paths {
		pairs, err := makePairs(keysPerCollection, 128, uint64(pathIndex+10), keyOrderSequential, 1)
		if err != nil {
			b.Fatal(err)
		}
		if err := collections.PutCollectionBatch(b.Context(), path, pairs); err != nil {
			b.Fatal(err)
		}
		pairsByCollection[pathIndex] = pairs
	}
	const operations = 100_000
	var checksum uint64
	b.SetBytes(operations * 128)
	b.ResetTimer()
	for operation := range operations {
		pathIndex := operation % len(paths)
		key := pairsByCollection[pathIndex][operation%keysPerCollection].Key
		value, err := collections.GetCollection(b.Context(), paths[pathIndex], key)
		if err != nil {
			b.Fatal(err)
		}
		checksum = consumePair(checksum, key, value)
	}
	b.StopTimer()
	b.ReportMetric(operations/b.Elapsed().Seconds(), "reads/s")
	b.ReportMetric(checksumMetric(checksum), "checksum")
}

func prepareCollectionEngine(b *testing.B, environment benchmarkEnvironment, paths [][][]byte) (Engine, error) {
	b.Helper()
	options := engineOpenOptions{
		Kind:         environment.kind,
		Mode:         DurabilityDurable,
		DataDir:      b.TempDir(),
		RedisAddr:    environment.redisAddress,
		RedisFlushDB: os.Getenv("KVBENCH_REDIS_FLUSHDB") == "1",
		ClientCount:  1,
	}
	engine, err := openEngine(b.Context(), options)
	if err != nil {
		return nil, err
	}
	collections, ok := engine.(collectionEngine)
	if !ok {
		return nil, errors.Join(fmt.Errorf("%s does not implement collections", environment.kind), engine.Close())
	}
	if err := collections.PrepareCollections(b.Context(), paths); err != nil {
		return nil, errors.Join(err, engine.Close())
	}
	if redisEngine, ok := engine.(*redisEngine); ok {
		if err := redisEngine.setDurability(b.Context(), environment.mode); err != nil {
			return nil, errors.Join(err, engine.Close())
		}
		return engine, nil
	}
	if err := engine.Close(); err != nil {
		return nil, err
	}
	options.Mode = environment.mode
	return openEngine(b.Context(), options)
}

func makeCollectionPaths(count, depth int) [][][]byte {
	paths := make([][][]byte, count)
	for collection := range paths {
		path := make([][]byte, depth)
		for level := range path {
			path[level] = fmt.Appendf(nil, "c%03d-l%d", collection, level)
		}
		paths[collection] = path
	}
	return paths
}

func BenchmarkLifecycle(b *testing.B) {
	environment := readBenchmarkEnvironment(b)
	if environment.kind == EngineRedis {
		b.Skip("Redis server lifecycle work must run outside the client process")
	}
	if environment.mode == DurabilityNoCommitSync {
		b.Skip("recovery requires acknowledged durable writes")
	}
	b.Run("create-load-close/"+string(environment.kind), func(b *testing.B) {
		pairs, err := makePairs(10_000, 128, 1, keyOrderSequential, 1)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		engine, err := openEngine(b.Context(), engineOpenOptions{Kind: environment.kind, Mode: environment.mode, DataDir: b.TempDir(), ClientCount: 1})
		if err != nil {
			b.StopTimer()
			b.Fatal(err)
		}
		if err := loadPairs(b.Context(), engine, pairs, 1_000); err != nil {
			b.StopTimer()
			b.Fatal(errors.Join(err, engine.Close()))
		}
		err = engine.Close()
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(10_000/b.Elapsed().Seconds(), "loaded-keys/s")
	})

	b.Run("open-clean/"+string(environment.kind), func(b *testing.B) {
		dataDir := b.TempDir()
		engine, err := openEngine(b.Context(), engineOpenOptions{Kind: environment.kind, Mode: environment.mode, DataDir: dataDir, ClientCount: 1})
		if err != nil {
			b.Fatal(err)
		}
		if err := engine.Close(); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		engine, err = openEngine(b.Context(), engineOpenOptions{Kind: environment.kind, Mode: environment.mode, DataDir: dataDir, ClientCount: 1})
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if err := engine.Close(); err != nil {
			b.Fatal(err)
		}
	})

	b.Run("close-after-writes/"+string(environment.kind), func(b *testing.B) {
		pairs, err := makePairs(10_000, 128, 1, keyOrderSequential, 1)
		if err != nil {
			b.Fatal(err)
		}
		engine, err := prepareBenchmarkEngine(b, environment.kind, environment.mode, "", 1, pairs)
		if err != nil {
			b.Fatal(err)
		}
		updates := makeTextPairs(1_000, 128)
		if err := loadPairs(b.Context(), engine, updates, 100); err != nil {
			b.Fatal(errors.Join(err, engine.Close()))
		}
		b.ResetTimer()
		err = engine.Close()
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
	})

	b.Run("recover-after-process-kill/"+string(environment.kind), func(b *testing.B) {
		dataDir := b.TempDir()
		createKilledDatabase(b, environment.kind, dataDir)
		b.ResetTimer()
		engine, err := openEngine(b.Context(), engineOpenOptions{Kind: environment.kind, Mode: DurabilityDurable, DataDir: dataDir, ClientCount: 1})
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { closeBenchmarkEngine(b, engine) })
		value, err := engine.Get(b.Context(), []byte("crash-key"))
		if err != nil || !bytes.Equal(value, []byte("crash-value")) {
			b.Fatalf("recovered value is %q with error %v", value, err)
		}
	})
}
