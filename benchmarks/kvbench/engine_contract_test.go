package kvbench

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

func TestEmbeddedEngineContract(t *testing.T) {
	for _, test := range []struct {
		name string
		open func(*testing.T, DurabilityMode) Engine
	}{
		{name: "kvlite", open: openTestKVLiteEngine},
		{name: "bbolt", open: openTestBBoltEngine},
		{name: "lmdb", open: openTestLMDBEngine},
	} {
		for _, mode := range []DurabilityMode{DurabilityDurable, DurabilityNoCommitSync} {
			t.Run(test.name+"/"+string(mode), func(t *testing.T) {
				testEngineContract(t, test.open(t, mode))
			})
		}
	}
}

func TestBBoltConcurrentBatchSettings(t *testing.T) {
	for _, clients := range []int{1, 8, 32} {
		for _, mode := range []DurabilityMode{DurabilityDurable, DurabilityNoCommitSync} {
			t.Run(fmt.Sprintf("%s/clients=%d", mode, clients), func(t *testing.T) {
				engine, err := openBBoltEngine(filepath.Join(t.TempDir(), "bbolt.db"), mode, clients)
				if err != nil {
					t.Fatal(err)
				}
				defer engine.Close()
				if err := engine.Validate(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := engine.Put(t.Context(), []byte("key"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				if err := engine.Delete(t.Context(), []byte("key")); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// TestWarmUpHandleKeepsStoredData checks that the untimed warm-up commit of a timed run leaves the stored data unchanged.
func TestWarmUpHandleKeepsStoredData(t *testing.T) {
	for _, test := range []struct {
		name string
		open func(*testing.T, DurabilityMode) Engine
	}{
		{name: "kvlite", open: openTestKVLiteEngine},
		{name: "bbolt", open: openTestBBoltEngine},
		{name: "lmdb", open: openTestLMDBEngine},
	} {
		for _, mode := range []DurabilityMode{DurabilityDurable, DurabilityNoCommitSync} {
			t.Run(test.name+"/"+string(mode), func(t *testing.T) {
				engine := test.open(t, mode)
				pairs, err := makePairs(8, 32, 1, keyOrderSequential, 1)
				if err != nil {
					t.Fatal(err)
				}
				if err := loadPairs(t.Context(), engine, pairs, 1_000); err != nil {
					t.Fatal(err)
				}
				warmUpHandle(t, engine, pairs[0])
				if count, err := engine.Count(t.Context()); err != nil || count != len(pairs) {
					t.Fatalf("count after warm-up: %d, %v", count, err)
				}
				for _, pair := range pairs {
					if value, err := engine.Get(t.Context(), pair.Key); err != nil || !bytes.Equal(value, pair.Value) {
						t.Fatalf("value of %x after warm-up: %v", pair.Key, err)
					}
				}
			})
		}
	}
}

func TestRedisEngineContract(t *testing.T) {
	address := os.Getenv("KVBENCH_REDIS_ADDR")
	if address == "" {
		t.Skip("KVBENCH_REDIS_ADDR is empty")
	}
	mode := DurabilityDurable
	if value := os.Getenv("KVBENCH_DURABILITY"); value != "" {
		mode = DurabilityMode(value)
	}
	engine, err := openRedisEngine(context.Background(), address, mode, 1, os.Getenv("KVBENCH_REDIS_FLUSHDB") == "1")
	if err != nil {
		t.Fatal(err)
	}
	testEngineContract(t, engine)
}

func TestBucketDeleteEngineContract(t *testing.T) {
	for _, test := range []struct {
		name string
		open func(*testing.T, DurabilityMode) Engine
	}{
		{name: "kvlite", open: openTestKVLiteEngine},
		{name: "bbolt", open: openTestBBoltEngine},
		{name: "lmdb", open: openTestLMDBEngine},
	} {
		for _, mode := range []DurabilityMode{DurabilityDurable, DurabilityNoCommitSync} {
			t.Run(test.name+"/"+string(mode), func(t *testing.T) {
				engine := test.open(t, mode)
				t.Cleanup(func() {
					if err := engine.Close(); err != nil {
						t.Errorf("close engine: %v", err)
					}
				})
				buckets, ok := engine.(bucketDeleteEngine)
				if !ok {
					t.Fatal("engine does not support bucket deletion")
				}
				ctx := context.Background()
				name := []byte("delete-me")
				pairs := []Pair{{Key: []byte("key"), Value: []byte("value")}}
				if err := buckets.PrepareBucket(ctx, name, pairs); err != nil {
					t.Fatal(err)
				}
				if exists, err := buckets.BucketExists(ctx, name); err != nil || !exists {
					t.Fatalf("prepared bucket: exists=%t, error=%v", exists, err)
				}
				if err := buckets.DeleteBucket(ctx, name); err != nil {
					t.Fatal(err)
				}
				if exists, err := buckets.BucketExists(ctx, name); err != nil || exists {
					t.Fatalf("deleted bucket: exists=%t, error=%v", exists, err)
				}
			})
		}
	}
}

func testEngineContract(t *testing.T, engine Engine) {
	t.Helper()
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("close engine: %v", err)
		}
	})
	ctx := context.Background()
	pairs, err := makePairs(100, 128, 1, keyOrderSequential, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := loadPairs(ctx, engine, pairs, 10); err != nil {
		t.Fatal(err)
	}
	for _, pair := range pairs {
		got, err := engine.Get(ctx, pair.Key)
		if err != nil || !bytes.Equal(got, pair.Value) {
			t.Fatalf("read %x: got %x, want %x, error %v", pair.Key, got, pair.Value, err)
		}
		got[0] = 0
		again, err := engine.Get(ctx, pair.Key)
		if err != nil || !bytes.Equal(again, pair.Value) {
			t.Fatalf("read result aliases engine data for %x", pair.Key)
		}
	}
	batchValues, err := engine.GetBatch(ctx, [][]byte{pairs[2].Key, pairs[3].Key})
	if err != nil || len(batchValues) != 2 || !bytes.Equal(batchValues[0], pairs[2].Value) || !bytes.Equal(batchValues[1], pairs[3].Value) {
		t.Fatalf("batch read: got %q with error %v", batchValues, err)
	}
	batchValues, err = engine.GetBatch(ctx, [][]byte{pairs[3].Key, makeKey(99, 99), pairs[2].Key, pairs[3].Key})
	if err != nil || len(batchValues) != 4 || batchValues[1] != nil || !bytes.Equal(batchValues[0], pairs[3].Value) || !bytes.Equal(batchValues[2], pairs[2].Value) || !bytes.Equal(batchValues[3], pairs[3].Value) {
		t.Fatalf("batch order, duplicates or missing slot: %q, %v", batchValues, err)
	}
	batchValues[0][0] ^= 1
	if !bytes.Equal(batchValues[3], pairs[3].Value) {
		t.Fatal("duplicate batch results share mutable memory")
	}
	if value, err := engine.Get(ctx, pairs[3].Key); err != nil || !bytes.Equal(value, pairs[3].Value) {
		t.Fatalf("batch result changed stored data: %q, %v", value, err)
	}
	if got, err := engine.Count(ctx); err != nil || got != len(pairs) {
		t.Fatalf("count: got %d, want %d, error %v", got, len(pairs), err)
	}
	if _, err := engine.Get(ctx, makeKey(99, 99)); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("missing read: got %v, want ErrKeyNotFound", err)
	}
	updates := []Pair{{Key: pairs[0].Key, Value: []byte("first")}, {Key: pairs[1].Key, Value: []byte("second")}}
	if err := engine.PutBatch(ctx, updates); err != nil {
		t.Fatal(err)
	}
	if got, err := engine.Get(ctx, pairs[1].Key); err != nil || string(got) != "second" {
		t.Fatalf("batch update: got %q, error %v", got, err)
	}
	mixedValues, err := engine.MixedBatch(ctx, [][]byte{pairs[2].Key}, []Pair{{Key: pairs[3].Key, Value: []byte("mixed")}})
	if err != nil {
		t.Fatal(err)
	}
	if len(mixedValues) != 1 || !bytes.Equal(mixedValues[0], pairs[2].Value) {
		t.Fatalf("mixed transaction read: got %q, want %q", mixedValues, pairs[2].Value)
	}
	if got, err := engine.Get(ctx, pairs[3].Key); err != nil || string(got) != "mixed" {
		t.Fatalf("mixed transaction: got %q, error %v", got, err)
	}
	visited := 0
	if err := engine.ScanPrefix(ctx, nil, func(key, value []byte) error {
		if len(key) == 0 || value == nil {
			t.Fatalf("enumeration returned invalid entry %x=%x", key, value)
		}
		visited++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if visited != len(pairs) {
		t.Fatalf("enumeration visited %d entries, want %d", visited, len(pairs))
	}
	stats, err := engine.StorageStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.primaryBytes < 0 || stats.logBytes < 0 || stats.memoryBytes < 0 || (stats.hasAllocatedBytes && stats.allocatedBytes < 0) {
		t.Fatalf("storage statistics contain a negative size: %+v", stats)
	}
	collections, ok := engine.(collectionEngine)
	if !ok {
		t.Fatal("engine does not implement collections")
	}
	path := [][]byte{[]byte("parent"), []byte("child")}
	if err := collections.PrepareCollections(ctx, [][][]byte{path}); err != nil {
		t.Fatal(err)
	}
	collectionPair := Pair{Key: []byte("collection-key"), Value: []byte("collection-value")}
	if err := collections.PutCollectionBatch(ctx, path, []Pair{collectionPair}); err != nil {
		t.Fatal(err)
	}
	if got, err := collections.GetCollection(ctx, path, collectionPair.Key); err != nil || !bytes.Equal(got, collectionPair.Value) {
		t.Fatalf("collection read: got %q, want %q, error %v", got, collectionPair.Value, err)
	}
	if ordered, ok := engine.(orderedEngine); ok {
		orderedCount := 0
		if err := ordered.VisitOrdered(ctx, nil, nil, false, 0, func(_, value []byte) error {
			if value != nil {
				orderedCount++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if orderedCount != len(pairs) {
			t.Fatalf("ordered visit returned %d entries, want %d", orderedCount, len(pairs))
		}
	}

	key := []byte("owned-put-key")
	value := []byte("owned-put-value")
	wantKey := bytes.Clone(key)
	wantValue := bytes.Clone(value)
	if err := engine.Put(ctx, key, value); err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	value[0] = 'x'
	if got, err := engine.Get(ctx, wantKey); err != nil || !bytes.Equal(got, wantValue) {
		t.Fatalf("Put retained input memory: got %q, want %q, error %v", got, wantValue, err)
	}

	batch := []Pair{{Key: []byte("owned-batch-key"), Value: []byte("owned-batch-value")}}
	wantBatchKey := bytes.Clone(batch[0].Key)
	wantBatchValue := bytes.Clone(batch[0].Value)
	if err := engine.PutBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	batch[0].Key[0] = 'x'
	batch[0].Value[0] = 'x'
	if got, err := engine.Get(ctx, wantBatchKey); err != nil || !bytes.Equal(got, wantBatchValue) {
		t.Fatalf("PutBatch retained input memory: got %q, want %q, error %v", got, wantBatchValue, err)
	}
	if err := engine.Put(ctx, []byte("empty-value"), nil); err != nil {
		t.Fatal(err)
	}
	if values, err := engine.GetBatch(ctx, [][]byte{[]byte("empty-value"), []byte("absent-value")}); err != nil || len(values) != 2 || values[0] == nil || len(values[0]) != 0 || values[1] != nil {
		t.Fatalf("empty and missing batch values: %q, %v", values, err)
	}

	beforeDeleteCount, err := engine.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Delete(ctx, pairs[0].Key); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Get(ctx, pairs[0].Key); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("deleted read: got %v, want ErrKeyNotFound", err)
	}
	if err := engine.Delete(ctx, pairs[0].Key); err != nil {
		t.Fatalf("repeated delete: %v", err)
	}
	if err := engine.DeleteBatch(ctx, [][]byte{pairs[1].Key, pairs[2].Key}); err != nil {
		t.Fatal(err)
	}
	if got, err := engine.Count(ctx); err != nil || got != beforeDeleteCount-3 {
		t.Fatalf("count after delete: got %d, want %d, error %v", got, beforeDeleteCount-3, err)
	}
}

func openTestKVLiteEngine(t *testing.T, mode DurabilityMode) Engine {
	t.Helper()
	engine, err := openKVLiteEngine(filepath.Join(t.TempDir(), "kvlite.db"), mode)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func openTestBBoltEngine(t *testing.T, mode DurabilityMode) Engine {
	t.Helper()
	engine, err := openBBoltEngine(filepath.Join(t.TempDir(), "bbolt.db"), mode, 1)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func openTestLMDBEngine(t *testing.T, mode DurabilityMode) Engine {
	t.Helper()
	engine, err := openLMDBEngine(filepath.Join(t.TempDir(), "lmdb.db"), mode)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func TestDurableEmbeddedEngineReopensWithoutCleanClose(t *testing.T) {
	if kind := os.Getenv("KVBENCH_CRASH_HELPER"); kind != "" {
		engine, err := openEngine(context.Background(), engineOpenOptions{
			Kind:        EngineKind(kind),
			Mode:        DurabilityDurable,
			DataDir:     os.Getenv("KVBENCH_CRASH_DIR"),
			ClientCount: 1,
		})
		if err == nil {
			err = engine.Put(context.Background(), []byte("crash-key"), []byte("crash-value"))
		}
		if err == nil && os.Getenv("KVBENCH_CRASH_DELETE") == "1" {
			err = engine.Delete(context.Background(), []byte("crash-key"))
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if os.Getenv("KVBENCH_CRASH_WAIT") == "1" {
			fmt.Fprintln(os.Stdout, "ready")
			for {
				time.Sleep(time.Second)
			}
		}
		os.Exit(0)
	}

	for _, kind := range []EngineKind{EngineKVLite, EngineBBolt, EngineLMDB} {
		t.Run(string(kind), func(t *testing.T) {
			for _, deleted := range []bool{false, true} {
				name := "put"
				if deleted {
					name = "delete"
				}
				t.Run(name, func(t *testing.T) {
					dataDir := t.TempDir()
					createKilledDatabase(t, kind, dataDir, deleted)
					engine, err := openEngine(context.Background(), engineOpenOptions{
						Kind:        kind,
						Mode:        DurabilityDurable,
						DataDir:     dataDir,
						ClientCount: 1,
					})
					if err != nil {
						t.Fatal(err)
					}
					defer engine.Close()
					got, err := engine.Get(context.Background(), []byte("crash-key"))
					if deleted {
						if !errors.Is(err, ErrKeyNotFound) {
							t.Fatalf("recovered delete: got %q, error %v", got, err)
						}
						return
					}
					if err != nil || string(got) != "crash-value" {
						t.Fatalf("recovered value: got %q, error %v", got, err)
					}
				})
			}
		})
	}
}

func createKilledDatabase(tb testing.TB, kind EngineKind, dataDir string, deleted bool) {
	tb.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestDurableEmbeddedEngineReopensWithoutCleanClose$")
	command.Env = append(os.Environ(), "KVBENCH_CRASH_HELPER="+string(kind), "KVBENCH_CRASH_DIR="+dataDir, "KVBENCH_CRASH_WAIT=1")
	if deleted {
		command.Env = append(command.Env, "KVBENCH_CRASH_DELETE=1")
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		tb.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		tb.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "ready" {
		_ = command.Process.Kill()
		_ = command.Wait()
		tb.Fatalf("crash helper did not become ready: %v", scanner.Err())
	}
	if err := command.Process.Kill(); err != nil {
		tb.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		tb.Fatal("crash helper exited without a kill error")
	}
	// Linux can release the file locks of a killed process some milliseconds after Wait returns.
	// Wait for them here, so a timed open measures recovery and not one lock retry of the engine.
	if err := waitForReleasedFileLocks(dataDir, 5*time.Second); err != nil {
		tb.Fatal(err)
	}
}

func TestLMDBVersion(t *testing.T) {
	_, _, _, version := lmdb.Version()
	t.Logf("%s; Go binding: github.com/PowerDNS/lmdb-go; map size: %d; maximum key size: %d; the 32768-byte key case is skipped", version, lmdbMapSize, lmdbMaxKeySize)
}

func TestLMDBTransactions(t *testing.T) {
	for _, mode := range []DurabilityMode{DurabilityDurable, DurabilityNoCommitSync} {
		t.Run(string(mode), func(t *testing.T) {
			engine := openTestLMDBEngine(t, mode).(*lmdbEngine)
			defer engine.Close()
			ctx := t.Context()
			key := []byte("key")
			if err := engine.Put(ctx, key, []byte("value")); err != nil {
				t.Fatal(err)
			}
			for _, read := range []func() ([][]byte, error){
				func() ([][]byte, error) { return engine.GetBatch(ctx, [][]byte{key, key}) },
				func() ([][]byte, error) { return engine.MixedBatch(ctx, [][]byte{key, key}, nil) },
			} {
				values, err := read()
				if err != nil {
					t.Fatal(err)
				}
				values[0][0] = 'x'
				if string(values[1]) != "value" {
					t.Fatal("read results share memory")
				}
				if value, err := engine.Get(ctx, key); err != nil || string(value) != "value" {
					t.Fatalf("read result changed stored data: %q, %v", value, err)
				}
			}
			invalid := []Pair{{Key: []byte("new"), Value: []byte("value")}, {Key: bytes.Repeat([]byte("x"), lmdbMaxKeySize+1), Value: []byte("value")}}
			for _, write := range []func() error{
				func() error { return engine.PutBatch(ctx, invalid) },
				func() error { _, err := engine.MixedBatch(ctx, [][]byte{key}, invalid); return err },
			} {
				if err := write(); err == nil {
					t.Fatal("accepted an oversized key")
				}
				if _, err := engine.Get(ctx, []byte("new")); !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("failed transaction kept a write: %v", err)
				}
			}
			if err := engine.Put(ctx, []byte("empty"), nil); err != nil {
				t.Fatal(err)
			}
			if value, err := engine.Get(ctx, []byte("empty")); err != nil || len(value) != 0 {
				t.Fatalf("empty value: %q, %v", value, err)
			}
			if err := engine.env.SetFlags(lmdb.NoMetaSync); err != nil {
				t.Fatal(err)
			}
			if err := engine.Validate(ctx); err == nil {
				t.Fatal("accepted a changed synchronization setting")
			}
		})
	}
}

func TestLMDBPutBatchPreservesOrderAndInputOwnership(t *testing.T) {
	for _, mode := range []DurabilityMode{DurabilityDurable, DurabilityNoCommitSync} {
		t.Run(string(mode), func(t *testing.T) {
			engine := openTestLMDBEngine(t, mode)
			defer engine.Close()
			ctx := t.Context()
			if err := engine.PutBatch(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if count, err := engine.Count(ctx); err != nil || count != 0 {
				t.Fatalf("empty batch changed data: count=%d error=%v", count, err)
			}
			pairs := []Pair{
				{Key: []byte("m"), Value: []byte("before")},
				{Key: []byte("z"), Value: nil},
				{Key: []byte("a"), Value: []byte("first")},
				{Key: []byte("m"), Value: []byte("after")},
				{Key: []byte("b"), Value: []byte{}},
			}
			original := make([]Pair, len(pairs))
			for index, pair := range pairs {
				original[index] = Pair{Key: bytes.Clone(pair.Key), Value: bytes.Clone(pair.Value)}
			}
			if err := engine.PutBatch(ctx, pairs); err != nil {
				t.Fatal(err)
			}
			for index, pair := range pairs {
				if !bytes.Equal(pair.Key, original[index].Key) || !bytes.Equal(pair.Value, original[index].Value) {
					t.Fatal("batch changed the caller's order or bytes")
				}
				pair.Key[0] ^= 1
				if len(pair.Value) > 0 {
					pair.Value[0] ^= 1
				}
			}
			for key, want := range map[string]string{"a": "first", "b": "", "m": "after", "z": ""} {
				value, err := engine.Get(ctx, []byte(key))
				if err != nil || value == nil || string(value) != want {
					t.Fatalf("batch value %q: got %q error=%v, want %q", key, value, err, want)
				}
			}
			if err := engine.Validate(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLMDBScans(t *testing.T) {
	engine := openTestLMDBEngine(t, DurabilityDurable).(*lmdbEngine)
	defer engine.Close()
	ctx := t.Context()
	for _, key := range []string{"a", "aa", "ab", "b"} {
		if err := engine.Put(ctx, []byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		name  string
		visit func(func([]byte, []byte) error) error
		want  []string
	}{
		{"prefix", func(fn func([]byte, []byte) error) error { return engine.ScanPrefix(ctx, []byte("a"), fn) }, []string{"a", "aa", "ab"}},
		{"missing-prefix", func(fn func([]byte, []byte) error) error { return engine.ScanPrefix(ctx, []byte("z"), fn) }, nil},
		{"range", func(fn func([]byte, []byte) error) error {
			return engine.VisitOrdered(ctx, []byte("aa"), []byte("b"), false, 0, fn)
		}, []string{"aa", "ab"}},
		{"limit", func(fn func([]byte, []byte) error) error { return engine.VisitOrdered(ctx, nil, nil, false, 2, fn) }, []string{"a", "aa"}},
		{"reverse", func(fn func([]byte, []byte) error) error {
			return engine.VisitOrdered(ctx, []byte("aa"), nil, true, 0, fn)
		}, []string{"b", "ab", "aa"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var keys []string
			if err := test.visit(func(key, value []byte) error {
				keys = append(keys, string(key))
				if !bytes.Equal(key, value) {
					t.Fatalf("scan value: %q=%q", key, value)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(keys, test.want) {
				t.Fatalf("keys: %q, want %q", keys, test.want)
			}
		})
	}
	stop := errors.New("stop scan")
	for _, visit := range []func(func([]byte, []byte) error) error{
		func(fn func([]byte, []byte) error) error { return engine.ScanPrefix(ctx, nil, fn) },
		func(fn func([]byte, []byte) error) error { return engine.VisitOrdered(ctx, nil, nil, false, 0, fn) },
	} {
		if err := visit(func(_, _ []byte) error { return stop }); !errors.Is(err, stop) {
			t.Fatalf("scan callback error: %v", err)
		}
	}
}

func TestExistingBucketOpenDoesNotRewriteData(t *testing.T) {
	for _, kind := range []EngineKind{EngineKVLite, EngineBBolt} {
		for _, mode := range []DurabilityMode{DurabilityDurable, DurabilityNoCommitSync} {
			t.Run(string(kind)+"/"+string(mode), func(t *testing.T) {
				options := engineOpenOptions{Kind: kind, Mode: mode, DataDir: t.TempDir(), ClientCount: 1}
				engine, err := openEngine(t.Context(), options)
				if err != nil {
					t.Fatal(err)
				}
				if err := engine.Put(t.Context(), []byte("kept"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				if err := engine.Close(); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(options.DataDir, string(kind)+".db")
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				engine, err = openEngine(t.Context(), options)
				if err != nil {
					t.Fatal(err)
				}
				defer engine.Close()
				value, err := engine.Get(t.Context(), []byte("kept"))
				if err != nil || string(value) != "value" {
					t.Fatalf("reopen lost data: %q %v", value, err)
				}
				after, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before, after) {
					t.Fatal("opening an existing benchmark bucket rewrote the database")
				}
			})
		}
	}
}
