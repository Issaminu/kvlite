package kvbench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestEmbeddedEngineContract(t *testing.T) {
	for _, test := range []struct {
		name string
		open func(*testing.T, DurabilityMode) Engine
	}{
		{name: "kvlite", open: openTestKVLiteEngine},
		{name: "bbolt", open: openTestBBoltEngine},
	} {
		for _, mode := range []DurabilityMode{DurabilityDurable, DurabilityNoCommitSync} {
			t.Run(test.name+"/"+string(mode), func(t *testing.T) {
				testEngineContract(t, test.open(t, mode))
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
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		os.Exit(0)
	}

	for _, kind := range []EngineKind{EngineKVLite, EngineBBolt} {
		t.Run(string(kind), func(t *testing.T) {
			dataDir := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestDurableEmbeddedEngineReopensWithoutCleanClose$")
			command.Env = append(os.Environ(), "KVBENCH_CRASH_HELPER="+string(kind), "KVBENCH_CRASH_DIR="+dataDir)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("crash helper: %v\n%s", err, output)
			}
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
			if err != nil || string(got) != "crash-value" {
				t.Fatalf("recovered value: got %q, error %v", got, err)
			}
		})
	}
}
