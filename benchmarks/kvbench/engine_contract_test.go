package kvbench

import (
	"bytes"
	"context"
	"errors"
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
	engine, err := openBBoltEngine(filepath.Join(t.TempDir(), "bbolt.db"), mode)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}
