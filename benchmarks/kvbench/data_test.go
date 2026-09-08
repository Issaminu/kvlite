package kvbench

import (
	"encoding/binary"
	"testing"
)

func TestMakeKeyUsesWorkloadAndIndex(t *testing.T) {
	key := makeKey(7, 11)
	if len(key) != 16 {
		t.Fatalf("key length: got %d, want 16", len(key))
	}
	if got := binary.BigEndian.Uint64(key[:8]); got != 7 {
		t.Fatalf("workload: got %d, want 7", got)
	}
	if got := binary.BigEndian.Uint64(key[8:]); got != 11 {
		t.Fatalf("index: got %d, want 11", got)
	}
}

func TestRandomPairsAreDeterministicAndUnique(t *testing.T) {
	first, err := makePairs(1_000, 1, 7, keyOrderRandom, 42)
	if err != nil {
		t.Fatal(err)
	}
	second, err := makePairs(1_000, 1, 7, keyOrderRandom, 42)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]struct{}, len(first))
	for index, pair := range first {
		if string(pair.Key) != string(second[index].Key) {
			t.Fatal("random pairs changed for one seed")
		}
		seen[string(pair.Key)] = struct{}{}
	}
	if len(seen) != 1_000 {
		t.Fatalf("unique keys: got %d, want 1000", len(seen))
	}
}

func TestMakePairsUsesIndependentValueSlices(t *testing.T) {
	pairs, err := makePairs(2, 32, 9, keyOrderSequential, 1)
	if err != nil {
		t.Fatal(err)
	}
	pairs[0].Value[0] = 0
	if pairs[1].Value[0] == 0 {
		t.Fatal("pairs share a mutable value slice")
	}
}
