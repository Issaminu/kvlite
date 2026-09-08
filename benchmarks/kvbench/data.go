package kvbench

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
)

type keyOrder string

const (
	keyOrderSequential keyOrder = "sequential"
	keyOrderRandom     keyOrder = "random"
)

// Pair contains one key and one value for a benchmark operation.
type Pair struct {
	Key   []byte
	Value []byte
}

func makeKey(workload, index uint64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[:8], workload)
	binary.BigEndian.PutUint64(key[8:], index)
	return key
}

func makePairs(count, valueSize int, workload uint64, order keyOrder, seed int64) ([]Pair, error) {
	if count < 0 {
		return nil, fmt.Errorf("pair count must not be negative: %d", count)
	}
	if valueSize < 0 {
		return nil, fmt.Errorf("value size must not be negative: %d", valueSize)
	}
	pairs := make([]Pair, count)
	for index := range pairs {
		pairs[index] = Pair{
			Key:   makeKey(workload, uint64(index)),
			Value: bytes.Repeat([]byte{byte(index%251 + 1)}, valueSize),
		}
	}
	switch order {
	case keyOrderSequential:
	case keyOrderRandom:
		random := rand.New(rand.NewSource(seed))
		random.Shuffle(len(pairs), func(first, second int) {
			pairs[first], pairs[second] = pairs[second], pairs[first]
		})
	default:
		return nil, fmt.Errorf("unknown key order %q", order)
	}
	return pairs, nil
}
