package blackout

import (
	"fmt"
	"maps"
	"slices"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// workload is the workload generator. It plays the user.
// It picks each operation, the changes of each transaction, and the keys, values, and scan ranges.
type workload struct {
	rand        *disk.Rand
	config      *Config
	keys        []string
	bucketNames []string
	nextKey     int
	pageSize    int
}

func newWorkload(seed uint64, config *Config, pageSize int) *workload {
	work := &workload{rand: disk.NewRand(streamSeed(seed, streamWorkload)), config: config, pageSize: pageSize}
	for index := range config.BucketCount {
		work.bucketNames = append(work.bucketNames, fmt.Sprintf("bucket-%d", index))
	}
	work.keys = make([]string, config.KeyCount)
	for id := range work.keys {
		var key []byte
		switch config.KeyShape {
		case keysOrdered:
			key = fmt.Appendf(nil, "%08d", id)
		case keysShort:
			key = make([]byte, work.rand.Range(1, 24))
			work.rand.Fill(key)
		default:
			size := work.rand.Range(100, pageSize/2)
			if work.rand.Chance(disk.OneIn(20)) {
				size = work.rand.Range(pageSize, min(2*pageSize, kvlite.MaxKeySize))
			}
			key = make([]byte, size)
			work.rand.Fill(key)
		}
		work.keys[id] = string(key)
	}
	return work
}

// nextOperation picks the operation of the next step from the given weights.
func (work *workload) nextOperation(weights []uint64) operation {
	return operation(work.rand.Weighted(weights))
}

// newTransaction returns the changes of one write callback, and whether the callback aborts after them.
func (work *workload) newTransaction(model *stateModel) ([]txMutation, bool) {
	mutations := make([]txMutation, work.rand.Range(1, work.config.MaxMutations))
	for index := range mutations {
		mutations[index] = work.newMutation(model)
	}
	return mutations, work.rand.Chance(work.config.Abort)
}

func (work *workload) newMutation(model *stateModel) txMutation {
	kind := mutationKind(work.rand.Weighted(work.config.MutationWeights))
	if len(model.state) == 0 && !work.rand.Chance(disk.OneIn(10)) {
		kind = mutationCreateBucket
	}
	mutation := txMutation{kind: kind}
	switch kind {
	case mutationCreateBucket:
		mutation.bucket = work.bucketNames[work.rand.IntN(len(work.bucketNames))]
	case mutationDeleteBucket:
		mutation.bucket = work.pickBucket(model)
	default:
		mutation.bucket = work.pickBucket(model)
		mutation.key = work.pickKey()
		if kind == mutationPut {
			mutation.value = work.newValue()
		}
	}
	return mutation
}

// pickBucket usually returns an existing bucket.
// Sometimes it returns any name, which can be missing.
func (work *workload) pickBucket(model *stateModel) string {
	existing := slices.Sorted(maps.Keys(model.state))
	if len(existing) > 0 && !work.rand.Chance(disk.OneIn(10)) {
		return existing[work.rand.IntN(len(existing))]
	}
	return work.bucketNames[work.rand.IntN(len(work.bucketNames))]
}

func (work *workload) pickKey() string {
	count := len(work.keys)
	if work.config.Access != accessHotCold && !work.rand.Chance(disk.OneIn(4)) {
		id := work.nextKey % count
		work.nextKey++
		if work.config.Access == accessDescending {
			id = count - 1 - id
		}
		return work.keys[id]
	}
	if work.rand.Bool() {
		return work.keys[work.rand.IntN(min(4, count))]
	}
	return work.keys[work.rand.IntN(count)]
}

func (work *workload) newValue() []byte {
	var size int
	switch work.rand.Weighted(work.config.ValueWeights) {
	case 0:
		size = work.rand.Range(0, 48)
	case 1:
		size = work.rand.Range(49, work.pageSize/4)
	default:
		size = work.rand.Range(work.pageSize/2, 3*work.pageSize)
	}
	value := make([]byte, size)
	work.rand.Fill(value)
	return value
}

// scanKind is one way to read a range of a bucket.
type scanKind uint8

const (
	scanCursorForward scanKind = iota
	scanReverse
	scanRange
	scanSeek
	scanKindCount
)

// newScan picks a scan kind and its bounds.
func (work *workload) newScan() (kind scanKind, start, end string) {
	kind = scanKind(work.rand.IntN(int(scanKindCount)))
	switch kind {
	case scanRange:
		start, end = work.pickKey(), work.pickKey()
		if start > end {
			start, end = end, start
		}
	case scanSeek:
		start = work.pickKey()
	}
	return kind, start, end
}
