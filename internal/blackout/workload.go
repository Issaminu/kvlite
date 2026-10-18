package blackout

import (
	"fmt"
	"maps"
	"math/bits"
	"slices"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// nestedNames are the names of nested buckets.
// The workload also uses them as plain keys now and then, so a plain value and a nested bucket can collide on one name.
var nestedNames = []string{"n0", "n1", "n2", "n3"}

// maxBucketDepth is the deepest bucket path that the workload creates.
const maxBucketDepth = 4

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
	prefix := make([]byte, work.rand.Range(50, 500))
	work.rand.Fill(prefix)
	for id := range work.keys {
		var key []byte
		switch config.KeyShape {
		case keysOrdered:
			key = fmt.Appendf(nil, "%08d", id)
		case keysShort:
			key = make([]byte, work.rand.Range(1, 24))
			work.rand.Fill(key)
		case keysPrefixed:
			// Long shared prefixes shape branch separators and prefix scans.
			key = fmt.Appendf(append([]byte(nil), prefix[:work.rand.Range(1, len(prefix))]...), "/%06d", id)
		default:
			size := work.rand.Range(100, pageSize/2)
			switch {
			case work.rand.Chance(disk.OneIn(40)):
				// The largest key that KVLite accepts.
				size = kvlite.MaxKeySize
			case work.rand.Chance(disk.OneIn(20)):
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
// A transaction can be empty, or hold thousands of changes.
func (work *workload) newTransaction(model *stateModel) ([]txMutation, bool) {
	count := work.rand.Range(1, work.config.MaxMutations)
	if work.rand.Chance(disk.OneIn(30)) {
		count = 0
	}
	mutations := make([]txMutation, count)
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
		mutation.path = work.newBucketPath(model)
	case mutationDeleteBucket:
		mutation.path = work.pickBucket(model)
	case mutationDeleteBatch:
		mutation.path = work.pickBucket(model)
		mutation.keys = make([]string, work.rand.Range(1, 16))
		for index := range mutation.keys {
			mutation.keys[index] = work.pickKey()
		}
	default:
		mutation.path = work.pickBucket(model)
		mutation.key = work.pickKey()
		if kind == mutationPut {
			if old, ok := model.state[mutation.path][mutation.key]; ok && old != nil && work.rand.Chance(disk.OneIn(10)) {
				// Rewrite the same value: a change that changes nothing.
				mutation.value = old
			} else {
				mutation.value = work.newValue()
			}
		}
	}
	return mutation
}

// newBucketPath returns the path of a bucket to create.
// It can be a top-level bucket, a nested bucket, or a name that already exists.
func (work *workload) newBucketPath(model *stateModel) string {
	if work.config.NestedBuckets && len(model.state) > 0 && work.rand.Bool() {
		parent := work.pickBucket(model)
		if pathDepth(parent) < maxBucketDepth {
			name := nestedNames[work.rand.IntN(len(nestedNames))]
			return parent + "/" + name
		}
	}
	return work.bucketNames[work.rand.IntN(len(work.bucketNames))]
}

// pickBucket usually returns an existing bucket path.
// Sometimes it returns a top-level name, which can be missing.
func (work *workload) pickBucket(model *stateModel) string {
	existing := slices.Sorted(maps.Keys(model.state))
	if len(existing) > 0 && !work.rand.Chance(disk.OneIn(10)) {
		return existing[work.rand.IntN(len(existing))]
	}
	return work.bucketNames[work.rand.IntN(len(work.bucketNames))]
}

func (work *workload) pickKey() string {
	count := len(work.keys)
	if work.config.NestedBuckets && work.rand.Chance(disk.OneIn(20)) {
		// A nested bucket name as a plain key.
		return nestedNames[work.rand.IntN(len(nestedNames))]
	}
	switch work.config.Access {
	case accessAscending, accessDescending:
		if !work.rand.Chance(disk.OneIn(4)) {
			id := work.nextKey % count
			work.nextKey++
			if work.config.Access == accessDescending {
				id = count - 1 - id
			}
			return work.keys[id]
		}
	case accessSkewed:
		// The number of trailing zero bits of a random word is geometric, so small key IDs are much more likely than large ones.
		level := bits.TrailingZeros64(work.rand.Uint64() | 1<<62)
		return work.keys[work.rand.IntN(min(count, 1<<min(level, 30)))]
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
	case 2:
		size = work.rand.Range(work.pageSize/2, 3*work.pageSize)
	default:
		// Values of several MiB need long continuation chains.
		size = work.rand.Range(1<<20, 4<<20)
	}
	value := make([]byte, size)
	work.rand.Fill(value)
	return value
}

// scanKind is one way to read a range of a bucket.
type scanKind uint8

const (
	scanCursorForward scanKind = iota
	scanCursorBackward
	scanReverse
	scanRange
	scanPrefix
	scanSeek
	scanKindCount
)

var scanNames = [scanKindCount]string{"cursor forward", "cursor backward", "ScanReverse", "ScanRange", "ScanPrefix", "Seek and Next"}

// newScan picks a scan kind and its bounds.
func (work *workload) newScan() (kind scanKind, start, end string) {
	kind = scanKind(work.rand.IntN(int(scanKindCount)))
	switch kind {
	case scanRange:
		start, end = work.pickKey(), work.pickKey()
		if start > end {
			start, end = end, start
		}
	case scanPrefix:
		key := work.pickKey()
		start = key[:work.rand.Range(0, len(key))]
	case scanSeek:
		start = work.pickKey()
	}
	return kind, start, end
}
