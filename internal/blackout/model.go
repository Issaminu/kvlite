package blackout

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"

	"github.com/Issaminu/kvlite/internal/checksum"
)

// dbState is the reference content of a database: bucket name, then key, then value.
type dbState map[string]map[string][]byte

type mutationKind uint8

const (
	mutationPut mutationKind = iota
	mutationDelete
	mutationCreateBucket
	mutationDeleteBucket
	mutationGet
)

var mutationNames = [...]string{"put", "delete", "create-bucket", "delete-bucket", "get"}

// txMutation is one call inside a write transaction.
// A get changes nothing.
type txMutation struct {
	kind   mutationKind
	bucket string
	key    string
	value  []byte
}

func (mutation txMutation) String() string {
	return fmt.Sprintf("%s(%q, %d-byte key, %d-byte value)", mutationNames[mutation.kind], mutation.bucket, len(mutation.key), len(mutation.value))
}

// undoRecord reverts one change of the model.
type undoRecord struct {
	bucket      string
	key         string
	bucketLevel bool              // the change created or deleted the bucket
	existed     bool              // the key or bucket existed before the change
	value       []byte            // previous value of the key
	content     map[string][]byte // previous content of a deleted bucket
}

// commitRecord is one acknowledged commit after the durable floor.
type commitRecord struct {
	undo []undoRecord // reverts the commit, oldest change first
	hash uint64       // hash of the state after the commit
}

// branchRecord is a commit whose result is unknown, because its WAL write or sync failed.
// Recovery can show the state at parent with or without the mutations.
type branchRecord struct {
	parent    int
	mutations []txMutation
	hash      uint64
}

// stateModel holds the expected database state and every older state that recovery can still return.
//
// States are numbered by acknowledged commits.
// latest is the state that every reader must see.
// floor is the oldest state that a power failure can return, because later commits can still lack a sync.
// commits holds the undo records of states floor+1 to latest.
type stateModel struct {
	state     dbState
	hash      uint64
	journal   []undoRecord // changes since the last commit or rollback, oldest first
	floor     int
	floorHash uint64
	latest    int
	commits   []commitRecord
	branches  []branchRecord
}

func newStateModel() *stateModel {
	return &stateModel{state: dbState{}}
}

// entryHash returns a 64-bit hash of one entry or, with bucketLevel, of one bucket.
// The model combines entry hashes with XOR, so the order of changes does not matter.
// It uses CRC32C, like the rest of KVLite, and then a splitmix64 finish.
// The finish is required: CRC is linear, so XOR-combined raw CRCs give the same hash to two states that swap equal-length values between equal-length keys.
// Length prefixes keep bucket "a" with key "bc" apart from bucket "ab" with key "c".
func entryHash(bucket, key string, value []byte, bucketLevel bool) uint64 {
	var lengths [12]byte
	binary.LittleEndian.PutUint32(lengths[0:4], uint32(len(bucket)))
	binary.LittleEndian.PutUint32(lengths[4:8], uint32(len(key)))
	binary.LittleEndian.PutUint32(lengths[8:12], uint32(len(value)))
	kind := []byte{0}
	if bucketLevel {
		kind[0] = 1
	}
	hash := uint64(checksum.Sum32(kind, lengths[:], []byte(bucket), []byte(key), value))
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	hash *= 0x94d049bb133111eb
	return hash ^ hash>>31
}

func stateHash(state dbState) uint64 {
	var hash uint64
	for bucket, content := range state {
		hash ^= entryHash(bucket, "", nil, true)
		for key, value := range content {
			hash ^= entryHash(bucket, key, value, false)
		}
	}
	return hash
}

// apply changes the model and records the undo record in the journal.
// It reports whether the mutation changed the state.
func (model *stateModel) apply(mutation txMutation) bool {
	content := model.state[mutation.bucket]
	switch mutation.kind {
	case mutationPut:
		old, existed := content[mutation.key]
		model.journal = append(model.journal, undoRecord{bucket: mutation.bucket, key: mutation.key, existed: existed, value: old})
		if existed {
			model.hash ^= entryHash(mutation.bucket, mutation.key, old, false)
		}
		value := slices.Clone(mutation.value)
		if value == nil {
			value = []byte{}
		}
		content[mutation.key] = value
		model.hash ^= entryHash(mutation.bucket, mutation.key, value, false)
	case mutationDelete:
		old, existed := content[mutation.key]
		if !existed {
			return false
		}
		model.journal = append(model.journal, undoRecord{bucket: mutation.bucket, key: mutation.key, existed: true, value: old})
		model.hash ^= entryHash(mutation.bucket, mutation.key, old, false)
		delete(content, mutation.key)
	case mutationCreateBucket:
		model.journal = append(model.journal, undoRecord{bucket: mutation.bucket, bucketLevel: true})
		model.state[mutation.bucket] = map[string][]byte{}
		model.hash ^= entryHash(mutation.bucket, "", nil, true)
	case mutationDeleteBucket:
		model.journal = append(model.journal, undoRecord{bucket: mutation.bucket, bucketLevel: true, existed: true, content: content})
		model.hash ^= entryHash(mutation.bucket, "", nil, true)
		for key, value := range content {
			model.hash ^= entryHash(mutation.bucket, key, value, false)
		}
		delete(model.state, mutation.bucket)
	default:
		return false
	}
	return true
}

// undoInto reverts one change in state and returns the hash change.
func undoInto(state dbState, undo undoRecord) uint64 {
	var hash uint64
	if undo.bucketLevel {
		hash ^= entryHash(undo.bucket, "", nil, true)
		if !undo.existed {
			delete(state, undo.bucket)
			return hash
		}
		content := maps.Clone(undo.content)
		for key, value := range content {
			hash ^= entryHash(undo.bucket, key, value, false)
		}
		state[undo.bucket] = content
		return hash
	}
	content := state[undo.bucket]
	if current, ok := content[undo.key]; ok {
		hash ^= entryHash(undo.bucket, undo.key, current, false)
	}
	if undo.existed {
		content[undo.key] = undo.value
		hash ^= entryHash(undo.bucket, undo.key, undo.value, false)
	} else {
		delete(content, undo.key)
	}
	return hash
}

// mark returns the journal position for a later commit or rollback.
func (model *stateModel) mark() int {
	return len(model.journal)
}

// rollback reverts the journal back to mark.
func (model *stateModel) rollback(mark int) {
	for index := len(model.journal) - 1; index >= mark; index-- {
		model.hash ^= undoInto(model.state, model.journal[index])
	}
	model.journal = model.journal[:mark]
}

// commit turns the journal since mark into one acknowledged commit.
// A commit that changed nothing adds no state, and it reports false.
func (model *stateModel) commit(mark int) bool {
	if len(model.journal) == mark {
		return false
	}
	undo := slices.Clone(model.journal[mark:])
	model.journal = model.journal[:mark]
	model.latest++
	model.commits = append(model.commits, commitRecord{undo: undo, hash: model.hash})
	return true
}

// uncertain records the journal since mark as a branch with an unknown result, then reverts it.
func (model *stateModel) uncertain(mark int, mutations []txMutation) {
	hash := model.hash
	model.rollback(mark)
	if hash == model.hash && len(mutations) == 0 {
		return
	}
	model.branches = append(model.branches, branchRecord{parent: model.latest, mutations: slices.Clone(mutations), hash: hash})
}

// setFloor marks every state before latest as unrecoverable after a power failure.
// The caller knows that latest is durable.
func (model *stateModel) setFloor() {
	model.floor = model.latest
	model.floorHash = model.hash
	model.commits = nil
	model.branches = slices.DeleteFunc(model.branches, func(branch branchRecord) bool { return branch.parent < model.floor })
}

// hashAt returns the hash of state index, for floor <= index <= latest.
func (model *stateModel) hashAt(index int) uint64 {
	if index == model.floor {
		return model.floorHash
	}
	return model.commits[index-model.floor-1].hash
}

// stateAt returns a copy of state index, for floor <= index <= latest.
func (model *stateModel) stateAt(index int) dbState {
	state := make(dbState, len(model.state))
	for bucket, content := range model.state {
		state[bucket] = maps.Clone(content)
	}
	for commit := len(model.commits) - 1; commit >= index-model.floor; commit-- {
		undo := model.commits[commit].undo
		for change := len(undo) - 1; change >= 0; change-- {
			undoInto(state, undo[change])
		}
	}
	return state
}

// reset makes state index the latest state.
// It drops the commits after index.
func (model *stateModel) reset(index int) {
	if index == model.latest {
		return
	}
	model.state = model.stateAt(index)
	model.hash = model.hashAt(index)
	model.commits = model.commits[:index-model.floor]
	model.latest = index
	model.branches = slices.DeleteFunc(model.branches, func(branch branchRecord) bool { return branch.parent > index })
}

// resolve finds the state that recovery returned and makes it the latest state.
// A state before lowest, or a branch, is a valid result only after a crash.
// It returns a description of the result, whether the result is a branch, and an error when no allowed state matches.
func (model *stateModel) resolve(recovered dbState, lowest int, allowBranches bool) (string, bool, error) {
	if len(model.journal) != 0 {
		return "", false, fmt.Errorf("model has %d uncommitted changes during recovery", len(model.journal))
	}
	hash := stateHash(recovered)
	for index := model.latest; index >= lowest; index-- {
		if model.hashAt(index) != hash || !statesEqual(model.stateAt(index), recovered) {
			continue
		}
		lost := model.latest - index
		model.reset(index)
		if lost == 0 {
			return "latest state", false, nil
		}
		return fmt.Sprintf("state %d, without %d unsynced commits", index, lost), false, nil
	}
	if allowBranches {
		for _, branch := range model.branches {
			if branch.parent < lowest || branch.hash != hash {
				continue
			}
			expected := model.stateAt(branch.parent)
			for _, mutation := range branch.mutations {
				applyInto(expected, mutation)
			}
			if !statesEqual(expected, recovered) {
				continue
			}
			model.reset(branch.parent)
			mark := model.mark()
			for _, mutation := range branch.mutations {
				model.apply(mutation)
			}
			model.commit(mark)
			model.branches = slices.DeleteFunc(model.branches, func(other branchRecord) bool { return other.parent >= branch.parent })
			return fmt.Sprintf("state %d plus the commit whose result was unknown", branch.parent), true, nil
		}
	}
	return "", false, fmt.Errorf("recovered state matches no allowed state %d..%d (branches allowed: %t, %d branches)\n%s",
		lowest, model.latest, allowBranches, len(model.branches), stateDiff(model.state, recovered))
}

// applyInto applies a mutation to a state without journal records.
func applyInto(state dbState, mutation txMutation) {
	switch mutation.kind {
	case mutationPut:
		value := slices.Clone(mutation.value)
		if value == nil {
			value = []byte{}
		}
		state[mutation.bucket][mutation.key] = value
	case mutationDelete:
		delete(state[mutation.bucket], mutation.key)
	case mutationCreateBucket:
		state[mutation.bucket] = map[string][]byte{}
	case mutationDeleteBucket:
		delete(state, mutation.bucket)
	}
}

func statesEqual(left, right dbState) bool {
	if len(left) != len(right) {
		return false
	}
	for bucket, leftContent := range left {
		rightContent, ok := right[bucket]
		if !ok || len(leftContent) != len(rightContent) {
			return false
		}
		for key, value := range leftContent {
			other, ok := rightContent[key]
			if !ok || !bytes.Equal(value, other) {
				return false
			}
		}
	}
	return true
}

// stateDiff describes up to ten differences between the expected and the actual state.
func stateDiff(want, got dbState) string {
	var lines []string
	add := func(format string, args ...any) {
		if len(lines) < 10 {
			lines = append(lines, fmt.Sprintf(format, args...))
		}
	}
	for _, bucket := range slices.Sorted(maps.Keys(want)) {
		gotContent, ok := got[bucket]
		if !ok {
			add("  bucket %q: missing (want %d keys)", bucket, len(want[bucket]))
			continue
		}
		for _, key := range slices.Sorted(maps.Keys(want[bucket])) {
			value, ok := gotContent[key]
			switch {
			case !ok:
				add("  bucket %q key %x: missing", bucket, shortKey(key))
			case !bytes.Equal(value, want[bucket][key]):
				add("  bucket %q key %x: %d-byte value differs from the %d-byte expected value", bucket, shortKey(key), len(value), len(want[bucket][key]))
			}
		}
		for _, key := range slices.Sorted(maps.Keys(gotContent)) {
			if _, ok := want[bucket][key]; !ok {
				add("  bucket %q key %x: unexpected", bucket, shortKey(key))
			}
		}
	}
	for _, bucket := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[bucket]; !ok {
			add("  bucket %q: unexpected (%d keys)", bucket, len(got[bucket]))
		}
	}
	if len(lines) == 0 {
		return "  (no difference from the latest state)"
	}
	return "  difference from the latest state:\n" + joinLines(lines)
}

func shortKey(key string) string {
	if len(key) > 12 {
		return key[:12]
	}
	return key
}

func joinLines(lines []string) string {
	var buffer bytes.Buffer
	for _, line := range lines {
		buffer.WriteString(line)
		buffer.WriteByte('\n')
	}
	return buffer.String()
}
