package blackout

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/Issaminu/kvlite/internal/checksum"
)

// dbState is the reference content of a database.
// It maps each bucket path to the entries of that bucket.
// A path joins bucket names with "/", for example "bucket-0/n1"; generated bucket names never contain "/".
// A nil value names a nested bucket, whose own path holds its entries.
// A stored empty value is a non-nil empty slice.
type dbState map[string]map[string][]byte

// parentPath returns the path of the bucket that holds the bucket at path, and the bucket's name.
// A top-level bucket has the parent "".
func parentPath(path string) (string, string) {
	index := strings.LastIndexByte(path, '/')
	if index < 0 {
		return "", path
	}
	return path[:index], path[index+1:]
}

// pathDepth returns the number of bucket names in path.
func pathDepth(path string) int {
	return strings.Count(path, "/") + 1
}

type mutationKind uint8

const (
	mutationPut mutationKind = iota
	mutationDelete
	mutationCreateBucket
	mutationDeleteBucket
	mutationGet
	mutationDeleteBatch
	mutationScan
	mutationChecksums // SetDataPageChecksums; the workload never picks it as a change inside a callback
	mutationKindCount
)

var mutationNames = [mutationKindCount]string{"put", "delete", "create-bucket", "delete-bucket", "get", "delete-batch", "scan", "checksums"}

// metaPath is the path of a pseudo-bucket for database settings that commits change.
// The settings are model state, so the floor and branch rules cover them like data.
// Generated bucket names never start with a zero byte.
const metaPath = "\x00meta"

// checksumsKey is the entry of metaPath that holds the data-page checksum mode.
const checksumsKey = "data-page-checksums"

// checksumsValue returns the metaPath value of a checksum mode.
func checksumsValue(enabled bool) []byte {
	if enabled {
		return []byte{1}
	}
	return []byte{0}
}

// txMutation is one call inside a write transaction.
// A get or a scan changes nothing.
type txMutation struct {
	kind  mutationKind
	path  string // bucket path; for create-bucket and delete-bucket, the path of that bucket
	key   string
	value []byte
	keys  []string // keys of a delete-batch
}

func (mutation txMutation) String() string {
	return fmt.Sprintf("%s(%q, %d-byte key, %d-byte value, %d keys)", mutationNames[mutation.kind], mutation.path, len(mutation.key), len(mutation.value), len(mutation.keys))
}

// undoRecord reverts one change of the model.
// It saves every entry and bucket that the change replaced.
type undoRecord struct {
	path    string
	key     string
	existed bool                         // the key existed before the change
	value   []byte                       // previous value of the key; nil for a nested bucket
	buckets map[string]map[string][]byte // previous content of each replaced bucket path
	created string                       // path of a bucket that the change created
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
	// past holds the hash of every state from 0 to latest, so a failure report can name an older state that recovery returned.
	past []uint64
}

// newStateModel returns a model whose state 0 is a new database with the given checksum mode.
func newStateModel(checksums bool) *stateModel {
	state := dbState{metaPath: {checksumsKey: checksumsValue(checksums)}}
	hash := stateHash(state)
	return &stateModel{state: state, hash: hash, floorHash: hash, past: []uint64{hash}}
}

// bucketPaths returns the path of every bucket in key order.
func (model *stateModel) bucketPaths() []string {
	paths := make([]string, 0, len(model.state))
	for path := range model.state {
		if path != metaPath {
			paths = append(paths, path)
		}
	}
	slices.Sort(paths)
	return paths
}

// checksums reports the data-page checksum mode of the latest state.
func (model *stateModel) checksums() bool {
	return bytes.Equal(model.state[metaPath][checksumsKey], checksumsValue(true))
}

const (
	hashValue  byte = iota // a plain entry
	hashNested             // an entry that names a nested bucket
	hashBucket             // the existence of a bucket path
)

// entryHash returns a 64-bit hash of one entry, or of one bucket path.
// The model combines entry hashes with XOR, so the order of changes does not matter.
// It uses CRC32C, like the rest of KVLite, and then a splitmix64 finish.
// The finish is required: CRC is linear, so XOR-combined raw CRCs give the same hash to two states that swap equal-length values between equal-length keys.
// Length prefixes keep path "a" with key "bc" apart from path "ab" with key "c".
func entryHash(path, key string, value []byte, kind byte) uint64 {
	var lengths [12]byte
	binary.LittleEndian.PutUint32(lengths[0:4], uint32(len(path)))
	binary.LittleEndian.PutUint32(lengths[4:8], uint32(len(key)))
	binary.LittleEndian.PutUint32(lengths[8:12], uint32(len(value)))
	hash := uint64(checksum.Sum32([]byte{kind}, lengths[:], []byte(path), []byte(key), value))
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	hash *= 0x94d049bb133111eb
	return hash ^ hash>>31
}

// valueHash returns the hash of one entry of the bucket at path.
func valueHash(path, key string, value []byte) uint64 {
	if value == nil {
		return entryHash(path, key, nil, hashNested)
	}
	return entryHash(path, key, value, hashValue)
}

// bucketHash returns the hash of the bucket at path and all of its entries.
func bucketHash(path string, content map[string][]byte) uint64 {
	hash := entryHash(path, "", nil, hashBucket)
	for key, value := range content {
		hash ^= valueHash(path, key, value)
	}
	return hash
}

func stateHash(state dbState) uint64 {
	var hash uint64
	for path, content := range state {
		hash ^= bucketHash(path, content)
	}
	return hash
}

// subtree returns path and every bucket path below it, in sorted order.
func (state dbState) subtree(path string) []string {
	paths := []string{path}
	prefix := path + "/"
	for other := range state {
		if strings.HasPrefix(other, prefix) {
			paths = append(paths, other)
		}
	}
	slices.Sort(paths)
	return paths
}

// applyChange applies mutation to state.
// It returns the undo record, the hash change, and whether the state changed.
// The caller must first check every precondition that KVLite checks: an applied mutation always succeeds.
func applyChange(state dbState, mutation txMutation) (undoRecord, uint64, bool) {
	content := state[mutation.path]
	switch mutation.kind {
	case mutationPut:
		old, existed := content[mutation.key]
		undo := undoRecord{path: mutation.path, key: mutation.key, existed: existed, value: old}
		var hash uint64
		if existed {
			hash ^= valueHash(mutation.path, mutation.key, old)
		}
		value := slices.Clone(mutation.value)
		if value == nil {
			value = []byte{}
		}
		content[mutation.key] = value
		hash ^= valueHash(mutation.path, mutation.key, value)
		return undo, hash, true
	case mutationDelete:
		old, existed := content[mutation.key]
		if !existed {
			return undoRecord{}, 0, false
		}
		delete(content, mutation.key)
		return undoRecord{path: mutation.path, key: mutation.key, existed: true, value: old}, valueHash(mutation.path, mutation.key, old), true
	case mutationDeleteBatch:
		// A batch is several deletes. Its undo record saves the bucket.
		var hash uint64
		saved := maps.Clone(content)
		changed := false
		for _, key := range mutation.keys {
			if old, existed := content[key]; existed {
				hash ^= valueHash(mutation.path, key, old)
				delete(content, key)
				changed = true
			}
		}
		if !changed {
			return undoRecord{}, 0, false
		}
		return undoRecord{buckets: map[string]map[string][]byte{mutation.path: saved}}, hash, true
	case mutationChecksums:
		old := content[mutation.key]
		if bytes.Equal(old, mutation.value) {
			return undoRecord{}, 0, false
		}
		content[mutation.key] = mutation.value
		undo := undoRecord{path: mutation.path, key: mutation.key, existed: true, value: old}
		return undo, valueHash(mutation.path, mutation.key, old) ^ valueHash(mutation.path, mutation.key, mutation.value), true
	case mutationCreateBucket:
		parent, name := parentPath(mutation.path)
		state[mutation.path] = map[string][]byte{}
		hash := entryHash(mutation.path, "", nil, hashBucket)
		if parent != "" {
			state[parent][name] = nil
			hash ^= valueHash(parent, name, nil)
		}
		return undoRecord{created: mutation.path}, hash, true
	case mutationDeleteBucket:
		parent, name := parentPath(mutation.path)
		removed := map[string]map[string][]byte{}
		var hash uint64
		for _, path := range state.subtree(mutation.path) {
			removed[path] = state[path]
			hash ^= bucketHash(path, state[path])
			delete(state, path)
		}
		undo := undoRecord{buckets: removed}
		if parent != "" {
			undo.path, undo.key, undo.existed = parent, name, true
			delete(state[parent], name)
			hash ^= valueHash(parent, name, nil)
		}
		return undo, hash, true
	}
	return undoRecord{}, 0, false
}

// undoChange reverts one change in state and returns the hash change.
func undoChange(state dbState, undo undoRecord) uint64 {
	var hash uint64
	if undo.created != "" {
		parent, name := parentPath(undo.created)
		hash ^= bucketHash(undo.created, state[undo.created])
		delete(state, undo.created)
		if parent != "" {
			delete(state[parent], name)
			hash ^= valueHash(parent, name, nil)
		}
		return hash
	}
	if undo.buckets != nil {
		for path, content := range undo.buckets {
			if current, ok := state[path]; ok {
				hash ^= bucketHash(path, current)
			}
			restored := maps.Clone(content)
			state[path] = restored
			hash ^= bucketHash(path, restored)
		}
		if undo.existed {
			// The parent entry of a deleted nested bucket.
			state[undo.path][undo.key] = nil
			hash ^= valueHash(undo.path, undo.key, nil)
		}
		return hash
	}
	content := state[undo.path]
	if current, ok := content[undo.key]; ok {
		hash ^= valueHash(undo.path, undo.key, current)
	}
	if undo.existed {
		content[undo.key] = undo.value
		hash ^= valueHash(undo.path, undo.key, undo.value)
	} else {
		delete(content, undo.key)
	}
	return hash
}

// apply changes the model and records the undo record in the journal.
// It reports whether the mutation changed the state.
func (model *stateModel) apply(mutation txMutation) bool {
	undo, hash, changed := applyChange(model.state, mutation)
	if !changed {
		return false
	}
	model.journal = append(model.journal, undo)
	model.hash ^= hash
	return true
}

// mark returns the journal position for a later commit or rollback.
func (model *stateModel) mark() int {
	return len(model.journal)
}

// rollback reverts the journal back to mark.
func (model *stateModel) rollback(mark int) {
	for index := len(model.journal) - 1; index >= mark; index-- {
		model.hash ^= undoChange(model.state, model.journal[index])
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
	model.past = append(model.past, model.hash)
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
// A branch whose parent is older than the new floor can no longer appear: a later durable commit overwrote its WAL generation.
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

// cloneState returns a deep copy of state.
// Values are never changed in place, so the copy shares them.
func cloneState(state dbState) dbState {
	copied := make(dbState, len(state))
	for path, content := range state {
		copied[path] = maps.Clone(content)
	}
	return copied
}

// stateAt returns a copy of state index, for floor <= index <= latest.
func (model *stateModel) stateAt(index int) dbState {
	state := cloneState(model.state)
	for commit := len(model.commits) - 1; commit >= index-model.floor; commit-- {
		undo := model.commits[commit].undo
		for _, change := range slices.Backward(undo) {
			undoChange(state, change)
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
	model.past = model.past[:index+1]
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
	index, branch, found := model.find(recovered, lowest, allowBranches)
	switch {
	case found && branch == nil:
		lost := model.latest - index
		model.reset(index)
		if lost == 0 {
			return "latest state", false, nil
		}
		return fmt.Sprintf("state %d, without %d unsynced commits", index, lost), false, nil
	case found:
		parent, mutations := branch.parent, branch.mutations
		model.reset(parent)
		mark := model.mark()
		for _, mutation := range mutations {
			model.apply(mutation)
		}
		model.commit(mark)
		model.branches = slices.DeleteFunc(model.branches, func(other branchRecord) bool { return other.parent >= parent })
		return fmt.Sprintf("state %d plus the commit whose result was unknown", parent), true, nil
	}
	var report strings.Builder
	fmt.Fprintf(&report, "recovered state matches no allowed state %d..%d (branches allowed: %t, %d branches)",
		lowest, model.latest, allowBranches, len(model.branches))
	for index, pastHash := range slices.Backward(model.past) {
		if pastHash == hash {
			// Only the hash matches, because the model keeps no content before the floor.
			fmt.Fprintf(&report, "\nthe recovered state has the hash of older state %d, so recovery possibly lost %d acknowledged commits", index, lowest-index)
			break
		}
	}
	report.WriteString("\ndifference from the latest state:\n")
	report.WriteString(stateDiff(model.state, recovered))
	for _, branch := range model.branches {
		// The difference from each branch helps to find a commit that recovery kept only in part.
		expected := model.stateAt(branch.parent)
		for _, mutation := range branch.mutations {
			applyChange(expected, mutation)
		}
		fmt.Fprintf(&report, "\ndifference from state %d plus the %d changes of the commit whose result was unknown:\n%s", branch.parent, len(branch.mutations), stateDiff(expected, recovered))
		for index, mutation := range branch.mutations[:min(len(branch.mutations), 10)] {
			fmt.Fprintf(&report, "\n  change %d: %s bucket %q key %s, %d-byte value", index, mutationNames[mutation.kind], mutation.path, shortKey(mutation.key), len(mutation.value))
		}
	}
	return "", false, errors.New(report.String())
}

// find returns the state that equals recovered: the index of an acknowledged state from lowest to latest, or a branch.
// It does not change the model.
func (model *stateModel) find(recovered dbState, lowest int, allowBranches bool) (int, *branchRecord, bool) {
	hash := stateHash(recovered)
	for index := model.latest; index >= lowest; index-- {
		if model.hashAt(index) == hash && statesEqual(model.stateAt(index), recovered) {
			return index, nil, true
		}
	}
	if !allowBranches {
		return 0, nil, false
	}
	for index := range model.branches {
		branch := &model.branches[index]
		if branch.parent < lowest || branch.hash != hash {
			continue
		}
		expected := model.stateAt(branch.parent)
		for _, mutation := range branch.mutations {
			applyChange(expected, mutation)
		}
		if statesEqual(expected, recovered) {
			return 0, branch, true
		}
	}
	return 0, nil, false
}

func statesEqual(left, right dbState) bool {
	if len(left) != len(right) {
		return false
	}
	for path, leftContent := range left {
		rightContent, ok := right[path]
		if !ok || len(leftContent) != len(rightContent) {
			return false
		}
		for key, value := range leftContent {
			other, ok := rightContent[key]
			if !ok || (value == nil) != (other == nil) || !bytes.Equal(value, other) {
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
	if wantMode, gotMode := want[metaPath][checksumsKey], got[metaPath][checksumsKey]; !bytes.Equal(wantMode, gotMode) {
		add("  data-page checksum mode: got %v, want %v", gotMode, wantMode)
	}
	for _, path := range slices.Sorted(maps.Keys(want)) {
		if path == metaPath {
			continue
		}
		gotContent, ok := got[path]
		if !ok {
			add("  bucket %q: missing (want %d entries)", path, len(want[path]))
			continue
		}
		for _, key := range slices.Sorted(maps.Keys(want[path])) {
			value, ok := gotContent[key]
			switch {
			case !ok:
				add("  bucket %q key %s: missing", path, shortKey(key))
			case (value == nil) != (want[path][key] == nil):
				add("  bucket %q key %s: nested bucket and plain value differ", path, shortKey(key))
			case !bytes.Equal(value, want[path][key]):
				add("  bucket %q key %s: %d-byte value differs from the %d-byte expected value", path, shortKey(key), len(value), len(want[path][key]))
			}
		}
		for _, key := range slices.Sorted(maps.Keys(gotContent)) {
			if _, ok := want[path][key]; !ok {
				add("  bucket %q key %s: unexpected", path, shortKey(key))
			}
		}
	}
	for _, path := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[path]; !ok {
			add("  bucket %q: unexpected (%d entries)", path, len(got[path]))
		}
	}
	if len(lines) == 0 {
		return "  (no difference)"
	}
	return strings.Join(lines, "\n")
}

// shortKey returns key in hexadecimal for a report.
// A long key shows its first and last 8 bytes and its length, so keys with a long shared prefix stay distinct.
func shortKey(key string) string {
	if len(key) > 24 {
		return fmt.Sprintf("%x..%x (%d bytes)", key[:8], key[len(key)-8:], len(key))
	}
	return fmt.Sprintf("%x", key)
}
