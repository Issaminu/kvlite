package blackout

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strings"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// errAbort is the error that a callback returns on purpose to abort its transaction.
var errAbort = errors.New("blackout callback aborts its transaction")

// callbackEnding is how a write callback ends after its changes.
type callbackEnding uint8

const (
	endReturn callbackEnding = iota // return nil, or errAbort when the transaction aborts
	endPanic                        // panic with a unique value
	endGoexit                       // call runtime.Goexit
)

// panicValue is the value that a callback panics with. Each panic has its own step number.
type panicValue struct{ step int }

// openBucketPath opens the bucket at path, one level at a time, and checks each level against the model.
// It returns the bucket, or nil when a level fails.
// For a failure that the model expects, err is nil and the caller skips the change; otherwise err is the error that the callback returns.
func (runner *runner) openBucketPath(tx *kvlite.Tx, path string) (*kvlite.Bucket, error) {
	names := strings.Split(path, "/")
	var bucket *kvlite.Bucket
	var err error
	current := ""
	for depth, name := range names {
		var want error
		if depth == 0 {
			if _, ok := runner.model.state[name]; !ok {
				want = kvlite.ErrBucketNotFound
			}
			bucket, err = tx.Bucket([]byte(name))
			current = name
		} else {
			value, ok := runner.model.state[current][name]
			switch {
			case !ok:
				want = kvlite.ErrBucketNotFound
			case value != nil:
				want = kvlite.ErrIncompatibleValue
			}
			bucket, err = bucket.Bucket([]byte(name))
			current += "/" + name
		}
		if want != nil {
			return nil, runner.expectError("open bucket "+current, err, want)
		}
		if err := runner.checkCallError("open bucket "+current, err); err != nil {
			return nil, err
		}
	}
	return bucket, nil
}

// keyError returns the error that the model expects for a plain-key call on key in the bucket at path.
func (runner *runner) keyError(path, key string) error {
	if value, ok := runner.model.state[path][key]; ok && value == nil {
		return kvlite.ErrIncompatibleValue
	}
	return nil
}

// runMutation runs one change in a write callback.
// It applies the change to the model only after KVLite accepts it.
func (runner *runner) runMutation(tx *kvlite.Tx, mutation txMutation, applied *[]txMutation) error {
	runner.stats.Mutations[mutation.kind]++
	switch mutation.kind {
	case mutationCreateBucket, mutationDeleteBucket:
		return runner.runBucketChange(tx, mutation, applied)
	}
	bucket, err := runner.openBucketPath(tx, mutation.path)
	if bucket == nil {
		return err
	}
	key := []byte(mutation.key)
	switch mutation.kind {
	case mutationPut:
		// Put must copy its key and value: overwrite both buffers after the call.
		value := slices.Clone(mutation.value)
		err = bucket.Put(key, value)
		clear(key)
		clear(value)
		if want := runner.keyError(mutation.path, mutation.key); want != nil {
			return runner.expectError("Put", err, want)
		}
		err = runner.checkCallError("Put", err)
	case mutationDelete:
		err = bucket.Delete(key)
		if want := runner.keyError(mutation.path, mutation.key); want != nil {
			return runner.expectError("Delete", err, want)
		}
		err = runner.checkCallError("Delete", err)
	case mutationDeleteBatch:
		keys := make([][]byte, len(mutation.keys))
		var want error
		for index, key := range mutation.keys {
			keys[index] = []byte(key)
			if want == nil {
				want = runner.keyError(mutation.path, key)
			}
		}
		err = bucket.DeleteBatch(keys)
		if want != nil {
			// DeleteBatch can change some leaves before it fails, so the callback must return its error.
			if err := runner.expectError("DeleteBatch", err, want); err != nil {
				return err
			}
			return err
		}
		err = runner.checkCallError("DeleteBatch", err)
	case mutationGet:
		value, getErr := bucket.Get(key)
		if want := runner.keyError(mutation.path, mutation.key); want != nil {
			return runner.expectError("Bucket.Get in Update", getErr, want)
		}
		return runner.checkValue("Bucket.Get in Update", mutation.path, mutation.key, value, getErr)
	case mutationScan:
		return runner.scan(bucket, mutation.path, runner.model.state[mutation.path])
	}
	if err != nil {
		return err
	}
	if runner.model.apply(mutation) {
		*applied = append(*applied, mutation)
	}
	return nil
}

// runBucketChange creates or deletes the bucket at mutation.path.
func (runner *runner) runBucketChange(tx *kvlite.Tx, mutation txMutation, applied *[]txMutation) error {
	parent, name := parentPath(mutation.path)
	var want error
	var err error
	op := mutationNames[mutation.kind]
	if parent == "" {
		_, exists := runner.model.state[name]
		if mutation.kind == mutationCreateBucket {
			if exists {
				want = kvlite.ErrBucketExists
			}
			_, err = tx.CreateBucket([]byte(name))
		} else {
			if !exists {
				want = kvlite.ErrBucketNotFound
			}
			err = tx.DeleteBucket([]byte(name))
		}
	} else {
		bucket, openErr := runner.openBucketPath(tx, parent)
		if bucket == nil {
			return openErr
		}
		value, exists := runner.model.state[parent][name]
		switch {
		case exists && value != nil:
			want = kvlite.ErrIncompatibleValue
		case exists && mutation.kind == mutationCreateBucket:
			want = kvlite.ErrBucketExists
		case !exists && mutation.kind == mutationDeleteBucket:
			want = kvlite.ErrBucketNotFound
		}
		if mutation.kind == mutationCreateBucket {
			_, err = bucket.CreateBucket([]byte(name))
		} else {
			err = bucket.DeleteBucket([]byte(name))
		}
	}
	if want != nil {
		return runner.expectError(op+" "+mutation.path, err, want)
	}
	if err := runner.checkCallError(op+" "+mutation.path, err); err != nil {
		return err
	}
	if runner.model.apply(mutation) {
		*applied = append(*applied, mutation)
	}
	return nil
}

// runCallback runs the changes of one write callback and then ends it.
// A failed, aborted, panicking, or exiting callback reverts its model changes, because KVLite discards its changes too.
func (runner *runner) runCallback(tx *kvlite.Tx, mutations []txMutation, abort bool, ending callbackEnding, applied *[]txMutation) error {
	mark := runner.model.mark()
	var err error
	for _, mutation := range mutations {
		if err = runner.runMutation(tx, mutation, applied); err != nil {
			break
		}
	}
	if err == nil && abort {
		err = errAbort
	}
	if err != nil || ending != endReturn {
		runner.model.rollback(mark)
		*applied = nil
	}
	if err == nil {
		switch ending {
		case endPanic:
			panic(panicValue{step: runner.step})
		case endGoexit:
			runtime.Goexit()
		}
	}
	return err
}

// finishCommit applies the result of a commit to the model.
// err is the commit result of the callbacks that returned nil.
func (runner *runner) finishCommit(op string, mark int, err error, mutations []txMutation) {
	switch {
	case err == nil:
		if runner.model.commit(mark) {
			runner.stats.Commits++
			runner.mirrorCommit(mutations)
		}
		runner.event("%s committed %d changes: latest=%d", op, len(mutations), runner.model.latest)
	case errors.Is(err, kvlite.ErrWALWriteFailed):
		runner.stats.Uncertain++
		runner.stats.StoppedHandles++
		runner.stopped = true
		runner.uncertainOpen = true
		runner.model.uncertain(mark, mutations)
		runner.event("%s: WAL write failed, result unknown: %v", op, err)
	case runner.explained(err):
		// The failure happened before the WAL append, so nothing was written.
		runner.stats.Aborts++
		runner.model.rollback(mark)
		runner.event("%s: commit failed before the WAL append: %v", op, err)
	default:
		runner.model.rollback(mark)
		runner.fail("%s: unexpected commit error: %v", op, err)
	}
}

func (runner *runner) update() {
	mutations, abort := runner.work.newTransaction(runner.model)
	if len(mutations) == 1 && !abort && runner.work.rand.Bool() && runner.shortcut(mutations[0]) {
		return
	}
	mark := runner.model.mark()
	var applied []txMutation
	called := false
	var callbackErr error
	err := runner.db.Update(func(tx *kvlite.Tx) error {
		called = true
		if !tx.Writable() {
			runner.fail("Tx.Writable is false inside Update")
		}
		callbackErr = runner.runCallback(tx, mutations, abort, endReturn, &applied)
		return callbackErr
	})
	// In SyncFull, Update returns before the write worker finishes a checkpoint.
	// Wait for it, or the next step races with the checkpoint and the seed does not replay.
	runner.target.Quiesce(runner.db)
	switch {
	case !called:
		runner.fail("Update did not call its callback: %v", err)
	case callbackErr != nil:
		runner.stats.Aborts++
		if !errors.Is(err, callbackErr) {
			runner.fail("Update returned %v, want the callback error %v", err, callbackErr)
		}
		runner.event("update aborted: %v", callbackErr)
	default:
		runner.finishCommit("update", mark, err, applied)
	}
}

// shortcut runs one change on a top-level bucket through DB.Put, DB.Delete, or DB.DeleteBucket.
// It reports false when the change has no shortcut.
func (runner *runner) shortcut(mutation txMutation) bool {
	if strings.Contains(mutation.path, "/") {
		return false
	}
	_, exists := runner.model.state[mutation.path]
	var want error
	if !exists {
		want = kvlite.ErrBucketNotFound
	}
	var err error
	var op string
	switch mutation.kind {
	case mutationPut:
		op = "DB.Put"
		if want == nil {
			want = runner.keyError(mutation.path, mutation.key)
		}
		err = runner.db.Put([]byte(mutation.path), []byte(mutation.key), mutation.value)
	case mutationDelete:
		op = "DB.Delete"
		if want == nil {
			want = runner.keyError(mutation.path, mutation.key)
		}
		err = runner.db.Delete([]byte(mutation.path), []byte(mutation.key))
	case mutationDeleteBucket:
		op = "DB.DeleteBucket"
		err = runner.db.DeleteBucket([]byte(mutation.path))
	default:
		return false
	}
	runner.target.Quiesce(runner.db)
	if want != nil {
		_ = runner.expectError(op, err, want)
		return true
	}
	mark := runner.model.mark()
	var applied []txMutation
	if err == nil || errors.Is(err, kvlite.ErrWALWriteFailed) {
		if runner.model.apply(mutation) {
			applied = append(applied, mutation)
		}
	}
	runner.finishCommit(op, mark, err, applied)
	return true
}

// batch runs several callbacks as one SyncFull WAL commit, like concurrent Update calls that share a commit.
// It calls the batch executor directly, so the grouping does not depend on goroutine scheduling.
func (runner *runner) batch() {
	type callback struct {
		mutations []txMutation
		abort     bool
		ending    callbackEnding
		called    bool
		err       error
		applied   []txMutation
	}
	callbacks := make([]*callback, runner.work.rand.Range(2, 6))
	functions := make([]func(*kvlite.Tx) error, len(callbacks))
	for index := range callbacks {
		mutations, abort := runner.work.newTransaction(runner.model)
		item := &callback{mutations: mutations, abort: abort}
		switch {
		case runner.work.rand.Chance(disk.OneIn(40)):
			item.ending = endPanic
		case runner.work.rand.Chance(disk.OneIn(40)):
			item.ending = endGoexit
		}
		callbacks[index] = item
		functions[index] = func(tx *kvlite.Tx) error {
			item.called = true
			item.err = runner.runCallback(tx, item.mutations, item.abort, item.ending, &item.applied)
			return item.err
		}
	}
	mark := runner.model.mark()
	results := runner.target.Batch(runner.db, functions)

	var commitErr error
	var mutations []txMutation
	for index, item := range callbacks {
		result := results[index]
		if !item.called {
			runner.fail("batch callback %d did not run: %v", index, result.Err)
			return
		}
		if item.ending != endReturn && item.err == nil {
			if result.Panicked != (item.ending == endPanic) || result.Goexited != (item.ending == endGoexit) {
				runner.fail("batch callback %d: panicked=%t goexited=%t, want ending %d", index, result.Panicked, result.Goexited, item.ending)
				return
			}
			if result.Panicked && result.PanicValue != (panicValue{step: runner.step}) {
				runner.fail("batch callback %d panicked with %v, want its own value", index, result.PanicValue)
				return
			}
			continue
		}
		if result.Panicked || result.Goexited {
			runner.fail("batch callback %d: unexpected panic or Goexit: %v", index, result.PanicValue)
			return
		}
		if item.err != nil {
			continue
		}
		mutations = append(mutations, item.applied...)
		if result.Err != nil && commitErr == nil {
			commitErr = result.Err
		}
	}
	// A callback depends on the commit when it changed data, or when it ran after a change that it could read.
	// If the commit fails, each such callback must get an error, even one that failed by itself, because its result can depend on data that the commit lost.
	// A callback that depends on nothing keeps its own result. A panic or Goexit continues unchanged.
	pending := false
	for index, item := range callbacks {
		if item.ending != endReturn && item.err == nil {
			continue
		}
		result := results[index].Err
		depends := pending || item.err == nil && len(item.applied) > 0
		switch {
		case commitErr == nil && !errors.Is(result, item.err) && result != item.err:
			runner.fail("batch callback %d: got %v after a successful commit, want its own result %v", index, result, item.err)
		case commitErr != nil && depends && result == nil:
			runner.fail("batch callback %d depends on the failed commit but returned nil", index)
		case commitErr != nil && result != nil && !errors.Is(result, commitErr) && !errors.Is(result, item.err):
			runner.fail("batch callback %d returned %v, want its error %v or the commit error %v", index, result, item.err, commitErr)
		}
		if item.err == nil && len(item.applied) > 0 {
			pending = true
		}
	}
	if runner.failure != nil {
		runner.model.rollback(mark)
		return
	}
	runner.finishCommit(fmt.Sprintf("batch of %d", len(callbacks)), mark, commitErr, mutations)
}

func (runner *runner) get() {
	// DB.Get reads a top-level bucket.
	bucket := runner.work.bucketNames[runner.work.rand.IntN(len(runner.work.bucketNames))]
	key := runner.work.pickKey()
	value, err := runner.db.Get([]byte(bucket), []byte(key))
	if _, exists := runner.model.state[bucket]; !exists {
		_ = runner.expectError("DB.Get", err, kvlite.ErrBucketNotFound)
		return
	}
	if want := runner.keyError(bucket, key); want != nil {
		_ = runner.expectError("DB.Get", err, want)
		return
	}
	_ = runner.checkValue("DB.Get", bucket, key, value, err)
	// DB.Get returns a copy that the caller owns: changing it must not change the database.
	clear(value)
}

// view runs point reads and one scan in a read transaction.
func (runner *runner) view() {
	err := runner.db.View(func(tx *kvlite.Tx) error {
		if tx.Writable() {
			runner.fail("Tx.Writable is true inside View")
		}
		for range runner.work.rand.Range(0, 3) {
			path, key := runner.work.pickBucket(runner.model), runner.work.pickKey()
			bucket, err := runner.openBucketPath(tx, path)
			if bucket == nil {
				if err != nil {
					return err
				}
				continue
			}
			value, err := bucket.Get([]byte(key))
			if want := runner.keyError(path, key); want != nil {
				if err := runner.expectError("View Bucket.Get", err, want); err != nil {
					return err
				}
				continue
			}
			if err := runner.checkValue("View Bucket.Get", path, key, value, err); err != nil {
				return err
			}
		}
		path := runner.work.pickBucket(runner.model)
		bucket, err := runner.openBucketPath(tx, path)
		if bucket == nil {
			return err
		}
		return runner.scan(bucket, path, runner.model.state[path])
	})
	if err != nil && runner.failure == nil && !runner.explained(err) {
		runner.fail("View: unexpected error: %v", err)
	}
}

// scan runs one scan and compares its keys and values with the model.
// A nested bucket appears with a nil value.
func (runner *runner) scan(bucket *kvlite.Bucket, path string, content map[string][]byte) error {
	keys := slices.Sorted(maps.Keys(content))
	var got [][2][]byte
	collect := func(key, value []byte) error {
		var copied []byte
		if value != nil {
			copied = bytes.Clone(value)
		}
		got = append(got, [2][]byte{bytes.Clone(key), copied})
		return nil
	}
	kind, start, end := runner.work.newScan()
	runner.stats.Scans[kind]++
	want := keys
	op := scanNames[kind]
	var err error
	cursorWalk := func(first func(*kvlite.Cursor) ([]byte, []byte, error), next func(*kvlite.Cursor) ([]byte, []byte, error), limit int) {
		var cursor *kvlite.Cursor
		if cursor, err = bucket.Cursor(); err != nil {
			return
		}
		var key, value []byte
		count := 0
		for key, value, err = first(cursor); err == nil && key != nil && count < limit; key, value, err = next(cursor) {
			_ = collect(key, value)
			count++
		}
	}
	switch kind {
	case scanCursorForward:
		cursorWalk((*kvlite.Cursor).First, (*kvlite.Cursor).Next, len(keys)+1)
	case scanCursorBackward:
		cursorWalk((*kvlite.Cursor).Last, (*kvlite.Cursor).Prev, len(keys)+1)
		want = slices.Clone(keys)
		slices.Reverse(want)
	case scanReverse:
		err = bucket.ScanReverse(collect)
		want = slices.Clone(keys)
		slices.Reverse(want)
	case scanRange:
		op = fmt.Sprintf("ScanRange(%s, %s)", shortKey(start), shortKey(end))
		err = bucket.ScanRange([]byte(start), []byte(end), collect)
		want = nil
		for _, key := range keys {
			if key >= start && (end == "" || key < end) {
				want = append(want, key)
			}
		}
	case scanPrefix:
		op = fmt.Sprintf("ScanPrefix(%s)", shortKey(start))
		err = bucket.ScanPrefix([]byte(start), collect)
		want = nil
		for _, key := range keys {
			if strings.HasPrefix(key, start) {
				want = append(want, key)
			}
		}
	case scanSeek:
		op = fmt.Sprintf("Seek(%s) and Next", shortKey(start))
		cursorWalk(func(cursor *kvlite.Cursor) ([]byte, []byte, error) { return cursor.Seek([]byte(start)) }, (*kvlite.Cursor).Next, 8)
		index, _ := slices.BinarySearch(keys, start)
		want = keys[index:min(len(keys), index+8)]
	}
	if err != nil {
		return runner.checkCallError(op, err)
	}
	if len(got) != len(want) {
		runner.fail("%s in bucket %q: got %d entries, want %d", op, path, len(got), len(want))
		return errCheckFailed
	}
	for index, entry := range got {
		wantValue := content[want[index]]
		if string(entry[0]) != want[index] || (entry[1] == nil) != (wantValue == nil) || !bytes.Equal(entry[1], wantValue) {
			runner.fail("%s in bucket %q: entry %d is key %s with %d bytes, want key %s with %d bytes", op, path, index, shortKey(string(entry[0])), len(entry[1]), shortKey(want[index]), len(wantValue))
			return errCheckFailed
		}
	}
	return nil
}

func (runner *runner) checkpoint() {
	err := runner.target.Checkpoint(runner.db)
	runner.event("checkpoint: %v", err)
	_ = runner.checkCallError("checkpoint", err)
}

// toggleChecksums turns data-page checksums on or off.
// The mode is model state: SetDataPageChecksums commits it through the WAL like data.
func (runner *runner) toggleChecksums() {
	current := runner.target.DataPageChecksums(runner.db)
	if current != runner.model.checksums() {
		runner.fail("data-page checksum mode is %t, want %t", current, runner.model.checksums())
		return
	}
	enabled := !current
	mark := runner.model.mark()
	err := runner.db.SetDataPageChecksums(enabled)
	runner.event("SetDataPageChecksums(%t): %v", enabled, err)
	// SetDataPageChecksums checkpoints before it commits the new mode.
	// So in SyncNormal, a checkpoint inside the call makes only the earlier state durable.
	runner.updateFloor()
	var applied []txMutation
	if err == nil || errors.Is(err, kvlite.ErrWALWriteFailed) {
		mutation := txMutation{kind: mutationChecksums, path: metaPath, key: checksumsKey, value: checksumsValue(enabled)}
		if runner.model.apply(mutation) {
			applied = append(applied, mutation)
		}
	}
	runner.finishCommit("SetDataPageChecksums", mark, err, applied)
}

// reopen closes the handle cleanly and opens the file again.
// The reopened state must be exactly the latest state.
func (runner *runner) reopen() {
	runner.event("reopen")
	db := runner.db
	if !runner.closeDB() {
		if runner.failure == nil {
			runner.recoverCrash()
		}
		return
	}
	runner.checkClosed(db)
	runner.corruptAtRest()
	runner.openAndVerify(false)
}

// checkClosed checks the promises of a closed handle.
func (runner *runner) checkClosed(db *kvlite.DB) {
	if _, err := db.Get([]byte(runner.work.bucketNames[0]), []byte("key")); !errors.Is(err, kvlite.ErrDatabaseNotOpen) {
		runner.fail("Get after Close returned %v, want ErrDatabaseNotOpen", err)
	}
	if err := db.Update(func(*kvlite.Tx) error { return nil }); !errors.Is(err, kvlite.ErrDatabaseNotOpen) {
		runner.fail("Update after Close returned %v, want ErrDatabaseNotOpen", err)
	}
	if err := db.View(func(*kvlite.Tx) error { return nil }); !errors.Is(err, kvlite.ErrDatabaseNotOpen) {
		runner.fail("View after Close returned %v, want ErrDatabaseNotOpen", err)
	}
	if err := db.Close(); !errors.Is(err, kvlite.ErrDatabaseNotOpen) {
		runner.fail("a second Close returned %v, want ErrDatabaseNotOpen", err)
	}
	if db.Path() != runner.openPath {
		runner.fail("Path after Close returned %q, want %q", db.Path(), runner.openPath)
	}
}
