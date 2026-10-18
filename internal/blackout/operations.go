package blackout

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/Issaminu/kvlite"
)

// errAbort is the error that a callback returns on purpose to abort its transaction.
var errAbort = errors.New("blackout callback aborts its transaction")

// runMutation runs one change in a write callback.
// It applies the change to the model only after KVLite accepts it.
func (runner *runner) runMutation(tx *kvlite.Tx, mutation txMutation, applied *[]txMutation) error {
	name := []byte(mutation.bucket)
	_, exists := runner.model.state[mutation.bucket]
	switch mutation.kind {
	case mutationCreateBucket:
		_, err := tx.CreateBucket(name)
		if exists {
			return runner.expectError("CreateBucket", err, kvlite.ErrBucketExists)
		}
		if err := runner.checkCallError("CreateBucket", err); err != nil {
			return err
		}
	case mutationDeleteBucket:
		err := tx.DeleteBucket(name)
		if !exists {
			return runner.expectError("DeleteBucket", err, kvlite.ErrBucketNotFound)
		}
		if err := runner.checkCallError("DeleteBucket", err); err != nil {
			return err
		}
	default:
		bucket, err := tx.Bucket(name)
		if !exists {
			return runner.expectError("Bucket", err, kvlite.ErrBucketNotFound)
		}
		if err := runner.checkCallError("Bucket", err); err != nil {
			return err
		}
		key := []byte(mutation.key)
		switch mutation.kind {
		case mutationPut:
			err = runner.checkCallError("Put", bucket.Put(key, mutation.value))
		case mutationDelete:
			err = runner.checkCallError("Delete", bucket.Delete(key))
		case mutationGet:
			value, getErr := bucket.Get(key)
			return runner.checkValue("Bucket.Get in Update", mutation.bucket, mutation.key, value, getErr)
		}
		if err != nil {
			return err
		}
	}
	if runner.model.apply(mutation) {
		*applied = append(*applied, mutation)
	}
	return nil
}

// runCallback runs the changes of one write callback.
// A failed callback reverts its model changes, because KVLite discards its changes too.
func (runner *runner) runCallback(tx *kvlite.Tx, mutations []txMutation, abort bool, applied *[]txMutation) error {
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
	if err != nil {
		runner.model.rollback(mark)
		*applied = nil
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
		}
		runner.event("%s committed %d changes: latest=%d", op, len(mutations), runner.model.latest)
	case errors.Is(err, kvlite.ErrWALWriteFailed):
		runner.stats.Uncertain++
		runner.stats.StoppedHandles++
		runner.stopped = true
		runner.uncertainOpen = true
		runner.model.uncertain(mark, mutations)
		runner.event("%s: WAL write failed, result unknown: %v", op, err)
	case explained(err):
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
	mark := runner.model.mark()
	var applied []txMutation
	called := false
	var callbackErr error
	err := runner.db.Update(func(tx *kvlite.Tx) error {
		called = true
		callbackErr = runner.runCallback(tx, mutations, abort, &applied)
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

func (runner *runner) get() {
	bucket, key := runner.work.pickBucket(runner.model), runner.work.pickKey()
	value, err := runner.db.Get([]byte(bucket), []byte(key))
	if _, exists := runner.model.state[bucket]; !exists {
		_ = runner.expectError("Get", err, kvlite.ErrBucketNotFound)
		return
	}
	_ = runner.checkValue("DB.Get", bucket, key, value, err)
}

// view runs point reads and one scan in a read transaction.
func (runner *runner) view() {
	err := runner.db.View(func(tx *kvlite.Tx) error {
		for range runner.work.rand.Range(0, 3) {
			bucketName, key := runner.work.pickBucket(runner.model), runner.work.pickKey()
			bucket, err := tx.Bucket([]byte(bucketName))
			if _, exists := runner.model.state[bucketName]; !exists {
				if err := runner.expectError("View Bucket", err, kvlite.ErrBucketNotFound); err != nil {
					return err
				}
				continue
			}
			if err := runner.checkCallError("View Bucket", err); err != nil {
				return err
			}
			value, err := bucket.Get([]byte(key))
			if err := runner.checkValue("View Bucket.Get", bucketName, key, value, err); err != nil {
				return err
			}
		}
		bucketName := runner.work.pickBucket(runner.model)
		content, exists := runner.model.state[bucketName]
		bucket, err := tx.Bucket([]byte(bucketName))
		if !exists {
			return runner.expectError("View Bucket", err, kvlite.ErrBucketNotFound)
		}
		if err := runner.checkCallError("View Bucket", err); err != nil {
			return err
		}
		return runner.scan(bucket, bucketName, content)
	})
	if err != nil && runner.failure == nil && !explained(err) {
		runner.fail("View: unexpected error: %v", err)
	}
}

// scan runs one scan and compares its keys and values with the model.
func (runner *runner) scan(bucket *kvlite.Bucket, name string, content map[string][]byte) error {
	keys := slices.Sorted(maps.Keys(content))
	var got [][2][]byte
	collect := func(key, value []byte) error {
		got = append(got, [2][]byte{bytes.Clone(key), bytes.Clone(value)})
		return nil
	}
	kind, start, end := runner.work.newScan()
	want := keys
	var op string
	var err error
	switch kind {
	case scanCursorForward:
		op = "cursor forward"
		var cursor *kvlite.Cursor
		if cursor, err = bucket.Cursor(); err == nil {
			var key, value []byte
			for key, value, err = cursor.First(); err == nil && key != nil; key, value, err = cursor.Next() {
				_ = collect(key, value)
			}
		}
	case scanReverse:
		op = "ScanReverse"
		err = bucket.ScanReverse(collect)
		want = slices.Clone(keys)
		slices.Reverse(want)
	case scanRange:
		op = fmt.Sprintf("ScanRange(%x, %x)", shortKey(start), shortKey(end))
		err = bucket.ScanRange([]byte(start), []byte(end), collect)
		want = nil
		for _, key := range keys {
			if key >= start && key < end {
				want = append(want, key)
			}
		}
	case scanSeek:
		op = fmt.Sprintf("Seek(%x) and Next", shortKey(start))
		var cursor *kvlite.Cursor
		if cursor, err = bucket.Cursor(); err == nil {
			var key, value []byte
			count := 0
			for key, value, err = cursor.Seek([]byte(start)); err == nil && key != nil && count < 8; key, value, err = cursor.Next() {
				_ = collect(key, value)
				count++
			}
		}
		index, _ := slices.BinarySearch(keys, start)
		want = keys[index:min(len(keys), index+8)]
	}
	if err != nil {
		return runner.checkCallError(op, err)
	}
	if len(got) != len(want) {
		runner.fail("%s in bucket %q: got %d entries, want %d", op, name, len(got), len(want))
		return errCheckFailed
	}
	for index, entry := range got {
		if string(entry[0]) != want[index] || !bytes.Equal(entry[1], content[want[index]]) || entry[1] == nil {
			runner.fail("%s in bucket %q: entry %d is key %x with %d bytes, want key %x with %d bytes", op, name, index, shortKey(string(entry[0])), len(entry[1]), shortKey(want[index]), len(content[want[index]]))
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

// reopen closes the handle cleanly and opens the file again.
// The reopened state must be exactly the latest state.
func (runner *runner) reopen() {
	runner.event("reopen")
	if !runner.closeDB() {
		if runner.failure == nil {
			runner.recoverCrash()
		}
		return
	}
	runner.openAndVerify(false)
}
