package blackout

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// errCheckFailed is the error a callback returns after a failed check, so that its transaction ends.
var errCheckFailed = errors.New("blackout check failed")

// explained reports whether a simulated crash explains err.
// The error chain must contain the cause; "some error after some fault" is not enough.
func explained(err error) bool {
	return errors.Is(err, disk.ErrCrashed)
}

// expectError checks a call that the model expects to fail with want.
// An error that a fault explains ends the transaction.
func (runner *runner) expectError(op string, err, want error) error {
	if errors.Is(err, want) {
		return nil
	}
	if explained(err) {
		return err
	}
	runner.fail("%s: got error %v, want %v", op, err, want)
	return errCheckFailed
}

// checkCallError accepts nil, or an error that a fault explains.
func (runner *runner) checkCallError(op string, err error) error {
	if err == nil || explained(err) {
		return err
	}
	runner.fail("%s: unexpected error: %v", op, err)
	return errCheckFailed
}

// checkValue compares a point read with the model.
// A stored empty value must be a non-nil empty slice.
func (runner *runner) checkValue(op, bucket, key string, value []byte, err error) error {
	want, found := runner.model.state[bucket][key]
	if err != nil {
		if !found && errors.Is(err, kvlite.ErrKeyNotFound) {
			return nil
		}
		return runner.checkCallError(op, err)
	}
	if !found {
		runner.fail("%s: bucket %q key %x: got a %d-byte value for a missing key", op, bucket, shortKey(key), len(value))
		return errCheckFailed
	}
	if !bytes.Equal(value, want) || value == nil {
		runner.fail("%s: bucket %q key %x: got %d bytes, want %d bytes", op, bucket, shortKey(key), len(value), len(want))
		return errCheckFailed
	}
	return nil
}

// readState reads every bucket of the name universe in one read transaction.
// It checks that keys are in strictly increasing order and that no value is nil.
func readState(db *kvlite.DB, bucketNames []string) (dbState, error) {
	state := dbState{}
	err := db.View(func(tx *kvlite.Tx) error {
		for _, name := range bucketNames {
			bucket, err := tx.Bucket([]byte(name))
			if errors.Is(err, kvlite.ErrBucketNotFound) {
				continue
			}
			if err != nil {
				return fmt.Errorf("open bucket %q: %w", name, err)
			}
			content := map[string][]byte{}
			cursor, err := bucket.Cursor()
			if err != nil {
				return err
			}
			var previous []byte
			key, value, err := cursor.First()
			for ; err == nil && key != nil; key, value, err = cursor.Next() {
				if previous != nil && bytes.Compare(previous, key) >= 0 {
					return fmt.Errorf("bucket %q: key %x does not sort after %x", name, key, previous)
				}
				previous = bytes.Clone(key)
				if value == nil {
					return fmt.Errorf("bucket %q: key %x has a nil value", name, key)
				}
				content[string(key)] = bytes.Clone(value)
			}
			if err != nil {
				return fmt.Errorf("scan bucket %q: %w", name, err)
			}
			state[name] = content
		}
		return nil
	})
	return state, err
}

// checkPages walks the tree and compares it with the allocation bitmap.
func (runner *runner) checkPages(when string) {
	if err := runner.target.CheckPages(runner.db); err != nil {
		runner.fail("page ownership %s: %v", when, err)
	}
}

// verify reads the whole database, checks page ownership, and matches the result with the model.
// After a crash, the state can be any state that the model allows.
// After a clean close, it must be the latest state.
func (runner *runner) verify(afterCrash bool) {
	runner.stats.Recoveries++
	recovered, err := readState(runner.db, runner.work.bucketNames)
	if err != nil {
		runner.fail("read the recovered database: %v", err)
		return
	}
	runner.checkPages("after open")
	if runner.failure != nil {
		return
	}

	lowest := runner.model.latest
	if afterCrash && runner.powerLost {
		lowest = runner.model.floor
	}
	before := runner.model.latest
	result, branch, err := runner.model.resolve(recovered, lowest, afterCrash)
	if err != nil {
		runner.fail("recovery (after crash: %t, power lost: %t): %v", afterCrash, runner.powerLost, err)
		return
	}
	if runner.model.latest < before {
		runner.stats.LostUnsyncedCommits += before - runner.model.latest
	}
	if runner.uncertainOpen {
		runner.uncertainOpen = false
		if branch {
			runner.stats.UncertainKept++
		} else {
			runner.stats.UncertainLost++
		}
	}
	runner.event("verified open: %s (state hash %x)", result, runner.model.hash)
	runner.powerLost = false
	if runner.config.Sync != kvlite.SyncNone {
		// Writable recovery syncs the WAL and the main file before it clears the WAL.
		runner.model.setFloor()
	}
}
