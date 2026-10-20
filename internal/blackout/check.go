package blackout

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// errCheckFailed is the error a callback returns after a failed check, so that its transaction ends.
var errCheckFailed = errors.New("blackout check failed")

// explained reports whether a fault explains err.
// The error chain must contain an injected I/O error or a simulated crash; "some error after some fault" is not enough.
// The one exception is a read that returned a flipped bit in the current step: KVLite then reports a checksum or format error, without the cause in the chain.
func (runner *runner) explained(err error) bool {
	if errors.Is(err, disk.ErrInjected) || errors.Is(err, disk.ErrCrashed) {
		return true
	}
	corrupted := runner.fsys.Stats().ReadCorruptions > runner.readCorruptions
	return corrupted && (errors.Is(err, kvlite.ErrChecksum) || errors.Is(err, kvlite.ErrInvalid))
}

// expectError checks a call that the model expects to fail with want.
// An error that a fault explains ends the transaction.
func (runner *runner) expectError(op string, err, want error) error {
	if errors.Is(err, want) {
		return nil
	}
	if runner.explained(err) {
		return err
	}
	runner.fail("%s: got error %v, want %v", op, err, want)
	return errCheckFailed
}

// checkCallError accepts nil, or an error that a fault explains.
func (runner *runner) checkCallError(op string, err error) error {
	if err == nil || runner.explained(err) {
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
		runner.fail("%s: bucket %q key %s: got a %d-byte value for a missing key", op, bucket, shortKey(key), len(value))
		return errCheckFailed
	}
	if !bytes.Equal(value, want) || value == nil {
		runner.fail("%s: bucket %q key %s: got %d bytes, want %d bytes", op, bucket, shortKey(key), len(value), len(want))
		return errCheckFailed
	}
	return nil
}

// readState reads the whole database: every top-level bucket that the catalog lists, every nested bucket, and the checksum mode.
// The catalog read finds buckets that the model does not know.
func (runner *runner) readState(db *kvlite.DB) (dbState, error) {
	names, err := runner.target.BucketNames(db)
	if err != nil {
		return nil, fmt.Errorf("read the bucket catalog: %w", err)
	}
	state, err := readBuckets(db, names)
	if err != nil {
		return nil, err
	}
	state[metaPath] = map[string][]byte{checksumsKey: checksumsValue(runner.target.DataPageChecksums(db))}
	return state, nil
}

// readBuckets reads the named top-level buckets, and every nested bucket below them, in one read transaction.
// It checks that keys are in strictly increasing order, and that [kvlite.Bucket.Stats] counts what the read found.
func readBuckets(db *kvlite.DB, names []string) (dbState, error) {
	state := dbState{}
	err := db.View(func(tx *kvlite.Tx) error {
		for _, name := range names {
			bucket, err := tx.Bucket([]byte(name))
			if err != nil {
				return fmt.Errorf("open bucket %q from the catalog: %w", name, err)
			}
			if err := readBucket(bucket, name, state); err != nil {
				return err
			}
			if err := checkBucketStats(bucket, name, state); err != nil {
				return err
			}
		}
		return nil
	})
	return state, err
}

// checkBucketStats compares the entry and bucket counts of Bucket.Stats with the read of the same transaction.
func checkBucketStats(bucket *kvlite.Bucket, path string, state dbState) error {
	stats, err := bucket.Stats()
	if err != nil {
		return fmt.Errorf("stats of bucket %q: %w", path, err)
	}
	keys, buckets := 0, 0
	for other, content := range state {
		if other != path && !strings.HasPrefix(other, path+"/") {
			continue
		}
		buckets++
		for _, value := range content {
			if value != nil {
				keys++
			}
		}
	}
	if stats.KeyN != keys || stats.BucketN != buckets {
		return fmt.Errorf("stats of bucket %q: KeyN=%d BucketN=%d, but the read found %d entries in %d buckets", path, stats.KeyN, stats.BucketN, keys, buckets)
	}
	return nil
}

// readBucket reads the bucket at path into state, and each nested bucket below it.
// A nested bucket is an entry with a nil value.
func readBucket(bucket *kvlite.Bucket, path string, state dbState) error {
	content := map[string][]byte{}
	state[path] = content
	cursor, err := bucket.Cursor()
	if err != nil {
		return fmt.Errorf("cursor of bucket %q: %w", path, err)
	}
	var previous []byte
	key, value, err := cursor.First()
	for ; err == nil && key != nil; key, value, err = cursor.Next() {
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			return fmt.Errorf("bucket %q: key %x does not sort after %x", path, key, previous)
		}
		previous = bytes.Clone(key)
		if value != nil {
			content[string(key)] = bytes.Clone(value)
			continue
		}
		content[string(key)] = nil
		child, err := bucket.Bucket(key)
		if err != nil {
			return fmt.Errorf("open nested bucket %x in %q: %w", key, path, err)
		}
		if err := readBucket(child, path+"/"+string(key), state); err != nil {
			return err
		}
	}
	if err != nil {
		return fmt.Errorf("scan bucket %q: %w", path, err)
	}
	return nil
}

// checkPages walks the tree and compares it with the allocation bitmap.
// The walk runs with faults disabled, so the check cannot fail because of an injected error.
func (runner *runner) checkPages(when string) {
	runner.fsys.SetFaults(false)
	defer runner.fsys.SetFaults(runner.faults)
	if err := runner.target.CheckPages(runner.db); err != nil {
		runner.fail("page ownership %s: %v", when, err)
	}
}

// verify reads the whole database, checks page ownership, and matches the result with the model.
// After a crash, the state can be any state that the model allows.
// After a clean close, it must be the latest state.
func (runner *runner) verify(afterCrash bool) {
	// The checks run with faults disabled, so they cannot fail because of an injected error.
	runner.fsys.SetFaults(false)
	defer runner.fsys.SetFaults(runner.faults)
	runner.stats.Recoveries++
	recovered, err := runner.readState(runner.db)
	if err != nil {
		runner.fail("read the recovered database: %v", err)
		return
	}
	runner.checkMappings(runner.fsys)
	runner.checkPages("after open")
	if runner.failure != nil {
		return
	}

	// A failed sync that dropped the cache view loses unsynced changes without a crash, as a power failure does.
	lowest, allowBranches := runner.allowedStates(afterCrash)
	droppedCache := runner.fsys.Stats().DroppedCaches > runner.droppedCaches
	runner.droppedCaches = runner.fsys.Stats().DroppedCaches
	before := runner.model.latest
	result, branch, err := runner.model.resolve(recovered, lowest, allowBranches)
	if err != nil {
		runner.fail("recovery (after crash: %t, power lost: %t, cache dropped: %t): %v", afterCrash, runner.powerLost, droppedCache, err)
		return
	}
	if runner.model.latest < before {
		runner.stats.LostUnsyncedCommits += before - runner.model.latest
	}
	if runner.readOnlyState != nil {
		// A failed sync that dropped the cache after the read-only recovery changed what reads see, as a power failure does.
		if runner.fsys.Stats().DroppedCaches == runner.readOnlyDrops && !statesEqual(runner.readOnlyState, recovered) {
			runner.fail("the read-only recovery showed another state than the writable recovery:\n%s", stateDiff(runner.readOnlyState, recovered))
		}
		runner.readOnlyState = nil
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
	// A writable recovery syncs and clears the WAL, so it decides each unknown result for good.
	// A lost branch that appears in a later recovery is a revived commit, not a valid answer.
	runner.model.branches = nil
	runner.powerLost = false
	if runner.syncing() {
		// Writable recovery syncs the WAL and the main file before it clears the WAL.
		runner.model.setFloor()
	} else {
		runner.clearTaint()
	}
}
