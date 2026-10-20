package blackout

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/Issaminu/kvlite"
)

// readers runs several read transactions at once, each one in its own goroutine.
// Only one goroutine runs at a time: the runner gives one reader a turn, and the workload generator picks the next reader, so the seed replays.
// A writer waits for every open View, so this step has readers only.
//
// Each reader keeps the slices that its reads return, without a copy, while the other readers read and change the node cache.
// At the end of its View, every kept slice must still hold its bytes: KVLite documents that they stay valid until the callback returns.
func (runner *runner) readers() {
	count := runner.work.rand.Range(2, 4)
	runner.event("%d readers at once", count)
	type reader struct {
		id   int
		turn chan struct{} // the runner gives the reader a turn
		done chan bool     // the reader ends its turn; true means that the reader finished
	}
	active := make([]*reader, count)
	for index := range active {
		active[index] = &reader{id: index, turn: make(chan struct{}), done: make(chan bool)}
		go runner.runReader(active[index].id, active[index].turn, active[index].done)
	}
	for len(active) > 0 {
		next := runner.work.rand.IntN(len(active))
		runner.stats.ReaderTurns++
		active[next].turn <- struct{}{}
		if finished := <-active[next].done; finished {
			active = slices.Delete(active, next, next+1)
		}
	}
}

// runReader is one reader of readers.
// It waits for a turn before each read, and it ends each turn with done: false after a read, or true when it finished.
func (runner *runner) runReader(id int, turn <-chan struct{}, done chan<- bool) {
	finished := false
	defer func() {
		if value := recover(); value != nil {
			runner.fail("reader %d: panic: %v", id, value)
		}
		if !finished {
			done <- true
		}
	}()
	<-turn
	// yield ends the turn and waits for the next one. It reports false when the run already failed, so the reader stops.
	yield := func() bool {
		done <- false
		<-turn
		return runner.failure == nil
	}
	steps := runner.work.rand.Range(1, 8)
	op := fmt.Sprintf("reader %d", id)
	err := runner.db.View(func(tx *kvlite.Tx) error {
		var held []heldRead
		var cursor *kvlite.Cursor
		var cursorPath string
		var cursorKeys []string
		position := 0
		for step := range steps {
			if step > 0 && !yield() {
				return errCheckFailed
			}
			if cursor == nil || runner.work.rand.Bool() {
				// A point read.
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
					if err := runner.expectError(op+" Bucket.Get", err, want); err != nil {
						return err
					}
					continue
				}
				if err := runner.checkValue(op+" Bucket.Get", path, key, value, err); err != nil {
					return err
				}
				held = append(held, heldRead{op: op + " Bucket.Get", path: path, wantKey: key, value: value, want: runner.model.state[path][key]})
				if cursor == nil && runner.work.rand.Bool() {
					// Start a cursor in this bucket for later turns.
					if cursor, err = bucket.Cursor(); err != nil {
						return runner.checkCallError(op+" Cursor", err)
					}
					cursorPath, cursorKeys, position = path, slices.Sorted(maps.Keys(runner.model.state[path])), -1
				}
				continue
			}
			// One cursor step: First at the start, then Next.
			var key, value []byte
			var err error
			if position < 0 {
				key, value, err = cursor.First()
			} else {
				key, value, err = cursor.Next()
			}
			if err != nil {
				return runner.checkCallError(op+" cursor", err)
			}
			position++
			if position >= len(cursorKeys) {
				if key != nil {
					runner.fail("%s: cursor of bucket %q returned key %s after its last key", op, cursorPath, shortKey(string(key)))
					return errCheckFailed
				}
				cursor = nil
				continue
			}
			want := runner.model.state[cursorPath][cursorKeys[position]]
			if string(key) != cursorKeys[position] || (value == nil) != (want == nil) || string(value) != string(want) {
				runner.fail("%s: cursor of bucket %q at entry %d returned key %s with %d bytes, want key %s with %d bytes", op, cursorPath, position, shortKey(string(key)), len(value), shortKey(cursorKeys[position]), len(want))
				return errCheckFailed
			}
			held = append(held, heldRead{op: op + " cursor", path: cursorPath, key: key, wantKey: cursorKeys[position], value: value, want: want})
		}
		return runner.checkHeld(held)
	})
	if err != nil && runner.failure == nil && !runner.explained(err) && !errors.Is(err, errCheckFailed) {
		runner.fail("%s: View: unexpected error: %v", op, err)
	}
	finished = true
	done <- true
}
