package blackout

import (
	"bytes"
	"errors"
	"strings"

	"github.com/Issaminu/kvlite"
)

// contract is one API promise that the contracts operation checks.
type contract uint8

const (
	contractWriteInView       contract = iota // writes in View return ErrTxNotWritable
	contractClosedTx                          // a Tx used after its callback returns ErrTxClosed
	contractCursorInvalidated                 // a cursor used after a change to its bucket returns ErrCursorInvalidated
	contractNilScanCallback                   // a nil scan callback returns ErrScanCallbackRequired
	contractScanCallbackError                 // a scan callback error stops the scan and comes back
	contractKeyLimits                         // empty and oversized keys and names return their errors
	contractPanicInUpdate                     // a panic in Update discards the changes and reaches the caller
	contractGoexitInUpdate                    // runtime.Goexit in Update discards the changes and ends the goroutine
	contractCount
)

var contractNames = [contractCount]string{"write in View", "closed Tx", "cursor invalidated", "nil scan callback", "scan callback error", "key limits", "panic in Update", "Goexit in Update"}

// errStopScan is the error that a scan callback returns to stop the scan.
var errStopScan = errors.New("blackout scan callback stops the scan")

// contracts checks one API promise.
// The checks change no committed data, so the model does not change.
func (runner *runner) contracts() {
	check := contract(runner.work.rand.IntN(int(contractCount)))
	runner.stats.Contracts[check]++
	runner.event("contract: %s", contractNames[check])
	switch check {
	case contractWriteInView:
		runner.checkWriteInView()
	case contractClosedTx:
		runner.checkClosedTx()
	case contractCursorInvalidated:
		runner.checkCursorInvalidated()
	case contractNilScanCallback:
		runner.checkNilScanCallback()
	case contractScanCallbackError:
		runner.checkScanCallbackError()
	case contractKeyLimits:
		runner.checkKeyLimits()
	case contractPanicInUpdate:
		runner.checkPanicInUpdate()
	case contractGoexitInUpdate:
		runner.checkGoexitInUpdate()
	}
}

// existingBucket returns an existing bucket path, or false when the database has no bucket.
func (runner *runner) existingBucket() (string, bool) {
	if len(runner.model.bucketPaths()) == 0 {
		return "", false
	}
	path := runner.work.pickBucket(runner.model)
	_, ok := runner.model.state[path]
	return path, ok
}

// viewCheck runs check in a read transaction and accepts only a nil error or an error that a fault explains.
func (runner *runner) viewCheck(op string, check func(tx *kvlite.Tx) error) {
	err := runner.db.View(check)
	if err != nil && runner.failure == nil && !explained(err) {
		runner.fail("%s: unexpected error: %v", op, err)
	}
}

func (runner *runner) checkWriteInView() {
	runner.viewCheck("write in View", func(tx *kvlite.Tx) error {
		if _, err := tx.CreateBucket([]byte("never")); !errors.Is(err, kvlite.ErrTxNotWritable) {
			runner.fail("CreateBucket in View returned %v, want ErrTxNotWritable", err)
		}
		path, ok := runner.existingBucket()
		if !ok {
			return nil
		}
		bucket, err := runner.openPath(tx, path)
		if bucket == nil {
			return err
		}
		if err := bucket.Put([]byte("key"), []byte("value")); !errors.Is(err, kvlite.ErrTxNotWritable) {
			runner.fail("Put in View returned %v, want ErrTxNotWritable", err)
		}
		if err := bucket.Delete([]byte("key")); !errors.Is(err, kvlite.ErrTxNotWritable) {
			runner.fail("Delete in View returned %v, want ErrTxNotWritable", err)
		}
		if err := bucket.DeleteBatch([][]byte{[]byte("key")}); !errors.Is(err, kvlite.ErrTxNotWritable) {
			runner.fail("DeleteBatch in View returned %v, want ErrTxNotWritable", err)
		}
		if _, err := bucket.CreateBucket([]byte("never")); !errors.Is(err, kvlite.ErrTxNotWritable) {
			runner.fail("Bucket.CreateBucket in View returned %v, want ErrTxNotWritable", err)
		}
		if err := tx.DeleteBucket([]byte(strings.SplitN(path, "/", 2)[0])); !errors.Is(err, kvlite.ErrTxNotWritable) {
			runner.fail("DeleteBucket in View returned %v, want ErrTxNotWritable", err)
		}
		return nil
	})
}

func (runner *runner) checkClosedTx() {
	var saved *kvlite.Tx
	var savedBucket *kvlite.Bucket
	err := runner.db.Update(func(tx *kvlite.Tx) error {
		saved = tx
		if path, ok := runner.existingBucket(); ok {
			bucket, err := runner.openPath(tx, path)
			if bucket == nil {
				return err
			}
			savedBucket = bucket
		}
		// Abort, so the transaction changes nothing.
		return errAbort
	})
	runner.target.Quiesce(runner.db)
	if !errors.Is(err, errAbort) {
		_ = runner.checkCallError("Update for a closed Tx", err)
		return
	}
	if saved.Writable() {
		runner.fail("Tx.Writable is true after the callback returned")
	}
	if _, err := saved.Bucket([]byte(runner.work.bucketNames[0])); !errors.Is(err, kvlite.ErrTxClosed) {
		runner.fail("Tx.Bucket after the callback returned %v, want ErrTxClosed", err)
	}
	if savedBucket != nil {
		if err := savedBucket.Put([]byte("key"), []byte("value")); !errors.Is(err, kvlite.ErrTxClosed) {
			runner.fail("Bucket.Put after the callback returned %v, want ErrTxClosed", err)
		}
		if _, err := savedBucket.Get([]byte("key")); !errors.Is(err, kvlite.ErrTxClosed) {
			runner.fail("Bucket.Get after the callback returned %v, want ErrTxClosed", err)
		}
	}
}

func (runner *runner) checkCursorInvalidated() {
	path, ok := runner.existingBucket()
	if !ok {
		return
	}
	err := runner.db.Update(func(tx *kvlite.Tx) error {
		bucket, err := runner.openPath(tx, path)
		if bucket == nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err := runner.checkCallError("Cursor", err); err != nil {
			return err
		}
		if _, _, err := cursor.First(); err != nil {
			return runner.checkCallError("Cursor.First", err)
		}
		if err := bucket.Put([]byte("blackout-cursor-key"), []byte("value")); err != nil {
			return runner.checkCallError("Put", err)
		}
		if _, _, err := cursor.Next(); !errors.Is(err, kvlite.ErrCursorInvalidated) && !explained(err) {
			runner.fail("Cursor.Next after a Put returned %v, want ErrCursorInvalidated", err)
		}
		// Abort, so the Put changes nothing.
		return errAbort
	})
	runner.target.Quiesce(runner.db)
	if !errors.Is(err, errAbort) {
		_ = runner.checkCallError("Update for cursor invalidation", err)
	}
}

func (runner *runner) checkNilScanCallback() {
	path, ok := runner.existingBucket()
	if !ok {
		return
	}
	runner.viewCheck("nil scan callback", func(tx *kvlite.Tx) error {
		bucket, err := runner.openPath(tx, path)
		if bucket == nil {
			return err
		}
		if err := bucket.ScanRange(nil, nil, nil); !errors.Is(err, kvlite.ErrScanCallbackRequired) {
			runner.fail("ScanRange with a nil callback returned %v, want ErrScanCallbackRequired", err)
		}
		if err := bucket.ScanPrefix(nil, nil); !errors.Is(err, kvlite.ErrScanCallbackRequired) {
			runner.fail("ScanPrefix with a nil callback returned %v, want ErrScanCallbackRequired", err)
		}
		if err := bucket.ScanReverse(nil); !errors.Is(err, kvlite.ErrScanCallbackRequired) {
			runner.fail("ScanReverse with a nil callback returned %v, want ErrScanCallbackRequired", err)
		}
		return nil
	})
}

func (runner *runner) checkScanCallbackError() {
	path, ok := runner.existingBucket()
	if !ok {
		return
	}
	runner.viewCheck("scan callback error", func(tx *kvlite.Tx) error {
		bucket, err := runner.openPath(tx, path)
		if bucket == nil {
			return err
		}
		calls := 0
		stop := func(key, value []byte) error {
			calls++
			return errStopScan
		}
		for _, scan := range []func() error{
			func() error { return bucket.ScanRange(nil, nil, stop) },
			func() error { return bucket.ScanPrefix(nil, stop) },
			func() error { return bucket.ScanReverse(stop) },
		} {
			calls = 0
			err := scan()
			if explained(err) {
				return err
			}
			if len(runner.model.state[path]) == 0 {
				if err != nil || calls != 0 {
					runner.fail("scan of an empty bucket returned %v after %d calls", err, calls)
				}
				continue
			}
			if !errors.Is(err, errStopScan) || calls != 1 {
				runner.fail("scan with a stopping callback returned %v after %d calls, want errStopScan after 1 call", err, calls)
			}
		}
		return nil
	})
}

func (runner *runner) checkKeyLimits() {
	if _, err := runner.db.Get(nil, []byte("key")); !errors.Is(err, kvlite.ErrBucketNameRequired) {
		runner.fail("DB.Get with an empty bucket name returned %v, want ErrBucketNameRequired", err)
	}
	path, ok := runner.existingBucket()
	err := runner.db.Update(func(tx *kvlite.Tx) error {
		if _, err := tx.CreateBucket(nil); !errors.Is(err, kvlite.ErrBucketNameRequired) {
			runner.fail("CreateBucket with an empty name returned %v, want ErrBucketNameRequired", err)
		}
		if _, err := tx.Bucket(nil); !errors.Is(err, kvlite.ErrBucketNameRequired) {
			runner.fail("Tx.Bucket with an empty name returned %v, want ErrBucketNameRequired", err)
		}
		if !ok {
			return errAbort
		}
		bucket, err := runner.openPath(tx, path)
		if bucket == nil {
			if err != nil {
				return err
			}
			return errAbort
		}
		tooLarge := bytes.Repeat([]byte{'k'}, kvlite.MaxKeySize+1)
		for _, test := range []struct {
			name string
			err  error
			want error
		}{
			{"Put with an empty key", bucket.Put(nil, []byte("value")), kvlite.ErrKeyRequired},
			{"Delete with an empty key", bucket.Delete(nil), kvlite.ErrKeyRequired},
			{"Put with a key over MaxKeySize", bucket.Put(tooLarge, []byte("value")), kvlite.ErrKeyTooLarge},
			{"Delete with a key over MaxKeySize", bucket.Delete(tooLarge), kvlite.ErrKeyTooLarge},
		} {
			if !errors.Is(test.err, test.want) && !explained(test.err) {
				runner.fail("%s returned %v, want %v", test.name, test.err, test.want)
			}
		}
		if _, err := bucket.Get(nil); !errors.Is(err, kvlite.ErrKeyRequired) && !explained(err) {
			runner.fail("Get with an empty key returned %v, want ErrKeyRequired", err)
		}
		if _, err := bucket.CreateBucket(nil); !errors.Is(err, kvlite.ErrBucketNameRequired) && !explained(err) {
			runner.fail("Bucket.CreateBucket with an empty name returned %v, want ErrBucketNameRequired", err)
		}
		// A returned error does not change the bucket, but abort anyway, so the check changes nothing.
		return errAbort
	})
	runner.target.Quiesce(runner.db)
	if !errors.Is(err, errAbort) {
		_ = runner.checkCallError("Update for key limits", err)
	}
}

// panicChanges returns a transaction to run before a panic or a Goexit.
func (runner *runner) panicChanges() []txMutation {
	mutations, _ := runner.work.newTransaction(runner.model)
	return mutations
}

func (runner *runner) checkPanicInUpdate() {
	mutations := runner.panicChanges()
	var applied []txMutation
	var recovered any
	var err error
	func() {
		defer func() { recovered = recover() }()
		err = runner.db.Update(func(tx *kvlite.Tx) error {
			return runner.runCallback(tx, mutations, false, endPanic, &applied)
		})
	}()
	runner.target.Quiesce(runner.db)
	switch {
	case recovered == nil:
		// A change can fail before the panic, and then the callback returns its error instead.
		if err == nil {
			runner.fail("Update with a panicking callback returned nil and did not panic")
		}
	case recovered != (panicValue{step: runner.step}):
		runner.fail("Update panicked with %v, want the panic value of its callback", recovered)
	}
}

func (runner *runner) checkGoexitInUpdate() {
	mutations := runner.panicChanges()
	var applied []txMutation
	var err error
	returned := false
	done := make(chan struct{})
	go func() {
		defer close(done)
		err = runner.db.Update(func(tx *kvlite.Tx) error {
			return runner.runCallback(tx, mutations, false, endGoexit, &applied)
		})
		returned = true
	}()
	<-done
	runner.target.Quiesce(runner.db)
	// A change can fail before the Goexit, and then Update returns the callback error.
	if returned && err == nil {
		runner.fail("Update with an exiting callback returned nil")
	}
}
