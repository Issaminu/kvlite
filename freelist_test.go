package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// validatePageOwnership walks every tree page to test the bitmap against the tree.
// Normal Open checks only bitmap pages, so this test helper can find an incorrect bit on a child page.
func (db *DB) validatePageOwnership() error {
	if db.allocation == nil || db.meta == nil {
		return ErrInvalid
	}

	visited := make(map[page.ID]struct{})
	pending := []page.ID{db.meta.Root()}
	for len(pending) > 0 {
		last := len(pending) - 1
		pageID := pending[last]
		pending = pending[:last]
		if _, exists := visited[pageID]; exists {
			return fmt.Errorf("tree page %d has more than one owner: %w", pageID, ErrInvalid)
		}
		if !db.allocation.allocated(pageID) {
			return fmt.Errorf("allocation bitmap marks reachable page %d free: %w", pageID, ErrInvalid)
		}
		visited[pageID] = struct{}{}

		node, err := db.readNode(pageID)
		if err != nil {
			return err
		}
		if !node.IsLeaf() {
			pending = append(pending, node.Children...)
			continue
		}
		for index := 0; index < node.EntryCount(); index++ {
			entry := node.EntryAt(index)
			if entry.Flags()&btree.BucketLeafFlag == 0 {
				continue
			}
			rootPageID, err := page.DecodeID(entry.Value())
			if err != nil {
				return err
			}
			pending = append(pending, rootPageID)
		}
	}

	allocatedPages := db.allocation.allocatedPageCount()
	reservedPages := uint64(2 + len(db.allocation.segments))
	if allocatedPages < reservedPages {
		return fmt.Errorf("allocation bitmap marks %d pages used, want at least %d: %w", allocatedPages, reservedPages, ErrInvalid)
	}
	if uint64(len(visited)) == allocatedPages-reservedPages {
		return nil
	}
	for pageID := firstTreePageID; pageID <= db.meta.LastPage(); pageID++ {
		if !db.allocation.allocated(pageID) || db.allocation.isSegmentPage(pageID) {
			continue
		}
		if _, reachable := visited[pageID]; !reachable {
			return fmt.Errorf("allocated tree page %d has no owner: %w", pageID, ErrInvalid)
		}
	}
	return fmt.Errorf("tree owns %d pages, allocation bitmap marks %d tree pages used: %w", len(visited), allocatedPages-reservedPages, ErrInvalid)
}

func TestAllocationChangesPageRecordsOnlyIncludeChangedSegments(t *testing.T) {
	bitmap := newAllocationBitmap(allocationHeaderSize + 1)
	changes := newAllocationChanges(bitmap)
	changes.markAllocated(4)
	if got := changes.pageRecords(); len(got) != 1 || got[0].PageID != firstAllocationSegmentID || !bytes.Equal(got[0].Payload, changes.segment(0).data) {
		t.Fatalf("changed allocation pages: got %+v", got)
	}
	if got := newAllocationChanges(bitmap).pageRecords(); len(got) != 0 {
		t.Fatalf("unchanged allocation pages: got %+v", got)
	}
	changes.release(4)
	if got := changes.pageRecords(); len(got) != 0 {
		t.Fatalf("restored allocation page: got %+v", got)
	}
}

func TestAllocationChangesCloneIsIndependent(t *testing.T) {
	bitmap := newAllocationBitmap(allocationHeaderSize + 1)
	changes := newAllocationChanges(bitmap)
	changes.markAllocated(4)

	clone := changes.clone()
	clone.markAllocated(5)

	if bitmap.allocated(4) {
		t.Fatal("private allocation changed the committed bitmap")
	}
	if changes.allocated(5) {
		t.Fatal("cloned allocation changed its parent")
	}
	if !clone.allocated(5) {
		t.Fatal("clone did not record its allocation")
	}
}

func TestAllocationChangesReusesFreePageAndRetiresTailPage(t *testing.T) {
	bitmap := newAllocationBitmap(4096)
	bitmap.markAllocated(4)
	bitmap.markAllocated(5)
	changes := newAllocationChanges(bitmap)
	changes.release(4)

	if got, err := changes.allocate(5); err != nil || got != 4 {
		t.Fatalf("reused page: got %d, error %v; want page 4", got, err)
	}
	if changes.pagesReused != 1 {
		t.Fatalf("reused pages: got %d, want 1", changes.pagesReused)
	}
	if err := changes.retire(5); err != nil {
		t.Fatal(err)
	}
	if changes.pagesRetired != 1 {
		t.Fatalf("retired pages: got %d, want 1", changes.pagesRetired)
	}
	meta := page.NewMeta(4096)
	meta.SetLastPage(5)
	if !changes.finalize(meta) {
		t.Fatal("finalize did not lower the last page")
	}
	if got := meta.LastPage(); got != 4 {
		t.Fatalf("last page: got %d, want 4", got)
	}
	if changes.tailPagesRemoved != 1 {
		t.Fatalf("removed tail pages: got %d, want 1", changes.tailPagesRemoved)
	}
	if changes.allocated(5) {
		t.Fatal("retired tail page is still allocated")
	}
}

func TestAllocationFinalizeTracksRemovedTailSegment(t *testing.T) {
	bitmap := newAllocationBitmap(allocationHeaderSize + 1)
	setup := newAllocationChanges(bitmap)
	highest := firstTreePageID
	for range 5 {
		pageID, err := setup.allocate(highest)
		if err != nil {
			t.Fatal(err)
		}
		highest = pageID
	}
	bitmap = setup.publish()
	changes := newAllocationChanges(bitmap)
	if err := changes.retire(highest); err != nil {
		t.Fatal(err)
	}
	meta := page.NewMeta(4096)
	meta.SetLastPage(highest)
	changes.finalize(meta)

	if _, found := changes.obsoleteWALPages[8]; !found {
		t.Fatal("removed allocation page 8 was not recorded")
	}
	if got := meta.LastPage(); got != 7 {
		t.Fatalf("last page: got %d, want 7", got)
	}
	clone := changes.clone()
	delete(clone.obsoleteWALPages, 8)
	if _, found := changes.obsoleteWALPages[8]; !found {
		t.Fatal("clone changed the original obsolete page set")
	}
}

func TestReadAllocationBitmapReadsNewDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bitmap, err := db.readAllocationBitmap(db.meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(bitmap.segments) != 1 || bitmap.allocatedPageCount() != 4 {
		t.Fatalf("new database allocation: %d segments, %d used pages", len(bitmap.segments), bitmap.allocatedPageCount())
	}
	if bitmap.segments[0].firstFreeBit != firstTreePageID+1 {
		t.Fatalf("first free bit: got %d, want %d", bitmap.segments[0].firstFreeBit, firstTreePageID+1)
	}
}

func TestReadAllocationBitmapReadsOnlySegmentPages(t *testing.T) {
	const pageSize = int64(512)
	file, err := os.Create(filepath.Join(t.TempDir(), "database"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	bitmap := newAllocationBitmap(pageSize)
	bitmap.addSegment()
	meta := page.NewMeta(pageSize)
	meta.SetLastPage(bitmap.segmentPageID(1))
	meta.RefreshChecksum()
	for index := range bitmap.segments {
		pageID := bitmap.segmentPageID(index)
		if _, err := file.WriteAt(bitmap.encodeSegment(index), int64(pageID)*pageSize); err != nil {
			t.Fatal(err)
		}
	}
	db := &DB{file: file, meta: meta}
	loaded, err := db.readAllocationBitmap(meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.segments) != 2 || !loaded.allocated(meta.LastPage()) {
		t.Fatalf("loaded allocation: %d segments, last page %d allocated: %t", len(loaded.segments), meta.LastPage(), loaded.allocated(meta.LastPage()))
	}

	valid := bitmap.encodeSegment(1)
	data := slices.Clone(valid)
	data[allocationHeaderSize] ^= 1
	if _, err := file.WriteAt(data, int64(meta.LastPage())*pageSize); err != nil {
		t.Fatal(err)
	}
	if _, err := db.readAllocationBitmap(meta, nil); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corrupt allocation page: got %v, want checksum error", err)
	}
	records := []wal.WALRecord{{Header: wal.RecordHeader{Type: wal.RecordTypeAllocation, PageID: meta.LastPage()}, Payload: valid}}
	if _, err := db.readAllocationBitmap(meta, records); err != nil {
		t.Fatalf("WAL allocation page did not replace damaged main page: %v", err)
	}
}

func TestReadAllocationBitmapRejectsMissingTail(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "database"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	meta := page.NewMeta(512)
	db := &DB{file: file, meta: meta}
	if _, err := db.readAllocationBitmap(meta, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing allocation page: got %v, want invalid file error", err)
	}
}

func TestReadAllocationBitmapAcceptsCommittedWALTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	meta := page.NewMeta(db.meta.PageSize())
	meta.SetLastPage(firstTreePageID + 1)
	meta.RefreshChecksum()
	oldBitmap := newAllocationBitmap(meta.PageSize())
	bitmap := newAllocationBitmap(meta.PageSize())
	bitmap.markAllocated(meta.LastPage())
	records := []wal.WALRecord{
		{Header: wal.RecordHeader{Type: wal.RecordTypeAllocation, PageID: firstAllocationSegmentID}, Payload: oldBitmap.encodeSegment(0)},
		{Header: wal.RecordHeader{Type: wal.RecordTypeAllocation, PageID: firstAllocationSegmentID}, Payload: bitmap.encodeSegment(0)},
		{Header: wal.RecordHeader{Type: wal.RecordTypeNode, PageID: meta.LastPage()}},
	}
	loaded, err := db.readAllocationBitmap(meta, records)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.allocated(meta.LastPage()) {
		t.Fatalf("WAL tail page %d is free", meta.LastPage())
	}
}

func TestAllocationBitmapSurvivesPutAndReopen(t *testing.T) {
	for _, test := range []struct {
		name string
		mode Sync
	}{{"SyncNone", SyncNone}, {"SyncFull", SyncFull}} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: test.mode})
			if err != nil {
				t.Fatal(err)
			}
			createBucket(t, db, []byte("bucket"))
			if err := db.Put([]byte("bucket"), []byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			lastPage := db.meta.LastPage()
			if !db.allocation.allocated(lastPage) {
				t.Fatalf("new page %d is not allocated", lastPage)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := Open(path, 0600, &Options{Synchronous: test.mode})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if !reopened.allocation.allocated(lastPage) {
				t.Fatalf("reopened page %d is not allocated", lastPage)
			}
			value, err := reopened.Get([]byte("bucket"), []byte("key"))
			if err != nil || !bytes.Equal(value, []byte("value")) {
				t.Fatalf("reopened value: got %q, error %v", value, err)
			}
		})
	}
}

func TestAllocationBitmapUsesCommittedWALOnReadOnlyOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, []byte("bucket"))
	if err := db.Put([]byte("bucket"), []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	lastPage := db.meta.LastPage()
	if _, found := db.wal.Lookup(firstAllocationSegmentID); !found {
		t.Fatal("write did not put the allocation page in the WAL")
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.closeFiles(); err != nil {
		t.Fatal(err)
	}

	readOnly, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if !readOnly.allocation.allocated(lastPage) {
		t.Fatalf("read-only open marks WAL page %d free", lastPage)
	}
	value, err := readOnly.Get([]byte("bucket"), []byte("key"))
	if err != nil || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("read-only value: got %q, error %v", value, err)
	}
}

func TestOpenRejectsDamagedWALAllocationBeforeCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	damaged := slices.Clone(db.allocation.encodeSegment(0))
	damaged[allocationHeaderSize] ^= 1
	if _, err := db.wal.Commit(nil, []wal.AllocationRecord{{PageID: firstAllocationSegmentID, Payload: damaged}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.unmapMainFile(); err != nil {
		t.Fatal(err)
	}
	if err := db.closeFiles(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, 0600, &Options{Synchronous: SyncNone}); !errors.Is(err, ErrChecksum) {
		t.Fatalf("open with damaged WAL allocation page: got %v, want checksum error", err)
	}
	if _, err := os.Stat(path + "-wal"); err != nil {
		t.Fatalf("failed open removed the WAL: %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	mainPage := make([]byte, pageSize)
	if _, err := file.ReadAt(mainPage, int64(firstAllocationSegmentID)*pageSize); err != nil {
		t.Fatal(err)
	}
	if err := verifyAllocationPage(mainPage, pageSize, firstAllocationSegmentID); err != nil {
		t.Fatalf("failed open damaged the main allocation page: %v", err)
	}
}

func TestDeleteReusesRetiredPageAfterReopen(t *testing.T) {
	for _, test := range []struct {
		name string
		mode Sync
	}{{"SyncNone", SyncNone}, {"SyncFull", SyncFull}} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			db, err := Open(path, 0600, &Options{Synchronous: test.mode})
			if err != nil {
				t.Fatal(err)
			}
			createBucket(t, db, []byte("data"))
			value := bytes.Repeat([]byte("v"), 300)
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.Bucket([]byte("data"))
				if err != nil {
					return err
				}
				for key := byte(1); key <= 120; key++ {
					if err := bucket.Put([]byte{key}, value); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			createBucket(t, db, []byte("tail"))
			lastPage := db.meta.LastPage()
			usedPages := db.allocation.allocatedPageCount()
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.Bucket([]byte("data"))
				if err != nil {
					return err
				}
				for key := byte(1); key < 120; key++ {
					if err := bucket.Delete([]byte{key}); err != nil {
						return err
					}
				}
				return errDiscardTx
			}); !errors.Is(err, errDiscardTx) {
				t.Fatalf("rolled-back page retirement: got %v, want callback error", err)
			}
			if got := db.allocation.allocatedPageCount(); got != usedPages {
				t.Fatalf("used pages after rollback: got %d, want %d", got, usedPages)
			}
			if valueAfterRollback, err := db.Get([]byte("data"), []byte{1}); err != nil || !bytes.Equal(valueAfterRollback, value) {
				t.Fatalf("value after rollback: got %q, error %v", valueAfterRollback, err)
			}

			if err := db.Delete([]byte("data"), []byte{1}); err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.Bucket([]byte("data"))
				if err != nil {
					return err
				}
				for key := byte(2); key < 120; key++ {
					if err := bucket.Delete([]byte{key}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if test.mode == SyncNone {
				if db.wal.Stats().CommittedRecordCount == 0 {
					t.Fatal("delete did not leave committed WAL pages for recovery")
				}
				if err := db.unmapMainFile(); err != nil {
					t.Fatal(err)
				}
				if err := db.closeFiles(); err != nil {
					t.Fatal(err)
				}
				readOnly, err := Open(path, 0600, &Options{ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := readOnly.Get([]byte("data"), []byte{1}); !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("deleted key after read-only WAL recovery: %v", err)
				}
				if err := readOnly.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(path, 0600, &Options{Synchronous: test.mode})
				if err != nil {
					t.Fatal(err)
				}
			}

			var freePage page.ID
			for pageID := firstTreePageID; pageID <= lastPage; pageID++ {
				if !db.allocation.allocated(pageID) {
					freePage = pageID
					break
				}
			}
			if freePage == 0 {
				t.Fatal("delete did not retire an interior page")
			}
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket([]byte("reuse"))
				if err != nil {
					return err
				}
				if got := bucket.rootNode.PageID(); got != freePage {
					return fmt.Errorf("reused page: got %d, want %d", got, freePage)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if got := db.meta.LastPage(); got != lastPage {
				t.Fatalf("last page after reuse: got %d, want %d", got, lastPage)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := Open(path, 0600, &Options{Synchronous: test.mode})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got, err := reopened.Get([]byte("data"), []byte{120})
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("remaining value: got %q, error %v", got, err)
			}
			if err := reopened.View(func(tx *Tx) error {
				_, err := tx.Bucket([]byte("reuse"))
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDeleteAPIContracts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	createBucket(t, db, []byte("parent"))
	if err := db.Put([]byte("parent"), []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	walBytes := db.wal.Stats().TotalBytesWritten
	if err := db.Delete([]byte("parent"), []byte("absent")); err != nil {
		t.Fatalf("missing key: %v", err)
	}
	if got := db.wal.Stats().TotalBytesWritten; got != walBytes {
		t.Fatalf("missing-key delete wrote %d WAL bytes", got-walBytes)
	}
	if err := db.Delete([]byte("missing"), []byte("key")); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("missing bucket: got %v, want ErrBucketNotFound", err)
	}
	if err := db.Delete([]byte("parent"), nil); !errors.Is(err, ErrKeyRequired) {
		t.Fatalf("empty key: got %v, want ErrKeyRequired", err)
	}
	if err := db.Delete([]byte("parent"), make([]byte, MaxKeySize+1)); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("large key: got %v, want ErrKeyTooLarge", err)
	}
	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		if err := bucket.Delete([]byte("key")); !errors.Is(err, ErrTxNotWritable) {
			t.Fatalf("read-only transaction delete: got %v, want ErrTxNotWritable", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var closedBucket *Bucket
	if err := db.Update(func(tx *Tx) error {
		var err error
		closedBucket, err = tx.Bucket([]byte("parent"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := closedBucket.Delete([]byte("key")); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("closed transaction delete: got %v, want ErrTxClosed", err)
	}
	if err := db.Update(func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		_, err = parent.CreateBucket([]byte("child"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("parent"), []byte("child")); !errors.Is(err, ErrIncompatibleValue) {
		t.Fatalf("nested bucket key: got %v, want ErrIncompatibleValue", err)
	}
	if err := db.Update(func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		if err := parent.Delete([]byte("key")); err != nil {
			return err
		}
		return errDiscardTx
	}); !errors.Is(err, errDiscardTx) {
		t.Fatalf("rolled-back delete: got %v, want callback error", err)
	}
	if value, err := db.Get([]byte("parent"), []byte("key")); err != nil || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("value after rollback: got %q, error %v", value, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if err := readOnly.Delete([]byte("parent"), []byte("key")); !errors.Is(err, ErrDatabaseReadOnly) {
		t.Fatalf("read-only delete: got %v, want ErrDatabaseReadOnly", err)
	}
}

func TestAllocationStatsCountCommittedDeleteAndReuse(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "database"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const count = 256
	for index := range count {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "old-%04d", index), bytes.Repeat([]byte("v"), 512)); err != nil {
			t.Fatal(err)
		}
	}
	before, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("discard delete")
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for index := 0; index < count; index += 2 {
			if err := bucket.Delete(fmt.Appendf(nil, "old-%04d", index)); err != nil {
				return err
			}
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("discarded delete: got %v, want %v", err, wantErr)
	}
	afterRollback, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if afterRollback.PagesRetired != before.PagesRetired || afterRollback.PagesReused != before.PagesReused || afterRollback.TailPagesReclaimed != before.TailPagesReclaimed {
		t.Fatalf("rollback changed page work: before %+v, after %+v", before, afterRollback)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for index := 0; index < count; index += 2 {
			if err := bucket.Delete(fmt.Appendf(nil, "old-%04d", index)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	afterDelete, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if afterDelete.PagesRetired <= before.PagesRetired || afterDelete.ReusablePageCount == 0 {
		t.Fatalf("delete did not free pages: %+v", afterDelete)
	}
	for index := range count / 2 {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "new-%04d", index), bytes.Repeat([]byte("n"), 512)); err != nil {
			t.Fatal(err)
		}
	}
	afterReuse, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if afterReuse.PagesReused <= afterDelete.PagesReused {
		t.Fatalf("new keys did not reuse free pages: %+v", afterReuse)
	}
}

func TestDeleteRemovesRetiredTreePagesFromWAL(t *testing.T) {
	for _, checkpointBeforeDelete := range []bool{true, false} {
		name := "older WAL pages"
		if checkpointBeforeDelete {
			name = "checkpointed pages"
		}
		t.Run(name, func(t *testing.T) {
			db, err := openDB(filepath.Join(t.TempDir(), "database"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close database: %v", err)
				}
			})
			db.wal.SetCheckpointThresholdBytes(math.MaxUint64)

			const entryCount = 256
			value := bytes.Repeat([]byte("v"), 512)
			for index := range entryCount {
				if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%04d", index), value); err != nil {
					t.Fatal(err)
				}
			}
			if checkpointBeforeDelete {
				if err := db.checkpointWAL(); err != nil {
					t.Fatal(err)
				}
			}
			before, err := db.Stats()
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.Bucket(testBucketName)
				if err != nil {
					return err
				}
				for index := range entryCount {
					if err := bucket.Delete(fmt.Appendf(nil, "key-%04d", index)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			after, err := db.Stats()
			if err != nil {
				t.Fatal(err)
			}
			if after.PagesRetired <= before.PagesRetired {
				t.Fatal("delete did not retire a tree page")
			}
			for pageID, record := range db.wal.CommittedRecords() {
				if record.Header.Type == wal.RecordTypeNode && !db.allocation.allocated(pageID) {
					t.Fatalf("WAL kept retired tree page %d", pageID)
				}
			}
		})
	}
}

func TestCheckRejectsReachablePageMarkedFree(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 256 {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%04d", index), bytes.Repeat([]byte("v"), 256)); err != nil {
			t.Fatal(err)
		}
	}
	var childPageID page.ID
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if bucket.rootNode.IsLeaf() {
			return errors.New("test bucket did not split")
		}
		childPageID = bucket.rootNode.Children[0]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatalf("check valid database: %v", err)
	}
	pageSize := db.meta.PageSize()
	pagesPerSegment := db.allocation.pagesPerSegment()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	segment := make([]byte, pageSize)
	if _, err := file.ReadAt(segment, int64(firstAllocationSegmentID)*pageSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	clearAllocationBit(segment[allocationHeaderSize:], childPageID%pagesPerSegment)
	sealAllocationPage(segment)
	if _, err := file.WriteAt(segment, int64(firstAllocationSegmentID)*pageSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	readOnly, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open with checksum-valid bitmap: %v", err)
	}
	defer readOnly.Close()
	if err := readOnly.validatePageOwnership(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("check reachable page marked free: got %v, want ErrInvalid", err)
	}
}

func TestCheckRejectsAllocatedPageWithoutOwner(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "database"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const entryCount = 512
	value := bytes.Repeat([]byte("v"), 256)
	for index := range entryCount {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%04d", index), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for index := 0; index < entryCount; index += 2 {
			if err := bucket.Delete(fmt.Appendf(nil, "key-%04d", index)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatalf("check valid database: %v", err)
	}

	for pageID := firstTreePageID; pageID <= db.meta.LastPage(); pageID++ {
		if db.allocation.allocated(pageID) || db.allocation.isSegmentPage(pageID) {
			continue
		}
		db.allocation.markAllocated(pageID)
		if err := db.validatePageOwnership(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("check page %d with no owner: got %v, want ErrInvalid", pageID, err)
		}
		return
	}
	t.Fatal("delete did not leave an interior free page")
}

func TestAllocationReopensAndReusesPageFromSecondSegment(t *testing.T) {
	const pageSize int64 = 4096
	path := filepath.Join(t.TempDir(), "database")
	bitmap := newAllocationBitmap(pageSize)
	// Fill the first segment in a sparse file. This avoids writing thousands of tree pages.
	for index := range bitmap.segments[0].bitmapBits() {
		bitmap.segments[0].bitmapBits()[index] = 0xff
	}
	bitmap.segments[0].firstFreeBit = bitmap.pagesPerSegment()
	bitmap.addSegment()
	secondPage := bitmap.segmentPageID(1) + 1
	highPage := secondPage + 1
	// Keep a used page after secondPage so that secondPage is an interior free page.
	bitmap.markAllocated(highPage)

	meta := page.NewMeta(pageSize)
	meta.SetLastPage(highPage)
	meta.RefreshChecksum()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, pageSize} {
		if _, err := file.WriteAt(page.EncodeMeta(meta), offset); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	for index := range bitmap.segments {
		pageID := bitmap.segmentPageID(index)
		if _, err := file.WriteAt(bitmap.encodeSegment(index), int64(pageID)*pageSize); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	root := btree.NewLeafNode(firstTreePageID)
	if err := btree.WriteNode(io.NewOffsetWriter(file, int64(firstTreePageID)*pageSize), root, pageSize, true); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Truncate(int64(highPage+1) * pageSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("second-segment"))
		if err != nil {
			return err
		}
		if got := bucket.rootNode.PageID(); got != secondPage {
			return fmt.Errorf("bucket root page: got %d, want %d", got, secondPage)
		}
		return bucket.Put([]byte("key"), []byte("value"))
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	stats, err := db.Stats()
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if stats.PagesReused != 1 {
		_ = db.Close()
		t.Fatalf("reused pages: got %d, want 1", stats.PagesReused)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	readOnly, err := Open(path, 0600, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	value, err := readOnly.Get([]byte("second-segment"), []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("value")) {
		t.Fatalf("reopened value: got %q, want value", value)
	}
}

func TestDeleteWALFailureKeepsTreeAndAllocationState(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "database"), 0600, &Options{Synchronous: SyncFull})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.wal.SetSyncFileForTesting(nil)
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	createBucket(t, db, testBucketName)
	const entryCount = 128
	value := bytes.Repeat([]byte("v"), 256)
	for index := range entryCount {
		if err := db.Put(testBucketName, fmt.Appendf(nil, "key-%04d", index), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.checkpointWAL(); err != nil {
		t.Fatal(err)
	}
	allocationBefore := slices.Clone(db.allocation.encodeSegment(0))
	lastPageBefore := db.meta.LastPage()
	wantErr := errors.New("WAL sync failed")
	db.wal.SetSyncFileForTesting(func() error { return wantErr })
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		for index := range entryCount {
			if err := bucket.Delete(fmt.Appendf(nil, "key-%04d", index)); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("delete commit: got %v, want %v", err, wantErr)
	}
	db.wal.SetSyncFileForTesting(nil)
	if !bytes.Equal(db.allocation.encodeSegment(0), allocationBefore) {
		t.Fatal("failed delete commit changed allocation state")
	}
	if db.meta.LastPage() != lastPageBefore {
		t.Fatalf("last page after failed delete: got %d, want %d", db.meta.LastPage(), lastPageBefore)
	}
	for index := range entryCount {
		if _, err := db.Get(testBucketName, fmt.Appendf(nil, "key-%04d", index)); err != nil {
			t.Fatalf("read key %d after failed delete: %v", index, err)
		}
	}
}

func TestOpenRejectsCorruptAllocationSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(firstAllocationSegmentID)*pageSize + pageSize - 1
	if _, err := file.WriteAt([]byte{0xff}, offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	opened, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if opened != nil {
		_ = opened.Close()
		t.Fatal("Open accepted a damaged allocation page")
	}
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("Open error: got %v, want ErrChecksum", err)
	}
}

func TestOpenRejectsLastPageBeyondFileAndWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	meta := page.NewMeta(pageSize)
	meta.SetLastPage(^page.ID(0))
	meta.RefreshChecksum()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, pageSize} {
		if _, err := file.WriteAt(page.EncodeMeta(meta), offset); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	opened, err := Open(path, 0600, &Options{ReadOnly: true})
	if opened != nil {
		_ = opened.Close()
		t.Fatal("Open accepted an unavailable last page")
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Open error: got %v, want ErrInvalid", err)
	}
}

func TestBucketDeleteIsVisibleInsideTransactionAndAfterCommit(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "database"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.Bucket(testBucketName)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("key"), []byte("value")); err != nil {
			return err
		}
		if err := bucket.Delete([]byte("key")); err != nil {
			return err
		}
		if _, err := bucket.Get([]byte("key")); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("read after delete in transaction: got %v, want ErrKeyNotFound", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(testBucketName, []byte("key")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("read after commit: got %v, want ErrKeyNotFound", err)
	}
}
