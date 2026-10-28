package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
	"github.com/Issaminu/kvlite/internal/wal"
)

// validatePageOwnership walks every tree page to test the bitmap against the tree.
// Normal Open checks only bitmap pages, so this test helper can find an incorrect bit on a child page.
func (db *DB) validatePageOwnership() error {
	return db.validatePageOwnershipWith(db.readNode)
}

// validatePageOwnershipWith is validatePageOwnership with its own page reader.
func (db *DB) validatePageOwnershipWith(readNode func(page.ID) (*btree.Node, error)) error {
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

		node, err := readNode(pageID)
		if err != nil {
			return err
		}
		for _, id := range node.OverflowPages() {
			if _, exists := visited[id]; exists {
				return fmt.Errorf("continuation page %d has more than one owner: %w", id, ErrInvalid)
			}
			if !db.allocation.allocated(id) {
				return fmt.Errorf("reachable continuation page %d is free: %w", id, ErrInvalid)
			}
			visited[id] = struct{}{}
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
	bitmap := newAllocationBitmap(4096)
	id := page.NewMeta(4096).Root() + 1
	changes := newAllocationChanges(bitmap)
	changes.markAllocated(id)
	if got := changes.pageRecords(); len(got) != 1 || got[0].PageID != firstAllocationSegmentID || !bytes.Equal(got[0].Payload, changes.segment(0).data) {
		t.Fatalf("changed allocation pages: got %+v", got)
	}
	if got := newAllocationChanges(bitmap).pageRecords(); len(got) != 0 {
		t.Fatalf("unchanged allocation pages: got %+v", got)
	}
	changes.release(id)
	if got := changes.pageRecords(); len(got) != 0 {
		t.Fatalf("restored allocation page: got %+v", got)
	}
}

func TestAllocationChangesCloneIsIndependent(t *testing.T) {
	bitmap := newAllocationBitmap(4096)
	first := page.NewMeta(4096).Root() + 1
	changes := newAllocationChanges(bitmap)
	changes.markAllocated(first)

	clone := changes.clone()
	clone.markAllocated(first + 1)

	if bitmap.allocated(first) {
		t.Fatal("private allocation changed the committed bitmap")
	}
	if changes.allocated(first + 1) {
		t.Fatal("cloned allocation changed its parent")
	}
	if !clone.allocated(first + 1) {
		t.Fatal("clone did not record its allocation")
	}
}

func TestAllocationChangesReusesFreePageAndRetiresTailPage(t *testing.T) {
	bitmap := newAllocationBitmap(4096)
	first := page.NewMeta(4096).Root() + 1
	bitmap.markAllocated(first)
	bitmap.markAllocated(first + 1)
	changes := newAllocationChanges(bitmap)
	changes.release(first)

	if got, err := changes.allocate(first + 1); err != nil || got != first {
		t.Fatalf("reused page: got %d, error %v; want page %d", got, err, first)
	}
	if changes.pagesReused != 1 {
		t.Fatalf("reused pages: got %d, want 1", changes.pagesReused)
	}
	if err := changes.retire(first + 1); err != nil {
		t.Fatal(err)
	}
	if changes.pagesRetired != 1 {
		t.Fatalf("retired pages: got %d, want 1", changes.pagesRetired)
	}
	meta := page.NewMeta(4096)
	meta.SetLastPage(first + 1)
	if !changes.finalize(meta) {
		t.Fatal("finalize did not lower the last page")
	}
	if got := meta.LastPage(); got != first {
		t.Fatalf("last page: got %d, want %d", got, first)
	}
	if changes.tailPagesRemoved != 1 {
		t.Fatalf("removed tail pages: got %d, want 1", changes.tailPagesRemoved)
	}
	if changes.allocated(first + 1) {
		t.Fatal("retired tail page is still allocated")
	}
}

func TestAllocationFinalizeTracksRemovedTailSegment(t *testing.T) {
	const pageSize = 4096
	bitmap := newAllocationBitmap(pageSize)
	bitmap.addSegment()
	firstRemoved := bitmap.segmentsPerGroup()
	head := bitmap.segmentPageID(firstRemoved)
	highest := head + page.ID(bitmap.segmentsPerGroup())
	bitmap.markAllocated(highest)
	changes := newAllocationChanges(bitmap)
	if err := changes.retire(highest); err != nil {
		t.Fatal(err)
	}
	meta := page.NewMeta(pageSize)
	meta.SetLastPage(highest)
	changes.finalize(meta)
	for index := firstRemoved; index < len(bitmap.segments); index++ {
		id := bitmap.segmentPageID(index)
		if _, found := changes.obsoleteWALPages[id]; !found {
			t.Fatalf("removed allocation page %d was not recorded", id)
		}
	}
	if got := meta.LastPage(); got != meta.Root() {
		t.Fatalf("last page: got %d, want root %d", got, meta.Root())
	}
	clone := changes.clone()
	delete(clone.obsoleteWALPages, head)
	if _, found := changes.obsoleteWALPages[head]; !found {
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
	if len(bitmap.segments) != bitmap.segmentsPerGroup() || bitmap.allocatedPageCount() != uint64(page.AllocationSegmentPages(db.meta.PageSize()))+3 {
		t.Fatalf("new database allocation: %d segments, %d used pages", len(bitmap.segments), bitmap.allocatedPageCount())
	}
	if bitmap.segments[0].firstFreeBit != db.meta.Root()+1 {
		t.Fatalf("first free bit: got %d, want %d", bitmap.segments[0].firstFreeBit, db.meta.Root()+1)
	}
}

func TestReadAllocationBitmapReadsOnlySegmentPages(t *testing.T) {
	const pageSize = int64(4096)
	file, err := os.Create(filepath.Join(t.TempDir(), "database"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	bitmap := newAllocationBitmap(pageSize)
	bitmap.addSegment()
	meta := page.NewMeta(pageSize)
	lastIndex := len(bitmap.segments) - 1
	meta.SetLastPage(bitmap.segmentPageID(lastIndex))
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
	if len(loaded.segments) != 2*bitmap.segmentsPerGroup() || !loaded.allocated(meta.LastPage()) {
		t.Fatalf("loaded allocation: %d segments, last page %d allocated: %t", len(loaded.segments), meta.LastPage(), loaded.allocated(meta.LastPage()))
	}

	valid := bitmap.encodeSegment(lastIndex)
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
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	meta := page.NewMeta(db.meta.PageSize())
	meta.SetLastPage(meta.Root() + 1)
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
	db, err := Open(path, 0600, &Options{Synchronous: SyncNormal})
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
	}{{"SyncNone", SyncNone}, {"SyncNormal", SyncNormal}, {"SyncFull", SyncFull}} {
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
			if test.mode == SyncNormal {
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
	// The next logical frame is in the same physical bitmap group.
	secondPage := bitmap.pagesPerSegment()
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
	root := btree.NewLeafNode(meta.Root())
	if err := btree.WriteNode(io.NewOffsetWriter(file, int64(meta.Root())*pageSize), root, pageSize, true, false); err != nil {
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
	path := filepath.Join(t.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncFull})
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
	db = reopenAfterWALWriteFailure(t, db, path, &Options{Synchronous: SyncFull})
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

func TestHighestSetBitMatchesBitWalk(t *testing.T) {
	check := func(data []byte) {
		t.Helper()
		want := -1
		for index, value := range data {
			for bit := range 8 {
				if value&(1<<bit) != 0 {
					want = index*8 + bit
				}
			}
		}
		got, found := highestSetBit(data)
		if found != (want >= 0) || found && got != want {
			t.Fatalf("length=%d: got (%d, %t), want bit %d", len(data), got, found, want)
		}
	}
	check(nil)
	for length := 0; length <= 65; length++ {
		data := make([]byte, length)
		check(data)
		for bit := range length * 8 {
			data[bit/8] |= 1 << (bit % 8)
			check(data)
			data[0] |= 1
			check(data)
			clear(data)
		}
	}
	for _, length := range []int{4080, 4081, 4096, 4097, 16368} {
		data := make([]byte, length)
		check(data)
		for _, bit := range []int{0, 1, 7, 8, 63, 64, length * 4, length*8 - 1} {
			data[bit/8] |= 1 << (bit % 8)
			check(data)
			clear(data)
		}
		for index := range data {
			data[index] = 0xff
		}
		check(data)
	}
}

func TestHighestAllocatedMatchesOverlayBitWalk(t *testing.T) {
	for _, pageSize := range []int64{64, 128, 4096} {
		t.Run(fmt.Sprint(pageSize), func(t *testing.T) {
			bitmap := &allocationBitmap{pageSize: pageSize, groupSegments: 1, segments: make([]allocationSegment, 3)}
			for index := range bitmap.segments {
				bitmap.segments[index].data = make([]byte, pageSize)
			}
			pages := bitmap.pagesPerSegment()
			changes := newAllocationChanges(bitmap)
			walk := func(count int, segment func(int) *allocationSegment) page.ID {
				for index := count - 1; index >= 0; index-- {
					data := segment(index).bitmapBits()
					for bit := len(data)*8 - 1; bit >= 0; bit-- {
						if data[bit/8]&(1<<uint(bit%8)) != 0 {
							return page.ID(index)*pages + page.ID(bit)
						}
					}
				}
				return page.Meta1ID
			}
			check := func() {
				t.Helper()
				if got, want := bitmap.highestAllocated(), walk(len(bitmap.segments), func(index int) *allocationSegment { return &bitmap.segments[index] }); got != want {
					t.Fatalf("committed highest=%d, want %d", got, want)
				}
				if got, want := changes.highestAllocated(), walk(changes.segmentCount, changes.segment); got != want {
					t.Fatalf("private highest=%d, want %d", got, want)
				}
			}
			check()
			for _, id := range []page.ID{0, 1, pages - 1, pages, pages + 1, 2*pages - 1} {
				bitmap.markAllocated(id)
				check()
			}
			changes = newAllocationChanges(bitmap)
			clear(changes.writableSegment(1).bitmapBits())
			check()
			if got := bitmap.highestAllocated(); got != 2*pages-1 {
				t.Fatalf("private clear changed committed highest: %d", got)
			}
			for _, id := range []page.ID{2 * pages, 2*pages + 63, 3*pages - 1} {
				changes.markAllocated(id)
				check()
			}
			changes.segmentCount = 2
			check()
			changes.segmentCount = 4
			changes.dirtySegments[3] = &allocationSegment{data: make([]byte, pageSize)}
			changes.markAllocated(4*pages - 1)
			check()
		})
	}
}

func TestAllocationSpanAvoidsReservedStorage(t *testing.T) {
	bitmap := newAllocationBitmap(4096)
	changes := newAllocationChanges(bitmap)
	last := page.NewMeta(bitmap.pageSize).LastPage()
	head, err := changes.allocateSpan(last, 4)
	if err != nil {
		t.Fatal(err)
	}
	for step := page.ID(0); step < 4; step++ {
		if !changes.allocated(head+step) || changes.isSegmentPage(head+step) {
			t.Fatal("span uses free or reserved storage")
		}
	}
	if err := changes.retire(head + 1); err != nil {
		t.Fatal(err)
	}
	other, err := changes.allocate(head + 3)
	if err != nil || other == head+1 {
		t.Fatalf("retired page reused before publication: %d, %v", other, err)
	}
	for index := range bitmap.segments {
		for at := range bitmap.segments[index].bitmapBits() {
			bitmap.segments[index].bitmapBits()[at] = 0xff
		}
		bitmap.segments[index].firstFreeBit = bitmap.pagesPerSegment()
	}
	changes = newAllocationChanges(bitmap)
	boundary := bitmap.pagesPerGroup()
	head, err = changes.allocateSpan(boundary-2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if head != boundary+page.AllocationSegmentPages(bitmap.pageSize) {
		t.Fatalf("span head=%d crosses allocation storage", head)
	}
	for step := page.ID(0); step < page.AllocationSegmentPages(bitmap.pageSize); step++ {
		if !changes.allocated(boundary+step) || !changes.isSegmentPage(boundary+step) {
			t.Fatal("new allocation span is not reserved")
		}
	}
}

func TestAllocationSpanSearchReusesEarlierSpace(t *testing.T) {
	for _, test := range []struct {
		name       string
		firstCount int
		nextCount  int
		holeStart  page.ID
		holeCount  int
		freePage   page.ID
		want       page.ID
	}{
		{"freed page completes earlier span", 6, 6, 20, 5, 25, 20},
		{"smaller request fits earlier gap", 6, 5, 20, 5, 0, 20},
		{"freed page completes large span", 128, 128, 20, 127, 147, 20},
		{"search start cannot be negative", 128, 128, 2, 127, 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			bitmap := newAllocationBitmap(16384)
			root := page.NewMeta(bitmap.pageSize).Root()
			last := root + 512
			for id := root + 1; id <= last; id++ {
				bitmap.markAllocated(id)
			}
			changes := newAllocationChanges(bitmap)
			for offset := 0; offset < test.holeCount; offset++ {
				changes.release(root + test.holeStart + page.ID(offset))
			}
			head, err := changes.allocateSpan(last, test.firstCount)
			if err != nil || head != last+1 {
				t.Fatalf("initial span: got %d, error %v; want %d", head, err, last+1)
			}
			last = head + page.ID(test.firstCount) - 1
			if test.freePage != 0 {
				changes.release(root + test.freePage)
			}
			head, err = changes.allocateSpan(last, test.nextCount)
			if err != nil || head != root+test.want {
				t.Fatalf("earlier span: got %d, error %v; want %d", head, err, root+test.want)
			}
		})
	}

	t.Run("later release keeps earlier search position", func(t *testing.T) {
		bitmap := newAllocationBitmap(16384)
		root := page.NewMeta(bitmap.pageSize).Root()
		last := root + 512
		for id := root + 1; id <= last; id++ {
			bitmap.markAllocated(id)
		}
		changes := newAllocationChanges(bitmap)
		for _, start := range []page.ID{root + 20, root + 40} {
			for offset := page.ID(0); offset < 6; offset++ {
				changes.release(start + offset)
			}
		}
		head, err := changes.allocateSpan(last, 6)
		if err != nil || head != root+20 {
			t.Fatalf("first span: got %d, error %v; want %d", head, err, root+20)
		}
		changes.release(root + 400)
		head, err = changes.allocateSpan(last, 6)
		if err != nil || head != root+40 {
			t.Fatalf("next span: got %d, error %v; want %d", head, err, root+40)
		}
	})
}

func TestAllocationFramesUseIndependentPageImages(t *testing.T) {
	bitmap := newAllocationBitmap(4096)
	if len(bitmap.segments) != bitmap.segmentsPerGroup() {
		t.Fatal("initial bitmap group has an incomplete frame count")
	}
	for index := range bitmap.segments {
		if len(bitmap.segments[index].data) != int(bitmap.pageSize) {
			t.Fatal("bitmap frame is not one page")
		}
		if err := verifyAllocationPage(bitmap.encodeSegment(index), bitmap.pageSize, bitmap.segmentPageID(index)); err != nil {
			t.Fatal(err)
		}
	}
	original := slices.Clone(bitmap.segments[1].data)
	changes := newAllocationChanges(bitmap)
	changes.markAllocated(bitmap.pagesPerSegment() + 7)
	records := changes.pageRecords()
	if len(records) != 1 || records[0].PageID != bitmap.segmentPageID(1) || len(records[0].Payload) != int(bitmap.pageSize) {
		t.Fatal("one changed bitmap bit did not produce exactly one page image")
	}
	if !bytes.Equal(bitmap.segments[1].data, original) || &changes.segment(1).data[0] == &bitmap.segments[1].data[0] {
		t.Fatal("private frame change modified or borrowed its committed frame")
	}
	if err := verifyAllocationPage(records[0].Payload, bitmap.pageSize, records[0].PageID); err != nil {
		t.Fatal(err)
	}
	if err := verifyAllocationPage(append(slices.Clone(records[0].Payload), records[0].Payload...), bitmap.pageSize, records[0].PageID); !errors.Is(err, page.ErrInvalid) {
		t.Fatal("multi-page allocation record was accepted")
	}
}

func TestAllocationFinalizeKeepsInteriorRetirementAndReclaimsTail(t *testing.T) {
	bitmap := newAllocationBitmap(4096)
	meta := page.NewMeta(bitmap.pageSize)
	root := meta.Root()
	for offset := page.ID(1); offset <= 3; offset++ {
		bitmap.markAllocated(root + offset)
	}
	meta.SetLastPage(root + 3)
	changes := newAllocationChanges(bitmap)
	if err := changes.retire(root + 1); err != nil {
		t.Fatal(err)
	}
	if changes.finalize(meta) || meta.LastPage() != root+3 {
		t.Fatal("interior retirement moved the live data tail")
	}
	if changes.allocated(root+1) || !changes.allocated(root+3) {
		t.Fatal("interior retirement changed the wrong allocation bits")
	}
	if !bitmap.allocated(root + 1) {
		t.Fatal("private retirement changed committed allocation")
	}
	bitmap = changes.publish()
	changes = newAllocationChanges(bitmap)
	if err := changes.retire(root + 3); err != nil {
		t.Fatal(err)
	}
	if !changes.finalize(meta) || meta.LastPage() != root+2 {
		t.Fatal("retired data tail was not reclaimed")
	}
	if changes.tailPagesRemoved != 1 {
		t.Fatalf("reclaimed tail=%d", changes.tailPagesRemoved)
	}
}

func TestAllocationSegmentFitsMaximumNode(t *testing.T) {
	for _, size := range []int64{64, 128, 512, 4096, 16384, 65536} {
		span := int64(page.AllocationSegmentPages(size))
		available := span*(size-allocationHeaderSize)*8 - span - 2
		needed := (int64(btree.MaxValueSize) + int64(btree.MaxKeySize) + 128 + size - 1) / size
		if span <= 0 || available < needed {
			t.Fatalf("page size %d leaves %d pages, need %d", size, available, needed)
		}
	}
}

func TestAllocationNodeSpanCrossesLogicalFrame(t *testing.T) {
	bitmap := newAllocationBitmap(4096)
	boundary := bitmap.pagesPerSegment()
	for index := range bitmap.segments[0].bitmapBits() {
		bitmap.segments[0].bitmapBits()[index] = 0xff
	}
	clearAllocationBit(bitmap.segments[0].bitmapBits(), boundary-2)
	clearAllocationBit(bitmap.segments[0].bitmapBits(), boundary-1)
	bitmap.segments[0].firstFreeBit = boundary - 2
	bitmap.markAllocated(boundary + 2)
	changes := newAllocationChanges(bitmap)
	head, err := changes.allocateSpan(boundary+2, 4)
	if err != nil || head != boundary-2 {
		t.Fatalf("cross-frame span=%d error=%v", head, err)
	}
	for offset := page.ID(0); offset < 4; offset++ {
		if !changes.allocated(head+offset) || changes.isSegmentPage(head+offset) {
			t.Fatal("cross-frame span has invalid ownership")
		}
	}
	if changes.pagesReused != 4 || len(changes.dirtySegments) != 2 {
		t.Fatal("cross-frame allocation did not update both logical frames")
	}
	bitmap.markAllocated(boundary - 2)
	bitmap.markAllocated(boundary - 1)
	grow := newAllocationChanges(bitmap)
	if !grow.extendSpan(boundary-2, 2, 4, boundary+2) {
		t.Fatal("logical frame boundary blocked in-place growth")
	}
	if len(grow.dirtySegments) != 1 || !grow.allocated(boundary+1) {
		t.Fatal("growth did not limit copy-on-write to the changed frame")
	}
}

func TestNativeUnchangedSpansStillFinalizeRetirement(t *testing.T) {
	db := OpenMemory()
	defer db.Close()
	var retired page.ID
	if err := db.Update(func(tx *Tx) error {
		data, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		if err := data.Put([]byte("key"), []byte("old")); err != nil {
			return err
		}
		temporary, err := tx.CreateBucket([]byte("temporary"))
		if err != nil {
			return err
		}
		retired = temporary.rootNode.PageID()
		_, err = tx.CreateBucket([]byte("tail"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	last, root := db.meta.LastPage(), db.meta.Root()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.DeleteBucket([]byte("temporary")); err != nil {
			return err
		}
		data, err := tx.Bucket([]byte("data"))
		if err != nil {
			return err
		}
		return data.Put([]byte("key"), []byte("new"))
	}); err != nil {
		t.Fatal(err)
	}
	if db.allocation.allocated(retired) || db.meta.LastPage() != last || db.meta.Root() != root {
		t.Fatal("unchanged-span commit lost retirement or moved the live tail")
	}
	got, err := db.Get([]byte("data"), []byte("key"))
	if err != nil || string(got) != "new" {
		t.Fatalf("updated value=%q error=%v", got, err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeNodeSpanRepairsNestedRootAndKeepsRollbackPrivate(t *testing.T) {
	db := OpenMemory()
	defer db.Close()
	var oldHead page.ID
	if err := db.Update(func(tx *Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.CreateBucket([]byte("child"))
		if err != nil {
			return err
		}
		oldHead = child.rootNode.PageID()
		if err := child.Put([]byte("key"), []byte("old")); err != nil {
			return err
		}
		_, err = tx.CreateBucket([]byte("blocker"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !db.allocation.allocated(oldHead + 1) {
		t.Fatal("fixture did not block in-place growth")
	}
	original := db.memoryNodes[oldHead]
	value := bytes.Repeat([]byte{'v'}, int(db.meta.PageSize())*2)
	change := func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.Bucket([]byte("child"))
		if err != nil {
			return err
		}
		return child.Put([]byte("key"), value)
	}
	failure := errors.New("callback rollback")
	if err := db.Update(func(tx *Tx) error {
		if err := change(tx); err != nil {
			return err
		}
		return failure
	}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if original.PageID() != oldHead || string(original.EntryAt(0).Value()) != "old" {
		t.Fatal("failed callback changed committed node")
	}
	if err := db.Update(change); err != nil {
		t.Fatal(err)
	}
	if original.PageID() != oldHead || string(original.EntryAt(0).Value()) != "old" {
		t.Fatal("relocation changed the original node")
	}
	if db.allocation.allocated(oldHead) {
		t.Fatal("old span stayed allocated")
	}
	if err := db.View(func(tx *Tx) error {
		parent, err := tx.Bucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.Bucket([]byte("child"))
		if err != nil {
			return err
		}
		if err := child.loadRootNode(); err != nil {
			return err
		}
		if child.rootNode.PageID() == oldHead {
			return errors.New("nested root pointer was not repaired")
		}
		if child.rootNode.StoragePageCount() != btree.NodePageCount(child.rootNode, tx.meta.PageSize()) {
			return errors.New("node span count differs")
		}
		got, err := child.Get([]byte("key"))
		if err != nil {
			return err
		}
		if !bytes.Equal(got, value) {
			return errors.New("relocated value differs")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeNodeSpanMovesCatalogRoot(t *testing.T) {
	db := OpenMemory()
	defer db.Close()
	oldRoot := db.rootNode
	name := bytes.Repeat([]byte{'n'}, btree.MaxKeySize)
	if err := db.Update(func(tx *Tx) error { _, err := tx.CreateBucket(name); return err }); err != nil {
		t.Fatal(err)
	}
	if db.meta.Root() == oldRoot.PageID() || db.rootNode.PageID() != db.meta.Root() {
		t.Fatal("catalog relocation did not publish its metadata root")
	}
	if oldRoot.PageID() != firstAllocationSegmentID+page.AllocationSegmentPages(db.meta.PageSize()) || oldRoot.EntryCount() != 0 {
		t.Fatal("catalog relocation changed the committed original")
	}
	if err := db.View(func(tx *Tx) error { _, err := tx.Bucket(name); return err }); err != nil {
		t.Fatal(err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeNodeSpanBatchParentsStayPrivate(t *testing.T) {
	db := OpenMemory()
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("key"), []byte("old")); err != nil {
			return err
		}
		_, err = tx.CreateBucket([]byte("blocker"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	first := newTx(db, false)
	bucket, err := first.Bucket([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	oldHead := bucket.rootNode.PageID()
	value := bytes.Repeat([]byte{'a'}, int(db.meta.PageSize())*2)
	if err := bucket.Put([]byte("key"), value); err != nil {
		t.Fatal(err)
	}
	second := newWriteTx(db, first.meta, first.rootNode, first.store.dirty, first.allocation)
	other, err := second.Bucket([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Put([]byte("key"), bytes.Repeat([]byte{'b'}, len(value))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.store.dirty[oldHead].final.EntryAt(0).Value(), value) {
		t.Fatal("later failed callback changed an earlier image")
	}
	prepared := &Tx{db: db, meta: first.meta, rootNode: first.rootNode, allocation: first.allocation, allocationOwned: first.allocationOwned}
	prepared.store.dirty = first.store.dirty
	if err := prepared.prepareCommit(); err != nil {
		t.Fatal(err)
	}
	probe := newWriteTx(db, prepared.meta, prepared.rootNode, prepared.store.dirty, prepared.allocation)
	gotBucket, err := probe.Bucket([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := gotBucket.Get([]byte("key"))
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("prepared batch lost its parent reference: %v", err)
	}
	if gotBucket.rootNode.PageID() == oldHead {
		t.Fatal("fixture did not relocate the bucket root")
	}
	if prepared.store.dirty[gotBucket.rootNode.PageID()].original != nil {
		t.Fatal("relocated node kept a patch base")
	}
	if string(db.memoryNodes[oldHead].EntryAt(0).Value()) != "old" {
		t.Fatal("batch preparation changed committed state")
	}
}

func TestNativeNodeSpanRepairsUnchangedBranch(t *testing.T) {
	db := OpenMemory()
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		for index := range 12 {
			if err := bucket.Put(fmt.Appendf(nil, "%03d", index), bytes.Repeat([]byte{'s'}, int(tx.meta.PageSize())/2)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var oldHead page.ID
	var branch *btree.Node
	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket([]byte("data"))
		if err != nil {
			return err
		}
		if err := bucket.loadRootNode(); err != nil {
			return err
		}
		branch = bucket.rootNode
		if branch.IsLeaf() {
			return errors.New("fixture has no branch")
		}
		oldHead = branch.Children[0]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !db.allocation.allocated(oldHead + 1) {
		t.Fatal("fixture did not block the leaf span")
	}
	value := bytes.Repeat([]byte{'g'}, int(db.meta.PageSize())*2)
	if err := db.Put([]byte("data"), []byte("000"), value); err != nil {
		t.Fatal(err)
	}
	if branch.Children[0] != oldHead {
		t.Fatal("repair changed a committed branch")
	}
	got, err := db.Get([]byte("data"), []byte("000"))
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("branch child after move: %v", err)
	}
	if db.allocation.allocated(oldHead) {
		t.Fatal("moved leaf head remained allocated")
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeNodeSpanGrowsAndShrinksInPlace(t *testing.T) {
	db := OpenMemory()
	defer db.Close()
	name, key := []byte("data"), []byte("key")
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(name)
		if err != nil {
			return err
		}
		return bucket.Put(key, []byte("old"))
	}); err != nil {
		t.Fatal(err)
	}
	var head page.ID
	if err := db.View(func(tx *Tx) error {
		bucket, err := tx.Bucket(name)
		if err != nil {
			return err
		}
		if err := bucket.loadRootNode(); err != nil {
			return err
		}
		head = bucket.rootNode.PageID()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	original := db.memoryNodes[head]
	grown := bytes.Repeat([]byte("v"), int(db.meta.PageSize())*2)
	if err := db.Put(name, key, grown); err != nil {
		t.Fatal(err)
	}
	node := db.memoryNodes[head]
	if node == nil || node.StoragePageCount() != 3 || db.meta.LastPage() != head+2 {
		t.Fatal("growth did not extend the existing span")
	}
	if string(original.EntryAt(0).Value()) != "old" || original.StoragePageCount() != 1 {
		t.Fatal("growth changed the committed original")
	}
	for offset := page.ID(0); offset < 3; offset++ {
		if !db.allocation.allocated(head + offset) {
			t.Fatal("grown span contains a free page")
		}
	}
	if err := db.Put(name, key, []byte("small")); err != nil {
		t.Fatal(err)
	}
	node = db.memoryNodes[head]
	if node == nil || node.StoragePageCount() != 1 || db.meta.LastPage() != head {
		t.Fatal("shrink did not retain the head and reclaim the tail")
	}
	if db.allocation.allocated(head+1) || db.allocation.allocated(head+2) {
		t.Fatal("surplus pages remain allocated")
	}
	got, err := db.Get(name, key)
	if err != nil || string(got) != "small" {
		t.Fatalf("shrunken value=%q error=%v", got, err)
	}
	if err := db.validatePageOwnership(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeNodeSpanBatchParentChanges(t *testing.T) {
	for _, name := range []string{"split", "split-large-keys", "root-collapse", "split-then-delete"} {
		t.Run(name, func(t *testing.T) {
			db := OpenMemory()
			defer db.Close()
			pageSize := int(db.meta.PageSize())
			keySize := pageSize / 2
			if name == "split-large-keys" {
				keySize = btree.MaxKeySize
			}
			key := func(number int) []byte {
				return append(fmt.Appendf(nil, "%03d", number), bytes.Repeat([]byte{'k'}, keySize-3)...)
			}
			if err := db.Update(func(tx *Tx) error {
				bucket, err := tx.CreateBucket([]byte("data"))
				if err != nil {
					return err
				}
				for _, number := range []int{100, 200, 300} {
					if err := bucket.Put(key(number), bytes.Repeat([]byte{'s'}, pageSize/4)); err != nil {
						return err
					}
				}
				_, err = tx.CreateBucket([]byte("blocker"))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			first := newTx(db, false)
			bucket, err := first.Bucket([]byte("data"))
			if err != nil {
				t.Fatal(err)
			}
			if bucket.rootNode.IsLeaf() || len(bucket.rootNode.Children) != 3 {
				t.Fatal("fixture needs one branch with three leaf children")
			}
			oldHead := bucket.rootNode.Children[2]
			value := bytes.Repeat([]byte{'v'}, pageSize*2)
			if err := bucket.Put(key(300), value); err != nil {
				t.Fatal(err)
			}
			oldParent := first.store.dirty[oldHead].parent
			if oldParent == nil || oldParent.IsLeaf() {
				t.Fatal("first callback did not retain the leaf's branch parent")
			}

			second := newWriteTx(db, first.meta, first.rootNode, first.store.dirty, first.allocation)
			other, err := second.Bucket([]byte("data"))
			if err != nil {
				t.Fatal(err)
			}
			if name == "root-collapse" {
				for _, number := range []int{100, 200} {
					if err := other.Delete(key(number)); err != nil {
						t.Fatal(err)
					}
				}
			} else if err := other.Put(key(150), bytes.Repeat([]byte{'n'}, pageSize/4)); err != nil {
				t.Fatal(err)
			}
			parent := second.store.dirty[oldHead].parent
			if parent == nil || parent.PageID() == oldParent.PageID() {
				t.Fatal("later callback did not replace the saved parent")
			}
			if first.store.dirty[oldHead].parent != oldParent {
				t.Fatal("later callback changed the earlier callback's parent record")
			}
			if parent.IsLeaf() != (name == "root-collapse") {
				t.Fatal("saved parent has the wrong node type")
			}
			updated := bytes.Repeat([]byte{'w'}, len(value))
			if name == "split-then-delete" {
				if err := other.Delete(key(300)); err != nil {
					t.Fatal(err)
				}
			} else if err := other.Put(key(300), updated); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first.store.dirty[oldHead].final.EntryAt(0).Value(), value) {
				t.Fatal("a write after reparenting changed an earlier callback's image")
			}
			for id, change := range second.store.dirty {
				first.store.dirty[id] = change
			}
			prepared := &Tx{db: db, meta: second.meta, rootNode: second.rootNode, allocation: second.allocation, allocationOwned: second.allocationOwned}
			prepared.store.dirty = first.store.dirty
			if err := prepared.prepareCommit(); err != nil {
				t.Fatal(err)
			}
			if _, exists := prepared.store.dirty[oldHead]; exists {
				t.Fatal("fixture did not relocate the changed leaf")
			}
			probe := newWriteTx(db, prepared.meta, prepared.rootNode, prepared.store.dirty, prepared.allocation)
			visible, err := probe.Bucket([]byte("data"))
			if err != nil {
				t.Fatal(err)
			}
			got, err := visible.Get(key(300))
			if name == "split-then-delete" {
				if !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("deleted key after reparenting: %v", err)
				}
			} else if err != nil || !bytes.Equal(got, updated) {
				t.Fatalf("relocated leaf through its new parent: %v", err)
			}
			view := &DB{meta: prepared.meta, allocation: prepared.allocation.publish()}
			if err := view.validatePageOwnershipWith(probe.store.ReadNode); err != nil {
				t.Fatal(err)
			}
			if got := db.memoryNodes[oldHead].EntryAt(0).Value(); !bytes.Equal(got, bytes.Repeat([]byte{'s'}, pageSize/4)) {
				t.Fatal("preparation changed the committed leaf")
			}
		})
	}
}

func TestNativeNodeSpanBatchCatalogRootCollapse(t *testing.T) {
	db := OpenMemory()
	defer db.Close()
	key := func(number, size int) []byte {
		return append(fmt.Appendf(nil, "%03d", number), bytes.Repeat([]byte{'k'}, size-3)...)
	}
	keySize := int(db.meta.PageSize()) / 2
	if err := db.Update(func(tx *Tx) error {
		for _, number := range []int{100, 200, 300} {
			if _, err := tx.CreateBucket(key(number, keySize)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first := newTx(db, false)
	remaining := key(250, btree.MaxKeySize)
	if _, err := first.CreateBucket(remaining); err != nil {
		t.Fatal(err)
	}
	if _, err := first.CreateBucket(key(999, keySize)); err != nil {
		t.Fatal(err)
	}
	second := newWriteTx(db, first.meta, first.rootNode, first.store.dirty, first.allocation)
	for _, number := range []int{100, 200, 300, 999} {
		if err := second.DeleteBucket(key(number, keySize)); err != nil {
			t.Fatal(err)
		}
	}
	oldRoot := second.meta.Root()
	if !second.rootNode.IsLeaf() || second.store.dirty[oldRoot].parent != nil {
		t.Fatal("promoted catalog root kept its old branch parent")
	}
	for id, change := range second.store.dirty {
		first.store.dirty[id] = change
	}
	prepared := &Tx{db: db, meta: second.meta, rootNode: second.rootNode, allocation: second.allocation, allocationOwned: second.allocationOwned}
	prepared.store.dirty = first.store.dirty
	if err := prepared.prepareCommit(); err != nil {
		t.Fatal(err)
	}
	if prepared.meta.Root() == oldRoot {
		t.Fatal("fixture did not relocate the promoted catalog root")
	}
	probe := newWriteTx(db, prepared.meta, prepared.rootNode, prepared.store.dirty, prepared.allocation)
	if _, err := probe.Bucket(remaining); err != nil {
		t.Fatal(err)
	}
	view := &DB{meta: prepared.meta, allocation: prepared.allocation.publish()}
	if err := view.validatePageOwnershipWith(probe.store.ReadNode); err != nil {
		t.Fatal(err)
	}
}

// TestAllocationBitmapOwnsNodeSpanChecksAllBits covers byte and word boundaries.
func TestAllocationBitmapOwnsNodeSpanChecksAllBits(t *testing.T) {
	bitmap := newAllocationBitmap(4096)
	start := page.NewMeta(4096).Root() + 1
	for _, count := range []int{1, 2, 7, 8, 9, 17, 63, 64, 65, 128, 257} {
		for offset := page.ID(0); offset < 8; offset++ {
			head := start + offset
			for i := 0; i < count; i++ {
				bitmap.markAllocated(head + page.ID(i))
			}
			if !bitmap.ownsNodeSpan(head, count) {
				t.Fatalf("allocated span %d+%d was rejected", head, count)
			}
			for i := 0; i < count; i++ {
				id := head + page.ID(i)
				segment := bitmap.writableSegment(bitmap.segmentIndex(id))
				clearAllocationBit(segment.bitmapBits(), id%bitmap.pagesPerSegment())
				if bitmap.ownsNodeSpan(head, count) {
					t.Fatalf("span accepted free tail page %d", id)
				}
				setAllocationBit(segment.bitmapBits(), id%bitmap.pagesPerSegment())
			}
		}
	}
	for _, test := range []struct {
		head  page.ID
		count int
	}{{0, 1}, {firstAllocationSegmentID, 1}, {firstAllocationSegmentID + 1, 1}, {bitmap.pagesPerGroup(), 1}, {start, 0}, {^page.ID(0), 1}, {^page.ID(0), 2}, {bitmap.pagesPerGroup() - 1, 2}} {
		if bitmap.ownsNodeSpan(test.head, test.count) {
			t.Fatalf("invalid span %+v was accepted", test)
		}
	}
}

// Data can cross a logical bitmap frame because all metadata frames stay at the group start.
func TestAllocationBitmapOwnsNodeSpanAcrossFrames(t *testing.T) {
	bitmap := newAllocationBitmap(4096)
	head := bitmap.pagesPerSegment() - 7
	for i := 0; i < 20; i++ {
		bitmap.markAllocated(head + page.ID(i))
	}
	if !bitmap.ownsNodeSpan(head, 20) {
		t.Fatal("span across bitmap frames was rejected")
	}
	middle := bitmap.pagesPerSegment() + 3
	segment := bitmap.writableSegment(bitmap.segmentIndex(middle))
	clearAllocationBit(segment.bitmapBits(), middle%bitmap.pagesPerSegment())
	if bitmap.ownsNodeSpan(head, 20) {
		t.Fatal("missing bit in second frame was ignored")
	}
}

func TestReleaseWordMatchesIndividualPages(t *testing.T) {
	for _, size := range []int64{128, 4096} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			base := newAllocationBitmap(size)
			base.addSegment()
			checkReleaseWords(t, base)
		})
	}
	// A component fixture with a 72-bit frame exercises the scalar boundary fallback.
	t.Run("unaligned-frame", func(t *testing.T) {
		base := &allocationBitmap{pageSize: 25, groupSegments: 1}
		base.addSegment()
		base.addSegment()
		base.markAllocated(page.Meta0ID)
		base.markAllocated(page.Meta1ID)
		checkReleaseWords(t, base)
	})
}

func checkReleaseWords(t *testing.T, base *allocationBitmap) {
	t.Helper()
	width := base.pagesPerSegment()
	coverage := width * page.ID(len(base.segments))
	dataStart := firstAllocationSegmentID + page.ID(base.segmentsPerGroup())
	heads := []page.ID{0, 64, (dataStart + 63) / 64 * 64, (width - 1) / 64 * 64,
		width / 64 * 64, base.pagesPerGroup() / 64 * 64, (coverage - 1) / 64 * 64, (coverage + 63) / 64 * 64}
	for _, head := range heads {
		for bit := page.ID(0); bit < 64; bit++ {
			id := head + bit
			if id < coverage && bit%3 != 0 && !base.isSegmentPage(id) {
				base.markAllocated(id)
			}
		}
	}
	snapshot := make([]string, len(base.segments))
	for i := range base.segments {
		snapshot[i] = string(base.segments[i].data)
	}
	for _, head := range heads {
		for _, mask := range []uint64{0, ^uint64(0), 0x8000000000000001, 0x5555555555555555} {
			start := newAllocationChanges(base)
			start.spanCount, start.spanCursor = 17, coverage-1
			left, right := start.clone(), start.clone()
			for repeat := 0; repeat < 2; repeat++ {
				left.releaseWord(head/64, mask)
				for bit := page.ID(0); bit < 64; bit++ {
					if mask&(uint64(1)<<bit) != 0 {
						right.release(head + bit)
					}
				}
				if !reflect.DeepEqual(left, right) {
					t.Fatalf("release differs at head=%d mask=%x repeat=%d", head, mask, repeat)
				}
			}
			metaLeft, metaRight := page.NewMeta(4096), page.NewMeta(4096)
			metaLeft.SetLastPage(coverage - 1)
			metaRight.SetLastPage(coverage - 1)
			for repeat := 0; repeat < 2; repeat++ {
				left.finalize(metaLeft)
				right.finalize(metaRight)
				if !reflect.DeepEqual(left, right) || !reflect.DeepEqual(metaLeft, metaRight) ||
					!reflect.DeepEqual(left.walRetiredPages(), right.walRetiredPages()) {
					t.Fatalf("finalization differs at head=%d mask=%x repeat=%d", head, mask, repeat)
				}
			}
		}
	}
	for i := range base.segments {
		if string(base.segments[i].data) != snapshot[i] {
			t.Fatalf("release changed shared bitmap frame %d", i)
		}
	}
}

func retirementFixture(highest page.ID) *allocationBitmap {
	bitmap := newAllocationBitmap(4096)
	for id := page.NewMeta(4096).Root(); id <= highest; id++ {
		bitmap.markAllocated(id)
	}
	return bitmap
}

func TestRetirementSpanRoundTrip(t *testing.T) {
	for _, test := range []struct{ head, count int }{{63, 2}, {64, 64}, {126, 5}, {192, 130}} {
		t.Run(fmt.Sprintf("%d/%d", test.head, test.count), func(t *testing.T) {
			head, end := page.ID(test.head), page.ID(test.head+test.count)
			base := retirementFixture(end)
			changes := newAllocationChanges(base)
			if err := changes.retireSpan(head, test.count); err != nil {
				t.Fatal(err)
			}
			if changes.pagesRetired != uint64(test.count) {
				t.Fatalf("retirement count: %d", changes.pagesRetired)
			}
			seen := make(map[page.ID]bool)
			changes.forEachRetired(func(id page.ID) {
				if id < head || id >= end || seen[id] {
					t.Fatalf("invalid or repeated page: %d", id)
				}
				seen[id] = true
			})
			if len(seen) != test.count {
				t.Fatalf("visited %d pages", len(seen))
			}
			raw := changes.walRetiredPages()
			for id := head; id < end; id++ {
				if _, ok := raw[id]; !ok || !changes.allocated(id) || !changes.isRetired(id) {
					t.Fatalf("retirement lost ownership or raw page %d", id)
				}
			}
			if err := changes.retire(head); !errors.Is(err, page.ErrInvalid) {
				t.Fatalf("duplicate accepted: %v", err)
			}
			meta := page.NewMeta(4096)
			meta.SetLastPage(end)
			changes.finalize(meta)
			changes.finalize(meta)
			for id := head; id < end; id++ {
				if changes.allocated(id) || !base.allocated(id) {
					t.Fatalf("private release changed base or retained %d", id)
				}
			}
			published := newAllocationChanges(changes.publish())
			if id, err := published.allocate(meta.LastPage()); err != nil || id != head {
				t.Fatalf("reuse after publication: %d, %v", id, err)
			}
		})
	}
}

func TestRetirementCloneAndFailureIsolation(t *testing.T) {
	changes := newAllocationChanges(retirementFixture(100))
	if err := changes.retire(65); err != nil {
		t.Fatal(err)
	}
	trial := changes.clone()
	if err := trial.retireSpan(60, 10); !errors.Is(err, page.ErrInvalid) {
		t.Fatalf("overlap in later word accepted: %v", err)
	}
	if changes.isRetired(60) || changes.pagesRetired != 1 || len(changes.walRetiredPages()) != 1 {
		t.Fatal("failed private retirement changed earlier callback")
	}
	clone := changes.clone()
	if err := clone.retire(66); err != nil {
		t.Fatal(err)
	}
	if err := changes.retire(67); err != nil {
		t.Fatal(err)
	}
	if changes.isRetired(66) || clone.isRetired(67) {
		t.Fatal("same-word updates escaped their callback")
	}
	clone.obsoleteWALPages = map[page.ID]struct{}{firstAllocationSegmentID: {}}
	if raw := clone.walRetiredPages(); len(raw) != 3 {
		t.Fatalf("combined WAL retirement: %+v", raw)
	}
	empty := newAllocationChanges(retirementFixture(100))
	if empty.walRetiredPages() != nil {
		t.Fatal("empty WAL retirement is not nil")
	}
	empty.obsoleteWALPages = map[page.ID]struct{}{firstAllocationSegmentID: {}}
	if raw := empty.walRetiredPages(); len(raw) != 1 {
		t.Fatalf("allocation-only WAL retirement: %+v", raw)
	}
	invalid := newAllocationChanges(newAllocationBitmap(4096))
	for _, span := range []struct {
		head  page.ID
		count int
	}{{firstAllocationSegmentID, 1}, {63, 3}, {64, 0}, {^page.ID(0), 2}} {
		if err := invalid.retireSpan(span.head, span.count); !errors.Is(err, page.ErrInvalid) {
			t.Fatalf("invalid span accepted: %+v, %v", span, err)
		}
	}
}
