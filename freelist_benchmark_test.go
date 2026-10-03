package kvlite

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func BenchmarkOpenPersistentAllocation(b *testing.B) {
	for _, segmentCount := range []int{1, 8, 64, 512} {
		b.Run(fmt.Sprintf("segments-%d", segmentCount), func(b *testing.B) {
			path, logicalBytes := benchmarkAllocationDatabase(b, segmentCount)
			b.ReportAllocs()
			for b.Loop() {
				db, err := Open(path, 0600, &Options{ReadOnly: true})
				if err != nil {
					b.Fatal(err)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(logicalBytes), "logical-B")
			b.ReportMetric(float64(segmentCount), "allocation-pages")
		})
	}
}

func BenchmarkAllocationStateFirstWrite(b *testing.B) {
	for _, segmentCount := range []int{1, 8, 64, 512, 4096} {
		b.Run(fmt.Sprintf("segments-%d", segmentCount), func(b *testing.B) {
			bitmap := benchmarkAllocationBitmap(segmentCount)
			target := page.ID(segmentCount-1)*bitmap.pagesPerSegment() + 1
			if segmentCount == 1 {
				target = firstTreePageID + 1
			}
			b.ReportAllocs()
			for b.Loop() {
				state := newAllocationChanges(bitmap)
				state.markAllocated(target)
			}
		})
	}
}

func benchmarkAllocationBitmap(segmentCount int) *allocationBitmap {
	bitmap := newAllocationBitmap(4096)
	for len(bitmap.segments) < segmentCount {
		bitmap.addSegment()
	}
	return bitmap
}

func benchmarkAllocationDatabase(b *testing.B, segmentCount int) (string, int64) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "database")
	db, err := Open(path, 0600, &Options{Synchronous: SyncNone})
	if err != nil {
		b.Fatal(err)
	}
	pageSize := db.meta.PageSize()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}

	bitmap := newAllocationBitmap(pageSize)
	for len(bitmap.segments) < segmentCount {
		bitmap.addSegment()
	}
	meta := page.NewMeta(pageSize)
	meta.SetLastPage(bitmap.highestAllocated())
	meta.RefreshChecksum()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		b.Fatal(err)
	}
	for _, offset := range []int64{0, pageSize} {
		if _, err := file.WriteAt(page.EncodeMeta(meta), offset); err != nil {
			_ = file.Close()
			b.Fatal(err)
		}
	}
	for index := range bitmap.segments {
		pageID := bitmap.segmentPageID(index)
		if _, err := file.WriteAt(bitmap.encodeSegment(index), int64(pageID)*pageSize); err != nil {
			_ = file.Close()
			b.Fatal(err)
		}
	}
	logicalBytes := int64(meta.LastPage()+1) * pageSize
	if err := file.Truncate(logicalBytes); err != nil {
		_ = file.Close()
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	return path, logicalBytes
}
