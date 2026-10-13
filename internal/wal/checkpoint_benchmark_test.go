package wal

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// BenchmarkRepeatedCheckpoint measures file-backed checkpoints without sync.
// The first checkpoint warms the file and buffers outside the timer.
// Each measured iteration includes restoring the committed page map.
func BenchmarkRepeatedCheckpoint(b *testing.B) {
	for _, count := range []int{1, 256, 512} {
		for _, checksums := range []bool{false, true} {
			b.Run(fmt.Sprintf("pages=%d/checksums=%t", count, checksums), func(b *testing.B) {
				const pageSize = 4096
				dir := b.TempDir()
				main, err := os.Create(filepath.Join(dir, "main"))
				if err != nil {
					b.Fatal(err)
				}
				defer main.Close()
				file, err := os.Create(filepath.Join(dir, "wal"))
				if err != nil {
					b.Fatal(err)
				}
				defer file.Close()
				log := New(Config{File: file, PageSize: pageSize})
				pages := make(map[page.ID]CommittedPage, count)
				for i := range count {
					id := page.ID(3 + i)
					node := btree.NewLeafNode(id)
					if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), make([]byte, 128))); err != nil {
						b.Fatal(err)
					}
					pages[id] = CommittedPage{Header: RecordHeader{Type: RecordTypeNode, PageID: id}, Node: node}
				}
				maps.Copy(log.overlay, pages)
				if err := log.Checkpoint(main, int64(count+3)*pageSize, checksums); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(count * pageSize))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					maps.Copy(log.overlay, pages)
					if err := log.Checkpoint(main, int64(count+3)*pageSize, checksums); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
