package wal

import (
	"fmt"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// BenchmarkEncodeTransaction measures encoding without file writes or fixture setup.
func BenchmarkEncodeTransaction(b *testing.B) {
	for _, size := range []int{128, 64 << 10, 4 << 20} {
		for _, branch := range []bool{false, true} {
			kind := "leaf"
			if branch {
				kind = "branch"
			}
			b.Run(fmt.Sprintf("%s/bytes=%d", kind, size), func(b *testing.B) {
				const pageSize = 4096
				node := btree.NewLeafNode(3)
				if branch {
					node = btree.NewRootNode(3, btree.NewLeafNode(4), btree.NewLeafNode(5), make([]byte, size))
				} else if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), make([]byte, size))); err != nil {
					b.Fatal(err)
				}
				ids := make([]page.ID, btree.NodePageCount(node, pageSize)-1)
				for i := range ids {
					ids[i] = page.ID(6 + i)
				}
				node.SetOverflowPages(ids)
				nodes := []NodeRecord{{Final: node}}
				log := &WAL{pageSize: pageSize}
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := log.encodeTransaction(nodes, nil, nil, 1); err != nil {
						b.Fatal(err)
					}
					log.releaseEncodingBuffer()
				}
			})
		}
	}
}

// BenchmarkEncodeTransaction_ManyPages measures repeated transactions that are larger than the retained encoding buffer.
func BenchmarkEncodeTransaction_ManyPages(b *testing.B) {
	const pageSize = 4096
	const pageCount = 800 // About 3.1 MiB in each transaction.
	nodes := make([]NodeRecord, pageCount)
	for index := range nodes {
		node := btree.NewLeafNode(page.ID(3 + index))
		if err := node.InsertEntry(btree.NewEntry(0, []byte("key"), make([]byte, 3900))); err != nil {
			b.Fatal(err)
		}
		nodes[index] = NodeRecord{Final: node}
	}
	log := &WAL{pageSize: pageSize}
	b.ReportAllocs()
	b.SetBytes(pageCount * pageSize)
	for b.Loop() {
		if _, err := log.encodeTransaction(nodes, nil, nil, 1); err != nil {
			b.Fatal(err)
		}
		log.releaseEncodingBuffer()
	}
}
