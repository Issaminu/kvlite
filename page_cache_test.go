package kvlite

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

func TestNodeCache_AllowsConcurrentGets(t *testing.T) {
	const nodeCount = 8
	cache := newNodeCache(nodeCount)
	for pageID := page.ID(1); pageID <= nodeCount; pageID++ {
		cache.Put(btree.NewLeafNode(pageID))
	}

	const readerCount = 8
	const readsPerReader = 1_000
	start := make(chan struct{})
	errorsByReader := make(chan error, readerCount)
	var readers sync.WaitGroup
	readers.Add(readerCount)
	for readerID := range readerCount {
		go func() {
			defer readers.Done()
			<-start
			for readIndex := range readsPerReader {
				pageID := page.ID((readerID+readIndex)%nodeCount + 1)
				node, ok := cache.Get(pageID)
				if !ok || node.PageID() != pageID {
					errorsByReader <- fmt.Errorf("page %d: node=%v cached=%t", pageID, node, ok)
					return
				}
			}
		}()
	}
	close(start)
	readers.Wait()
	close(errorsByReader)
	for err := range errorsByReader {
		t.Fatal(err)
	}
}

func TestNodeCache_CoalescesReferencesBeforeClockSweep(t *testing.T) {
	cache := newNodeCache(2)
	first := btree.NewLeafNode(1)
	second := btree.NewLeafNode(2)
	third := btree.NewLeafNode(3)

	cache.Put(first)
	cache.Put(second)
	if _, ok := cache.Get(1); !ok {
		t.Fatal("first node not cached")
	}
	cache.Put(third)

	if _, ok := cache.Get(1); ok {
		t.Fatal("first clock candidate remains cached")
	}
	if _, ok := cache.Get(2); !ok {
		t.Fatal("second clock candidate was evicted")
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("cache length: got %d, want 2", got)
	}
}

func TestNodeCache_GivesReferencedNodeSecondChance(t *testing.T) {
	cache := newNodeCache(3)
	cache.Put(btree.NewLeafNode(1))
	cache.Put(btree.NewLeafNode(2))
	cache.Put(btree.NewLeafNode(3))
	cache.Put(btree.NewLeafNode(4))

	if _, ok := cache.Get(2); !ok {
		t.Fatal("second node not cached")
	}
	cache.Put(btree.NewLeafNode(5))

	if _, ok := cache.Get(2); !ok {
		t.Fatal("referenced node was evicted")
	}
	if _, ok := cache.Get(3); ok {
		t.Fatal("unreferenced node remains cached")
	}
}

func TestNodeCache_ZeroCapacityDoesNotKeepNodes(t *testing.T) {
	cache := newNodeCache(0)
	cache.Put(btree.NewLeafNode(1))
	if got := cache.Len(); got != 0 {
		t.Fatalf("cache length: got %d, want 0", got)
	}
}

func TestPageCacheCapacity(t *testing.T) {
	const pageSize = int64(4096)
	if got := pageCacheCapacity(16<<20, pageSize, false); got != 4096 {
		t.Fatalf("default cache pages: got %d, want 4096", got)
	}
	if got := pageCacheCapacity(4095, pageSize, false); got != 0 {
		t.Fatalf("small cache pages: got %d, want 0", got)
	}
	if got := pageCacheCapacity(16<<20, pageSize, true); got != 0 {
		t.Fatalf("disabled cache pages: got %d, want 0", got)
	}
}
