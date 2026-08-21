package kvlite

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

type nodeCacheEntry struct {
	pageID     page.ID
	node       *btree.Node
	referenced atomic.Bool
}

// nodeCache stores committed nodes with CLOCK eviction. Its methods permit concurrent calls, but callers must not mutate a stored node.
type nodeCache struct {
	mu       sync.RWMutex
	capacity int
	entries  map[page.ID]*list.Element
	order    list.List
	// hand names the next eviction candidate. It is nil only when order is empty.
	hand *list.Element
}

func newNodeCache(capacity int) *nodeCache {
	cache := &nodeCache{capacity: max(capacity, 0)}
	if cache.capacity > 0 {
		cache.entries = make(map[page.ID]*list.Element)
	}
	return cache
}

func (cache *nodeCache) Get(pageID page.ID) (*btree.Node, bool) {
	if cache == nil || cache.capacity == 0 {
		return nil, false
	}
	cache.mu.RLock()
	element, ok := cache.entries[pageID]
	if !ok {
		cache.mu.RUnlock()
		return nil, false
	}
	entry := element.Value.(*nodeCacheEntry)
	// Avoid an atomic write after the first reference in the current clock cycle.
	if !entry.referenced.Load() {
		entry.referenced.Store(true)
	}
	node := entry.node
	cache.mu.RUnlock()
	return node, true
}

func (cache *nodeCache) Put(node *btree.Node) {
	if cache == nil || cache.capacity == 0 {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	pageID := node.PageID()
	if element, ok := cache.entries[pageID]; ok {
		entry := element.Value.(*nodeCacheEntry)
		entry.node = node
		entry.referenced.Store(true)
		return
	}

	if cache.order.Len() == cache.capacity {
		cache.evictOne()
	}
	entry := &nodeCacheEntry{pageID: pageID, node: node}
	entry.referenced.Store(true)
	element := cache.order.PushBack(entry)
	cache.entries[pageID] = element
	if cache.hand == nil {
		cache.hand = element
	}
}

// evictOne clears each referenced entry once and removes the first entry that has not been referenced since the clock hand last passed it.
func (cache *nodeCache) evictOne() {
	for {
		element := cache.hand
		entry := element.Value.(*nodeCacheEntry)
		if entry.referenced.Load() {
			entry.referenced.Store(false)
			cache.advanceClockHand()
			continue
		}

		delete(cache.entries, entry.pageID)
		if cache.order.Len() == 1 {
			cache.hand = nil
		} else {
			cache.advanceClockHand()
		}
		cache.order.Remove(element)
		return
	}
}

func (cache *nodeCache) advanceClockHand() {
	cache.hand = cache.hand.Next()
	if cache.hand == nil {
		cache.hand = cache.order.Front()
	}
}

func (cache *nodeCache) Len() int {
	if cache == nil {
		return 0
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.order.Len()
}

// pageCacheCapacity converts [Options.PageCacheBytes] and the stored page size into the entry limit for [nodeCache].
// Open calls it after metadata is loaded because page size is fixed only then.
//
// A return value of 0 disables the shared cache: [newNodeCache] treats non-positive capacity as a no-op cache.
// disabled takes priority over cacheBytes. pageSize must be positive; otherwise the result is 0.
//
// The limit counts whole pages: cacheBytes/pageSize is truncated toward zero, so a budget smaller than one page yields 0.
func pageCacheCapacity(cacheBytes uint64, pageSize int64, disabled bool) int {
	if disabled || pageSize <= 0 {
		return 0
	}
	pageCount := cacheBytes / uint64(pageSize)
	// nodeCache.capacity is an int; clamp before converting from uint64.
	maxInt := int(^uint(0) >> 1)
	if pageCount > uint64(maxInt) {
		return maxInt
	}
	return int(pageCount)
}
