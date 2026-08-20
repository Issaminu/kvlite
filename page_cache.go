package kvlite

import (
	"container/list"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

type nodeCacheEntry struct {
	pageID page.ID
	node   *btree.Node
}

// nodeCache is a LRU (Least Recently Used) cache of nodes with capacity.
// Automatically handles removal of the least recently used element if cache size surpassed `capacity`.
type nodeCache struct {
	capacity int
	entries  map[page.ID]*list.Element
	order    list.List
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
	element, ok := cache.entries[pageID]
	if !ok {
		return nil, false
	}
	cache.order.MoveToFront(element)
	return element.Value.(nodeCacheEntry).node, true
}

func (cache *nodeCache) Put(node *btree.Node) {
	if cache == nil || cache.capacity == 0 {
		return
	}
	pageID := node.PageID()
	if element, ok := cache.entries[pageID]; ok {
		element.Value = nodeCacheEntry{pageID: pageID, node: node}
		cache.order.MoveToFront(element)
		return
	}

	element := cache.order.PushFront(nodeCacheEntry{pageID: pageID, node: node})
	cache.entries[pageID] = element
	if cache.order.Len() <= cache.capacity {
		return
	}

	oldest := cache.order.Back()
	delete(cache.entries, oldest.Value.(nodeCacheEntry).pageID)
	cache.order.Remove(oldest)
}

func (cache *nodeCache) Len() int {
	if cache == nil {
		return 0
	}
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
