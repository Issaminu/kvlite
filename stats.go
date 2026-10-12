package kvlite

import (
	"fmt"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/page"
)

// Stats contains storage state and cumulative write counters for one open [DB] handle.
// Obtain a reading with [DB.Stats]. Counters cover work by this handle, including checkpoints during Open.
// A memory database reports logical page sizes and counts, not Go heap use.
// Its file, WAL, and checkpoint fields are zero.
type Stats struct {
	// PageSize is the size of one database page in bytes.
	PageSize int
	// DataSize is the logical database size in bytes. It includes metadata and free pages.
	DataSize int64
	// FileSize is the current main-file size in bytes. A checkpoint can change it.
	FileSize int64
	// WALSize is the current WAL size in bytes, including its file header.
	WALSize int64
	// WALPageCount is the number of committed page images waiting for a checkpoint.
	// It includes tree, allocation, and metadata pages. Repeated writes to one page count once.
	WALPageCount int
	// WALBytesSinceCheckpoint is the number of bytes appended since the last completed checkpoint in this handle.
	// It includes a new WAL file header but excludes bytes recovered during Open.
	WALBytesSinceCheckpoint uint64
	// CheckpointThresholdBytes is the WAL byte limit that starts a checkpoint.
	CheckpointThresholdBytes uint64
	// WALBytesWritten is the total size of successful WAL transaction appends. A checkpoint does not decrease this value.
	WALBytesWritten uint64
	// WALCommitCount counts successful nonempty WAL commits since this handle opened.
	// Several Update callbacks can share one commit. Failed commits and recovered transactions do not count.
	WALCommitCount uint64
	// CheckpointCount is the number of checkpoints that wrote all committed pages and cleared the WAL.
	CheckpointCount uint64
	// CheckpointFailureCount counts failed checkpoint attempts since this handle opened.
	// It includes WAL cleanup and main-file mapping failures. Empty checkpoints do not count.
	CheckpointFailureCount uint64
	// LastCheckpointError is the error from the last nonempty checkpoint attempt.
	// It is nil before the first attempt and after a successful attempt.
	// An automatic checkpoint failure does not fail a write that already committed.
	LastCheckpointError error
	// AllocatedPageCount is the number of pages that contain metadata, allocation data, or live tree data.
	AllocatedPageCount uint64
	// ReusablePageCount is the number of free pages at or below LastPageID.
	ReusablePageCount uint64
	// AllocationMetadataPageCount is the number of pages that store allocation bitmaps.
	AllocationMetadataPageCount uint64
	// LastPageID is the highest logical database page ID.
	LastPageID uint64
	// PagesReused counts allocations from free pages since this handle opened.
	PagesReused uint64
	// PagesRetired counts tree pages freed by committed writes since this handle opened.
	PagesRetired uint64
	// TailPagesReclaimed counts pages removed from the logical database tail since this handle opened. A file checkpoint later shrinks the main file.
	TailPagesReclaimed uint64
}

// Stats returns storage state and cumulative write counters for this database handle.
// Stats waits for an active write or checkpoint to finish. Do not call it from a transaction callback.
// Use [Bucket.Stats] to inspect a bucket inside a callback.
// Stats scans the allocation bitmap. A file database also reads the main-file and WAL sizes.
// A memory database reports logical page counts and DataSize. DataSize does not measure Go heap use.
// Its cost grows with the allocation metadata. It does not start a checkpoint or scan tree pages.
// It returns [ErrDatabaseNotOpen] after close starts, or a file-size read error.
func (db *DB) Stats() (Stats, error) {
	if err := db.beginOperation(); err != nil {
		return Stats{}, err
	}
	defer db.endOperation()

	db.operationMu.RLock()
	defer db.operationMu.RUnlock()

	result := db.stats
	result.PageSize = int(db.meta.PageSize())
	var err error
	result.DataSize, err = db.logicalFileSize()
	if err != nil {
		return Stats{}, err
	}
	if db.wal != nil {
		stats := db.wal.Stats()
		info, err := db.file.Stat()
		if err != nil {
			return Stats{}, err
		}
		result.FileSize = info.Size()
		result.WALSize, err = db.wal.Size()
		if err != nil {
			return Stats{}, err
		}
		result.WALPageCount = stats.CommittedRecordCount
		result.WALBytesSinceCheckpoint = stats.BytesSinceCheckpoint
		result.CheckpointThresholdBytes = stats.CheckpointThresholdBytes
		result.WALBytesWritten = stats.TotalBytesWritten
		result.WALCommitCount = stats.CommitCount
		result.CheckpointCount = stats.CheckpointCount
	}
	result.AllocatedPageCount = db.allocation.allocatedPageCount()
	result.LastPageID = uint64(db.meta.LastPage())
	result.ReusablePageCount = result.LastPageID + 1 - result.AllocatedPageCount
	result.AllocationMetadataPageCount = uint64(len(db.allocation.segments))
	return result, nil
}

// Sub returns write counters accumulated since other and keeps the current storage state and checkpoint error.
// Both readings must come from the same open DB handle, with s taken after other.
// A nil other returns s. Counters start again when a database opens.
func (s Stats) Sub(other *Stats) Stats {
	if other == nil {
		return s
	}
	result := s
	result.WALBytesWritten -= other.WALBytesWritten
	result.WALCommitCount -= other.WALCommitCount
	result.CheckpointCount -= other.CheckpointCount
	result.CheckpointFailureCount -= other.CheckpointFailureCount
	result.PagesReused -= other.PagesReused
	result.PagesRetired -= other.PagesRetired
	result.TailPagesReclaimed -= other.TailPagesReclaimed
	return result
}

// BucketStats contains page, entry, and size counts for a bucket and all its nested buckets.
// Values returned by [Bucket.Stats] are independent copies.
// In a memory database, storage byte counts describe logical pages, not Go heap use.
type BucketStats struct {
	// BranchPageN is the number of branch pages, including nested buckets.
	BranchPageN int
	// LeafPageN is the number of leaf pages, including nested buckets.
	LeafPageN int
	// KeyN is the number of plain key/value entries, including nested buckets. Bucket names do not count.
	KeyN int
	// Depth is the greatest B+ tree depth among this bucket and its nested buckets.
	// An empty bucket has depth one. Bucket nesting does not add tree levels.
	Depth int
	// BranchAlloc is the number of bytes required for branch storage, including continuation pages.
	BranchAlloc int
	// BranchInuse is the number of bytes used by branch headers, keys, and page IDs. It includes continuation headers.
	BranchInuse int
	// LeafAlloc is the number of bytes required for leaf storage, including continuation pages.
	LeafAlloc int
	// LeafInuse is the number of bytes used by leaf headers, keys, and values. It includes continuation headers and page IDs.
	LeafInuse int
	// BucketN is the number of buckets, including this bucket and all nested buckets.
	BucketN int
	// KeyBytes is the number of bytes in leaf keys, including nested bucket names.
	KeyBytes int
	// ValueBytes is the number of bytes in plain values. Bucket root pointers do not count.
	ValueBytes int
}

// Add adds other to s. Depth becomes the greater of the two depths.
func (s *BucketStats) Add(other BucketStats) {
	s.BranchPageN += other.BranchPageN
	s.LeafPageN += other.LeafPageN
	s.KeyN += other.KeyN
	s.Depth = max(s.Depth, other.Depth)
	s.BranchAlloc += other.BranchAlloc
	s.BranchInuse += other.BranchInuse
	s.LeafAlloc += other.LeafAlloc
	s.LeafInuse += other.LeafInuse
	s.BucketN += other.BucketN
	s.KeyBytes += other.KeyBytes
	s.ValueBytes += other.ValueBytes
}

// Stats counts the pages and entries in bucket and all its nested buckets.
// It reads the current transaction state, including earlier writes in an Update callback.
// It visits every tree page. Used byte counts include page headers but exclude page padding.
// Page counts exclude continuation pages. Storage byte counts include them.
// Earlier writes can require continuation pages that will be assigned at commit.
// A large scan can delay a writer while this transaction holds the database read lock.
// It returns a read error, [ErrTxClosed], or [ErrBucketNotFound] when the bucket cannot be read.
func (bucket *Bucket) Stats() (BucketStats, error) {
	if err := bucket.liveError(); err != nil {
		return BucketStats{}, err
	}
	if err := bucket.loadRootNode(); err != nil {
		return BucketStats{}, err
	}
	return bucket.tx.collectBucketStats(bucket.rootNode, make(map[page.ID]struct{}))
}

func (tx *Tx) collectBucketStats(root *btree.Node, seen map[page.ID]struct{}) (BucketStats, error) {
	stats := BucketStats{BucketN: 1}
	var nested BucketStats
	pageSize := int(tx.meta.PageSize())
	var walk func(*btree.Node, int) error
	walk = func(node *btree.Node, depth int) error {
		if _, exists := seen[node.PageID()]; exists {
			return fmt.Errorf("statistics visit page %d twice: %w", node.PageID(), page.ErrInvalid)
		}
		seen[node.PageID()] = struct{}{}
		stats.Depth = max(stats.Depth, depth)
		pages := btree.NodePageCount(node, int64(pageSize))
		if pages == 0 {
			return fmt.Errorf("statistics page %d size: %w", node.PageID(), page.ErrInvalid)
		}
		inuse := node.EncodedSize()
		if pages > 1 {
			// Continued storage adds two uint32 fields, one header per later page, and one ID per later page.
			inuse += 2*4 + (pages-1)*(btree.ContinuationHeaderSize+page.IDSize)
		}
		if !node.IsLeaf() {
			stats.BranchPageN++
			stats.BranchAlloc += pages * pageSize
			stats.BranchInuse += inuse
			for _, id := range node.Children {
				child, err := tx.store.ReadNode(id)
				if err != nil {
					return err
				}
				if err := walk(child, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
		stats.LeafPageN++
		stats.LeafAlloc += pages * pageSize
		stats.LeafInuse += inuse
		for index := 0; index < node.EntryCount(); index++ {
			entry := node.EntryAt(index)
			stats.KeyBytes += len(entry.Key())
			if entry.Flags()&btree.BucketLeafFlag == 0 {
				stats.KeyN++
				stats.ValueBytes += len(entry.Value())
				continue
			}
			id, err := decodeBucketRootPageID(*entry)
			if err != nil {
				return err
			}
			child, err := tx.store.ReadNode(id)
			if err != nil {
				return err
			}
			childStats, err := tx.collectBucketStats(child, seen)
			if err != nil {
				return err
			}
			nested.Add(childStats)
		}
		return nil
	}
	if err := walk(root, 1); err != nil {
		return BucketStats{}, err
	}
	stats.Add(nested)
	return stats, nil
}
