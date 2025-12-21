package internal

type FsyncPolicy string

const (
	OnEveryWrite FsyncPolicy = "oneverywrite"
	Periodic     FsyncPolicy = "periodic"
	GroupCommit  FsyncPolicy = "groupcommit"
)

type Options struct {
	// Fsync strategy
	FsyncPolicy FsyncPolicy // OnEveryWrite, Periodic, GroupCommit

	// For background fsync
	FsyncIntervalMs int // default: 10ms

	// For group commit
	BatchSize      int // default: 100 writes
	BatchTimeoutMs int // default: 10ms

	// Checkpointing
	CheckpointEveryWrites  int // default: 10000
	CheckpointEverySeconds int // default: 300 (5 minutes)

	// Per-collection overrides
	CollectionOptions map[string]*CollectionOptions
}
