package internal

type FsyncPolicy byte

const (
	OnEveryWrite FsyncPolicy = 0
	Periodic     FsyncPolicy = 1
	GroupCommit  FsyncPolicy = 2
)

type Config struct {
	// For background fsync
	FsyncIntervalMs uint32 // default: 10ms

	// For group commit
	BatchSize      uint32 // default: 100 writes
	BatchTimeoutMs uint32 // default: 10ms

	// Checkpointing
	CheckpointEveryWrites  uint32 // default: 10000
	CheckpointEverySeconds uint32 // default: 300 (5 minutes)

	// Fsync strategy
	FsyncPolicy FsyncPolicy // OnEveryWrite, Periodic, GroupCommit
}

func DefaultConfig() Config {
	return Config{
		FsyncPolicy:            OnEveryWrite,
		FsyncIntervalMs:        10,
		BatchSize:              100,
		BatchTimeoutMs:         10,
		CheckpointEveryWrites:  10000,
		CheckpointEverySeconds: 300,
	}
}
