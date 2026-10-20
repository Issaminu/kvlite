package blackout

import (
	"testing"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// TestCheckpointMovesTheFloorOnce requires that one checkpoint moves the floor of a SyncNormal session only once.
// A checkpoint that cleared the taint of a SyncNone session was counted again at the next step.
// So a later commit without a sync counted as durable, and a power failure that lost it was a false failure (seed 7412297367199167378).
func TestCheckpointMovesTheFloorOnce(t *testing.T) {
	checkpoints := uint64(1)
	db := kvlite.OpenMemory()
	defer db.Close()
	runner := &runner{
		fsys:            disk.New(1, disk.Config{}),
		target:          Target{CheckpointCount: func(*kvlite.DB) uint64 { return checkpoints }},
		db:              db,
		model:           newStateModel(false),
		session:         session{sync: kvlite.SyncNormal},
		tainted:         true,
		checkpointCount: checkpoints,
	}
	// A commit and a checkpoint make the storage durable, so the taint ends and the floor moves to the commit.
	runner.model.latest++
	checkpoints++
	runner.updateFloor()
	if runner.tainted || runner.model.floor != 1 {
		t.Fatalf("after the checkpoint: tainted %v, floor %d, want false and 1", runner.tainted, runner.model.floor)
	}
	// A SyncNormal commit without a checkpoint is not durable.
	runner.model.latest++
	runner.updateFloor()
	if runner.model.floor != 1 {
		t.Fatalf("after a commit without a checkpoint: floor %d, want 1", runner.model.floor)
	}
}
