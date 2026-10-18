package blackout

import (
	"fmt"
	"maps"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// The checks in this file run after each step of a working handle.
//   - The page walk and the stats check run after every step.
//   - The process-crash image and the memory database are slower.
//     They run after one step in 8, or after every step with [Settings.DeepCheck].
//
// No check changes the run.
// The page walk and the stats check read without the node cache and with faults disabled.
// The slow checks work on copies, and their own random stream picks the steps.

// slowCheckRatio is the part of the steps that run the slow checks without [Settings.DeepCheck].
var slowCheckRatio = disk.OneIn(8)

// checkAfterStep runs the checks after one step.
func (runner *runner) checkAfterStep() {
	if runner.failure != nil || runner.db == nil || runner.stopped {
		return
	}
	if crashed, _ := runner.fsys.Crashed(); crashed {
		return
	}
	runner.checkMappings(runner.fsys)
	runner.checkPages("after step")
	runner.checkStats()
	if runner.failure == nil && (runner.settings.DeepCheck || runner.checkRand.Chance(slowCheckRatio)) {
		runner.checkImage()
		runner.checkMemory()
	}
}

// checkFiles checks the names and permission bits of the database files.
// The WAL lives next to the main file, also when Open got a symbolic link, and it has the mode of the main file.
func (runner *runner) checkFiles() {
	for _, name := range []string{runner.realPath, runner.realPath + "-wal"} {
		if perm, ok := runner.fsys.Perm(name); ok && perm != runner.config.FileMode {
			runner.fail("%s has mode %o, want %o", name, perm, runner.config.FileMode)
		}
	}
	if runner.config.PathMode == pathSymlink {
		if _, _, exists := runner.fsys.FileData(simDir + "/link-wal"); exists {
			runner.fail("a WAL exists next to the symbolic link, not next to the main file")
		}
	}
}

// checkMappings fails the run after a truncate that cut a live mapping of fsys.
func (runner *runner) checkMappings(fsys *disk.FS) {
	if count := fsys.Stats().MappingViolations; count > 0 {
		runner.fail("KVLite truncated the main file %d times under a live mapping; a read of the cut part stops a real process with SIGBUS", count)
	}
}

// checkStats compares DB.Stats with the simulated files, and with the earlier stats of the same handle.
func (runner *runner) checkStats() {
	runner.fsys.SetFaults(false)
	defer runner.fsys.SetFaults(runner.faults)
	stats, err := runner.db.Stats()
	if err != nil {
		runner.fail("DB.Stats: %v", err)
		return
	}
	runner.stats.StatsChecks++
	mainFile, _, _ := runner.fsys.FileData(runner.realPath)
	walFile, _, _ := runner.fsys.FileData(runner.realPath + "-wal")
	switch {
	case stats.PageSize != runner.work.pageSize:
		runner.fail("DB.Stats: PageSize is %d, want %d", stats.PageSize, runner.work.pageSize)
	case stats.FileSize != int64(len(mainFile)):
		runner.fail("DB.Stats: FileSize is %d, but the main file has %d bytes", stats.FileSize, len(mainFile))
	case stats.WALSize != int64(len(walFile)):
		runner.fail("DB.Stats: WALSize is %d, but the WAL file has %d bytes", stats.WALSize, len(walFile))
	case stats.CheckpointCount != runner.target.CheckpointCount(runner.db):
		runner.fail("DB.Stats: CheckpointCount is %d, but the WAL counts %d", stats.CheckpointCount, runner.target.CheckpointCount(runner.db))
	case runner.config.CheckpointBytes != 0 && stats.CheckpointThresholdBytes != runner.config.CheckpointBytes:
		runner.fail("DB.Stats: CheckpointThresholdBytes is %d, want the option value %d", stats.CheckpointThresholdBytes, runner.config.CheckpointBytes)
	case stats.WALPageCount == 0 && stats.WALSize == 0 && stats.FileSize != stats.DataSize:
		runner.fail("DB.Stats: with an empty WAL, FileSize %d differs from DataSize %d", stats.FileSize, stats.DataSize)
	}
	runner.checkFiles()
	if previous := runner.lastStats; previous != nil {
		// The counters of one handle never decrease.
		for _, counter := range []struct {
			name        string
			now, before uint64
		}{
			{"WALBytesWritten", stats.WALBytesWritten, previous.WALBytesWritten},
			{"WALCommitCount", stats.WALCommitCount, previous.WALCommitCount},
			{"CheckpointCount", stats.CheckpointCount, previous.CheckpointCount},
			{"CheckpointFailureCount", stats.CheckpointFailureCount, previous.CheckpointFailureCount},
			{"PagesReused", stats.PagesReused, previous.PagesReused},
			{"PagesRetired", stats.PagesRetired, previous.PagesRetired},
			{"TailPagesReclaimed", stats.TailPagesReclaimed, previous.TailPagesReclaimed},
		} {
			if counter.now < counter.before {
				runner.fail("DB.Stats: %s decreased from %d to %d in one handle", counter.name, counter.before, counter.now)
			}
		}
	}
	runner.lastStats = &stats
}

// checkImage opens a copy of the storage that a process crash would leave now.
// A process crash keeps every acknowledged commit, so the copy must hold exactly the latest state, unless a failed sync dropped the cache view.
// The check found the in-place B-tree parent bug: the open handle read correct data, but the files did not hold it.
func (runner *runner) checkImage() {
	runner.stats.ImageChecks++
	image := runner.fsys.CloneCache(streamSeed(runner.seed, streamCheck) ^ uint64(runner.step))
	db, err := runner.openDB(image, false)
	if err != nil {
		runner.fail("open the process-crash image: %v", err)
		return
	}
	state, err := runner.readState(db)
	if err == nil {
		err = runner.target.CheckPages(db)
	}
	closeErr := db.Close()
	runner.checkMappings(image)
	switch {
	case err != nil:
		runner.fail("the process-crash image: %v", err)
	case closeErr != nil:
		runner.fail("close the process-crash image: %v", closeErr)
	default:
		// A failed sync that dropped the cache view can lose unsynced commits without a crash, as a power failure does.
		lowest := runner.model.latest
		if runner.fsys.Stats().DroppedCaches > runner.droppedCaches {
			lowest = runner.model.floor
		}
		if _, _, found := runner.model.find(state, lowest, false); !found {
			runner.fail("the process-crash image matches no allowed state %d..%d\ndifference from the latest state:\n%s", lowest, runner.model.latest, stateDiff(runner.model.state, state))
		}
	}
}

// The memory database runs every acknowledged commit too: a second implementation of the same API, checked against the same model.
// It has no storage, so each verified open starts it again from the recovered state.

// resetMemory replaces the memory database with a new one that holds the latest state.
func (runner *runner) resetMemory() {
	if runner.memory != nil {
		if err := runner.memory.Close(); err != nil {
			runner.fail("close the memory database: %v", err)
			return
		}
	}
	runner.memory = kvlite.OpenMemory()
	paths := runner.model.bucketPaths()
	err := runner.memory.Update(func(tx *kvlite.Tx) error {
		// A parent path sorts before its nested paths.
		for _, path := range paths {
			if err := applyMutation(tx, txMutation{kind: mutationCreateBucket, path: path}); err != nil {
				return fmt.Errorf("create bucket %q: %w", path, err)
			}
		}
		for _, path := range paths {
			bucket, err := openBucket(tx, path)
			if err != nil {
				return err
			}
			content := runner.model.state[path]
			for _, key := range slices.Sorted(maps.Keys(content)) {
				if content[key] == nil {
					continue
				}
				if err := bucket.Put([]byte(key), content[key]); err != nil {
					return fmt.Errorf("put in bucket %q: %w", path, err)
				}
			}
		}
		return nil
	})
	if err != nil {
		runner.fail("fill the memory database: %v", err)
	}
}

// mirrorCommit runs the changes of one acknowledged commit on the memory database.
func (runner *runner) mirrorCommit(mutations []txMutation) {
	if runner.memory == nil || len(mutations) == 0 {
		return
	}
	err := runner.memory.Update(func(tx *kvlite.Tx) error {
		for _, mutation := range mutations {
			if err := applyMutation(tx, mutation); err != nil {
				return fmt.Errorf("%s: %w", mutation, err)
			}
		}
		return nil
	})
	if err != nil {
		runner.fail("memory database: %v", err)
	}
}

// checkMemory compares the memory database with the latest state.
func (runner *runner) checkMemory() {
	if runner.memory == nil {
		return
	}
	runner.stats.MemoryChecks++
	state, err := runner.readState(runner.memory)
	if err != nil {
		runner.fail("read the memory database: %v", err)
		return
	}
	// The checksum mode is a file setting.
	state[metaPath] = runner.model.state[metaPath]
	if !statesEqual(state, runner.model.state) {
		runner.fail("the memory database differs from the latest state:\n%s", stateDiff(runner.model.state, state))
	}
}

// applyMutation runs one model change in tx, without checks.
func applyMutation(tx *kvlite.Tx, mutation txMutation) error {
	switch mutation.kind {
	case mutationGet, mutationScan, mutationChecksums:
		return nil
	case mutationCreateBucket, mutationDeleteBucket:
		parent, name := parentPath(mutation.path)
		if parent == "" {
			if mutation.kind == mutationCreateBucket {
				_, err := tx.CreateBucket([]byte(name))
				return err
			}
			return tx.DeleteBucket([]byte(name))
		}
		bucket, err := openBucket(tx, parent)
		if err != nil {
			return err
		}
		if mutation.kind == mutationCreateBucket {
			_, err := bucket.CreateBucket([]byte(name))
			return err
		}
		return bucket.DeleteBucket([]byte(name))
	}
	bucket, err := openBucket(tx, mutation.path)
	if err != nil {
		return err
	}
	switch mutation.kind {
	case mutationPut:
		return bucket.Put([]byte(mutation.key), mutation.value)
	case mutationDelete:
		return bucket.Delete([]byte(mutation.key))
	case mutationDeleteBatch:
		keys := make([][]byte, len(mutation.keys))
		for index, key := range mutation.keys {
			keys[index] = []byte(key)
		}
		return bucket.DeleteBatch(keys)
	}
	return nil
}

// openBucket opens the bucket at path.
func openBucket(tx *kvlite.Tx, path string) (*kvlite.Bucket, error) {
	names := strings.Split(path, "/")
	bucket, err := tx.Bucket([]byte(names[0]))
	for _, name := range names[1:] {
		if err != nil {
			break
		}
		bucket, err = bucket.Bucket([]byte(name))
	}
	if err != nil {
		return nil, fmt.Errorf("open bucket %q: %w", path, err)
	}
	return bucket, nil
}

// guard runs fn under the step time limit.
// A step over the limit prints every goroutine stack and stops the process, because a hung call cannot be stopped from outside.
func (runner *runner) guard(fn func()) {
	seed, step, limit := runner.seed, runner.step, runner.settings.StepTimeout
	timer := time.AfterFunc(limit, func() {
		stacks := make([]byte, 16<<20)
		stacks = stacks[:runtime.Stack(stacks, true)]
		fmt.Fprintf(os.Stderr, "blackout seed %d: step %d did not finish in %s; goroutine stacks:\n%s\n", seed, step, limit, stacks)
		panic(fmt.Sprintf("blackout seed %d: step %d did not finish in %s (a deadlock or a hang)", seed, step, limit))
	})
	defer timer.Stop()
	fn()
}
