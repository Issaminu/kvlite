package blackout

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// StressSettings are the knobs of one stress run.
type StressSettings struct {
	// Clients is the number of goroutines. Zero means 6.
	Clients int
	// Operations is the number of operations of each client. Zero means 300.
	Operations int
	// Timeout is the time limit of the run. Zero means 5 minutes.
	// A run over the limit, such as a deadlock, prints every goroutine stack and stops the process.
	Timeout time.Duration
}

const (
	stressPath = "/sim/stress"
	stressKeys = 6 // keys in the bucket of each client
)

// Stress runs real goroutines against one database at the same time, and checks the history that they see.
// It finds what the deterministic runs cannot: data races, real group commit through the write worker, and Backup, Stats, checkpoints, and Close during writes.
// The storage has no faults and no crashes.
//
// Each client writes only its own bucket, and each commit writes every key of that bucket with the commit number in the value.
// So a reader can check, without a full model:
//   - atomic commits: all keys of one bucket show one commit;
//   - completed commits: a read shows at least every commit that returned before the read started;
//   - no invented commits: a read shows no commit that had not started when the read returned;
//   - monotonic reads: a client never sees a bucket go back to an older commit.
//
// A seed selects the options and the operation mix of each client.
// The schedule of the goroutines does not repeat, so a failure may not replay.
// Run it with the race detector too: go test -race -run TestBlackoutStress .
// It returns what the run did, for the test log, and the first failure.
func Stress(target Target, seed uint64, settings StressSettings) (StressResult, error) {
	if settings.Clients <= 0 {
		settings.Clients = 6
	}
	if settings.Operations <= 0 {
		settings.Operations = 300
	}
	if settings.Timeout <= 0 {
		settings.Timeout = 5 * time.Minute
	}
	rand := disk.NewRand(streamSeed(seed, streamConfig))
	run := &stressRun{
		target:    target,
		seed:      seed,
		clients:   settings.Clients,
		started:   make([]atomic.Int64, settings.Clients),
		completed: make([]atomic.Int64, settings.Clients),
	}
	run.options = &kvlite.Options{
		Synchronous:              [...]kvlite.Sync{kvlite.SyncFull, kvlite.SyncNormal, kvlite.SyncNone}[rand.Weighted([]uint64{3, 1, 1})],
		MaxWriteBatchSize:        [...]int{1, 2, 4, 100}[rand.IntN(4)],
		CheckpointThresholdBytes: [...]uint64{1, 64 << 10, 0}[rand.IntN(3)],
		DataPageChecksums:        rand.Bool(),
	}
	run.pageSize = [...]int{4096, 16384}[rand.IntN(2)]
	staleMappings := rand.Chance(disk.OneIn(4))
	run.fsys = disk.New(streamSeed(seed, streamDisk), disk.Config{StaleMappings: staleMappings})
	run.name = fmt.Sprintf("stress seed %d (clients=%d operations=%d sync=%s max-batch=%d checkpoint=%d checksums=%t page=%d stale-mappings=%t)",
		seed, settings.Clients, settings.Operations, syncNames[run.options.Synchronous], run.options.MaxWriteBatchSize,
		run.options.CheckpointThresholdBytes, run.options.DataPageChecksums, run.pageSize, staleMappings)

	timer := time.AfterFunc(settings.Timeout, func() {
		stacks := make([]byte, 16<<20)
		stacks = stacks[:runtime.Stack(stacks, true)]
		fmt.Fprintf(os.Stderr, "%s did not finish in %s; goroutine stacks:\n%s\n", run.name, settings.Timeout, stacks)
		panic(fmt.Sprintf("%s did not finish in %s (a deadlock or a hang)", run.name, settings.Timeout))
	})
	defer timer.Stop()

	if err := run.setup(); err != nil {
		return run.result(), fmt.Errorf("%s: %w", run.name, err)
	}
	run.parallel(func(client int) { run.client(client, settings.Operations) })
	if err := run.err(); err != nil {
		return run.result(), err
	}
	run.closeDuringWork()
	if run.err() == nil {
		run.verifyFinal("after Close")
	}
	if run.err() == nil && run.options.Synchronous != kvlite.SyncNone {
		// Close synced every completed commit, so a power failure keeps them all.
		run.fsys.Restart(true)
		run.verifyFinal("after a power failure")
	}
	return run.result(), run.err()
}

// StressResult counts what one stress run did.
type StressResult struct {
	Name        string
	Commits     int64 // completed commits of all clients
	Reads       int64
	Backups     int64
	Checkpoints int64
}

func (result StressResult) String() string {
	return fmt.Sprintf("%s: %d commits, %d reads, %d backups, %d checkpoints", result.Name, result.Commits, result.Reads, result.Backups, result.Checkpoints)
}

// result returns what the run did so far.
func (run *stressRun) result() StressResult {
	result := StressResult{Name: run.name, Reads: run.reads.Load(), Backups: run.backups.Load(), Checkpoints: run.checkpoints.Load()}
	for client := range run.clients {
		result.Commits += run.completed[client].Load()
	}
	return result
}

// stressRun is one stress run.
type stressRun struct {
	target      Target
	seed        uint64
	name        string
	clients     int
	options     *kvlite.Options
	pageSize    int
	fsys        *disk.FS
	db          *kvlite.DB
	started     []atomic.Int64 // commits that each client started
	completed   []atomic.Int64 // commits that returned success
	reads       atomic.Int64
	backups     atomic.Int64
	checkpoints atomic.Int64
	mu          sync.Mutex
	failure     error
}

// fail records the first failure. Every goroutine stops soon after it.
func (run *stressRun) fail(format string, args ...any) {
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.failure == nil {
		run.failure = fmt.Errorf("%s: %s\nThe goroutine schedule does not repeat, so this seed may pass on a replay.", run.name, fmt.Sprintf(format, args...))
	}
}

func (run *stressRun) err() error {
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.failure
}

// parallel runs body once for each client, each in its own goroutine, and waits for all.
func (run *stressRun) parallel(body func(client int)) {
	var group sync.WaitGroup
	for client := range run.clients {
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() {
				if value := recover(); value != nil {
					run.fail("client %d: panic: %v", client, value)
				}
			}()
			body(client)
		}()
	}
	group.Wait()
}

// setup opens the database and writes commit 0 of every client.
func (run *stressRun) setup() error {
	db, err := run.target.Open(stressPath, 0o600, run.options, run.fsys, run.pageSize)
	if err != nil {
		return fmt.Errorf("Open: %w", err)
	}
	run.db = db
	return db.Update(func(tx *kvlite.Tx) error {
		for client := range run.clients {
			bucket, err := tx.CreateBucket(stressBucket(client))
			if err != nil {
				return err
			}
			for key := range stressKeys {
				if err := bucket.Put(stressKey(key), stressValue(client, 0, key)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func stressBucket(client int) []byte { return fmt.Appendf(nil, "client-%d", client) }
func stressKey(key int) []byte       { return fmt.Appendf(nil, "key-%d", key) }

// stressValue returns the value of key in commit of client: a tag, then bytes that depend on all three.
// Sizes reach 6 KB, so some values need continuation pages.
func stressValue(client int, commit int64, key int) []byte {
	tag := fmt.Sprintf("c%d n%d k%d|", client, commit, key)
	size := len(tag) + int((uint64(client)*131+uint64(key)*977+uint64(commit)*7919)%6000)
	value := make([]byte, size)
	copy(value, tag)
	for index := len(tag); index < size; index++ {
		value[index] = byte(client + int(commit) + key + index)
	}
	return value
}

// stressCommit returns the commit number of a value of key in the bucket of client, and checks every byte of the value.
func stressCommit(value []byte, client, key int) (int64, error) {
	var gotClient, gotKey int
	var commit int64
	if _, err := fmt.Sscanf(string(value), "c%d n%d k%d|", &gotClient, &commit, &gotKey); err != nil {
		return 0, fmt.Errorf("a %d-byte value without a valid tag", len(value))
	}
	if gotClient != client || gotKey != key || !bytes.Equal(value, stressValue(client, commit, key)) {
		return 0, fmt.Errorf("the %d-byte value of commit %d is damaged or belongs to another key", len(value), commit)
	}
	return commit, nil
}

// client runs the operations of one client.
func (run *stressRun) client(client, operations int) {
	rand := disk.NewRand(streamSeed(run.seed, 1000+uint64(client)))
	lastSeen := make([]int64, run.clients)
	// write, own read, bucket read, DB.Get, Stats, checkpoint, Backup, checksum mode
	weights := []uint64{40, 15, 20, 10, 4, 3, 2, 2}
	for operation := 0; operation < operations && run.err() == nil; operation++ {
		switch rand.Weighted(weights) {
		case 0:
			if err := run.write(client); err != nil {
				run.fail("client %d: write: %v", client, err)
			}
		case 1:
			run.readOwn(client)
		case 2:
			run.readBucket(client, rand.IntN(run.clients), lastSeen)
		case 3:
			run.get(client, rand.IntN(run.clients), rand.IntN(stressKeys), lastSeen)
		case 4:
			if _, err := run.db.Stats(); err != nil {
				run.fail("client %d: DB.Stats: %v", client, err)
			}
		case 5:
			run.checkpoints.Add(1)
			if err := run.target.Checkpoint(run.db); err != nil {
				run.fail("client %d: checkpoint: %v", client, err)
			}
		case 6:
			run.backup(client, operation)
		case 7:
			if err := run.db.SetDataPageChecksums(rand.Bool()); err != nil {
				run.fail("client %d: SetDataPageChecksums: %v", client, err)
			}
		}
	}
}

// write commits the next commit of client: every key of its bucket.
func (run *stressRun) write(client int) error {
	commit := run.started[client].Add(1)
	err := run.db.Update(func(tx *kvlite.Tx) error {
		bucket, err := tx.Bucket(stressBucket(client))
		if err != nil {
			return err
		}
		for key := range stressKeys {
			if err := bucket.Put(stressKey(key), stressValue(client, commit, key)); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		run.completed[client].Store(commit)
	}
	return err
}

// readOwn reads the bucket of client: the only writer of that bucket must see its last commit.
func (run *stressRun) readOwn(client int) {
	run.reads.Add(1)
	want := run.completed[client].Load()
	err := run.db.View(func(tx *kvlite.Tx) error {
		bucket, err := tx.Bucket(stressBucket(client))
		if err != nil {
			return err
		}
		for key := range stressKeys {
			value, err := bucket.Get(stressKey(key))
			if err != nil {
				return err
			}
			commit, err := stressCommit(value, client, key)
			if err != nil {
				return err
			}
			if commit != want {
				return fmt.Errorf("key %d shows commit %d, want the last commit %d of this client", key, commit, want)
			}
		}
		return nil
	})
	if err != nil {
		run.fail("client %d: read its own bucket: %v", client, err)
	}
}

// readBucket scans the bucket of other in one View: every key must show one commit, inside the bounds of the history.
func (run *stressRun) readBucket(client, other int, lastSeen []int64) {
	run.reads.Add(1)
	low := run.completed[other].Load()
	seen := int64(-1)
	err := run.db.View(func(tx *kvlite.Tx) error {
		bucket, err := tx.Bucket(stressBucket(other))
		if err != nil {
			return err
		}
		cursor, err := bucket.Cursor()
		if err != nil {
			return err
		}
		key := 0
		for name, value, err := cursor.First(); name != nil || err != nil; name, value, err = cursor.Next() {
			if err != nil {
				return err
			}
			if key >= stressKeys || !bytes.Equal(name, stressKey(key)) {
				return fmt.Errorf("scan entry %d is key %q", key, name)
			}
			commit, err := stressCommit(value, other, key)
			if err != nil {
				return err
			}
			if seen >= 0 && commit != seen {
				return fmt.Errorf("one View shows commit %d for key %d and commit %d for an earlier key: a commit is visible in part", commit, key, seen)
			}
			seen = commit
			key++
		}
		if key != stressKeys {
			return fmt.Errorf("the scan returned %d keys, want %d", key, stressKeys)
		}
		return nil
	})
	high := run.started[other].Load()
	if err != nil {
		run.fail("client %d: read the bucket of client %d: %v", client, other, err)
		return
	}
	run.checkBounds(client, other, seen, low, high, lastSeen)
}

// get reads one key of other with DB.Get.
func (run *stressRun) get(client, other, key int, lastSeen []int64) {
	run.reads.Add(1)
	low := run.completed[other].Load()
	value, err := run.db.Get(stressBucket(other), stressKey(key))
	high := run.started[other].Load()
	if err != nil {
		run.fail("client %d: DB.Get of client %d key %d: %v", client, other, key, err)
		return
	}
	commit, err := stressCommit(value, other, key)
	if err != nil {
		run.fail("client %d: DB.Get of client %d key %d: %v", client, other, key, err)
		return
	}
	run.checkBounds(client, other, commit, low, high, lastSeen)
}

// checkBounds checks one commit that client saw in the bucket of other.
// low is the last completed commit before the read started, and high is the last started commit when the read returned.
func (run *stressRun) checkBounds(client, other int, commit, low, high int64, lastSeen []int64) {
	switch {
	case commit < low:
		run.fail("client %d: read commit %d of client %d, but commit %d completed before the read started", client, commit, other, low)
	case commit > high:
		run.fail("client %d: read commit %d of client %d, but only %d commits had started", client, commit, other, high)
	case commit < lastSeen[other]:
		run.fail("client %d: read commit %d of client %d after an earlier read showed commit %d", client, commit, other, lastSeen[other])
	default:
		lastSeen[other] = commit
	}
}

// backup makes a backup and checks it: one consistent state inside the bounds of the history.
// Only client 0 makes backups, so they do not pile up.
func (run *stressRun) backup(client, operation int) {
	if client != 0 {
		return
	}
	path := fmt.Sprintf("/sim/stress-backup-%d", operation)
	lows := make([]int64, run.clients)
	for other := range lows {
		lows[other] = run.completed[other].Load()
	}
	err := run.db.Backup(path)
	highs := make([]int64, run.clients)
	for other := range highs {
		highs[other] = run.started[other].Load()
	}
	if err != nil {
		run.fail("Backup: %v", err)
		return
	}
	run.backups.Add(1)
	defer run.fsys.Remove(path)
	backup, err := run.target.Open(path, 0o600, &kvlite.Options{ReadOnly: true, Synchronous: run.options.Synchronous}, run.fsys, run.pageSize)
	if err != nil {
		run.fail("open the backup: %v", err)
		return
	}
	defer backup.Close()
	err = backup.View(func(tx *kvlite.Tx) error {
		for other := range run.clients {
			bucket, err := tx.Bucket(stressBucket(other))
			if err != nil {
				return err
			}
			seen := int64(-1)
			for key := range stressKeys {
				value, err := bucket.Get(stressKey(key))
				if err != nil {
					return err
				}
				commit, err := stressCommit(value, other, key)
				if err != nil {
					return err
				}
				if seen >= 0 && commit != seen {
					return fmt.Errorf("client %d has commit %d for key %d and commit %d for an earlier key", other, commit, key, seen)
				}
				seen = commit
			}
			if seen < lows[other] || seen > highs[other] {
				return fmt.Errorf("client %d has commit %d, want a commit from %d to %d", other, seen, lows[other], highs[other])
			}
		}
		return nil
	})
	if err != nil {
		run.fail("the backup is not one state of the history: %v", err)
	}
}

// closeDuringWork closes the handle while every client still writes and reads.
// Close waits for the operations that started. Each later call must return ErrDatabaseNotOpen.
func (run *stressRun) closeDuringWork() {
	var started sync.WaitGroup
	started.Add(run.clients)
	closeErr := make(chan error, 1)
	go func() {
		started.Wait()
		closeErr <- run.db.Close()
	}()
	run.parallel(func(client int) {
		started.Done()
		for range 1000 {
			err := run.write(client)
			if errors.Is(err, kvlite.ErrDatabaseNotOpen) {
				return
			}
			if err != nil {
				run.fail("client %d: write during Close: %v", client, err)
				return
			}
			if err := run.db.View(func(*kvlite.Tx) error { return nil }); err != nil && !errors.Is(err, kvlite.ErrDatabaseNotOpen) {
				run.fail("client %d: View during Close: %v", client, err)
				return
			}
		}
		run.fail("client %d: 1000 writes succeeded during Close", client)
	})
	if err := <-closeErr; err != nil {
		run.fail("Close during work: %v", err)
		return
	}
	if _, err := run.db.Get(stressBucket(0), stressKey(0)); !errors.Is(err, kvlite.ErrDatabaseNotOpen) {
		run.fail("DB.Get after Close returned %v, want ErrDatabaseNotOpen", err)
	}
}

// verifyFinal opens the database again and requires every completed commit.
func (run *stressRun) verifyFinal(stage string) {
	db, err := run.target.Open(stressPath, 0o600, run.options, run.fsys, run.pageSize)
	if err != nil {
		run.fail("Open %s: %v", stage, err)
		return
	}
	defer db.Close()
	err = db.View(func(tx *kvlite.Tx) error {
		for client := range run.clients {
			bucket, err := tx.Bucket(stressBucket(client))
			if err != nil {
				return err
			}
			want := run.completed[client].Load()
			for key := range stressKeys {
				value, err := bucket.Get(stressKey(key))
				if err != nil {
					return err
				}
				commit, err := stressCommit(value, client, key)
				if err != nil {
					return err
				}
				if commit != want {
					return fmt.Errorf("client %d key %d shows commit %d, want its last completed commit %d", client, key, commit, want)
				}
			}
		}
		return nil
	})
	if err != nil {
		run.fail("%s: %v", stage, err)
	}
}
