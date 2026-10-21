// Package disk is the simulated storage of Blackout, KVLite's deterministic simulation test.
//
// Each file has two views.
// The cache view holds every completed write, like the operating-system page cache.
// Reads see it, and it survives a process crash.
// The durable view holds the bytes that survive a power failure.
// Each file also keeps its unsynced changes in order.
// A successful Sync applies those changes to the durable view.
//
// A power failure starts from the durable view and then keeps a random part of the unsynced changes.
// A sector is the unit of persistence: a sector keeps a prefix of its own writes, and sectors have no order between them.
// A power failure can also keep or lose a truncate, keep a longer file length without its data, and keep or lose each name change that no directory sync made durable.
//
// Calls can also fail while the process keeps running.
// A failed call does not mean that nothing happened: a failed write can still write a prefix, and a failed truncate or remove can still take effect.
// A failed sync can make some changes durable and drop the others for good, as Linux does after a writeback error ("fsyncgate").
//
// A seed selects every decision.
// The same seed and the same calls give the same results.
//
// A file can be mapped into memory ([fileio.Mapper]).
// A mapping shows later writes, as a shared mapping on Unix does, or keeps its first bytes, as on Windows.
// A truncate that cuts a live mapping counts as a violation, because a read of the cut part stops a real process.
//
// Names can be relative to Config.WorkingDir, and the last name element can be a symbolic link.
// Hard links share one inode, which a power failure decides once.
//
// The model has these limits: no file locks, no renames, and symbolic links only in the last name element.
package disk

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Issaminu/kvlite/internal/fileio"
	"unsafe"
)

var (
	// ErrInjected marks an I/O error that the configuration injected.
	// Each such error also matches [syscall.EIO].
	ErrInjected = errors.New("disk: injected I/O error")

	// ErrCrashed means the simulated process crashed.
	// Every call fails with it until [FS.Restart].
	ErrCrashed = errors.New("disk: process crashed")
)

// cachePageSize is the unit that a process crash can split a write at.
// The kernel copies one page at a time.
const cachePageSize = 4096

// SyncLie selects a deliberate durability defect.
// Tests use it to prove that their checks detect lost data.
type SyncLie uint8

const (
	// SyncLieNone makes every successful sync durable.
	SyncLieNone SyncLie = iota
	// SyncLieAllFiles makes file syncs return success without making data durable.
	SyncLieAllFiles
	// SyncLieMainFile makes file syncs lie for every file except names that end with "-wal".
	SyncLieMainFile
	// SyncLieDirectory makes directory syncs return success without making names durable.
	SyncLieDirectory
)

// Config selects the faults of one FS and how it persists data at a power failure.
// The zero Config injects no errors, uses 512-byte sectors, and keeps each unsynced change with probability one half.
type Config struct {
	// SectorSize is the unit of persistence at a power failure.
	// Zero means 512 bytes.
	SectorSize int

	// WorkingDir resolves relative names. Empty means "/".
	WorkingDir string

	// Probabilities of an injected error, for each call while faults are enabled.
	ReadFault     Ratio
	WriteFault    Ratio // A failed write can still change a prefix of its bytes.
	SyncFault     Ratio // A failed sync can make some changes durable and drop the others for good.
	TruncateFault Ratio // A failed truncate can still take effect.
	StatFault     Ratio
	OpenFault     Ratio
	RemoveFault   Ratio // A failed remove can still take effect.
	SyncDirFault  Ratio // A failed directory sync can make some names durable.
	MapFault      Ratio // A failed map makes KVLite read through ReadAt.

	// DisableMap makes every map fail, so readers use ReadAt, the fallback after a failed mapping.
	DisableMap bool

	// StaleMappings makes a mapping keep the bytes of its start, as on Windows.
	// Without it, a mapping shows later writes, as a shared mapping on Unix does.
	StaleMappings bool

	// FullFault is the probability that a write starts a disk-full episode.
	// For the next 1 to 30 mutating calls, each write or truncate that grows a file fails with ENOSPC, after it writes the part inside the current length.
	FullFault Ratio

	// ReadCorruption is the probability that a read returns one flipped bit, while the stored bytes stay correct.
	// It never applies to a WAL file.
	// A real read of a WAL comes from the page cache, so a second read returns the same bytes, and a wrong WAL byte is then bit rot, which Blackout does not inject (see damage.go).
	ReadCorruption Ratio

	// TornSectors lets a power failure keep part of a sector, so a sector is not atomic.
	TornSectors bool

	// Garbage fills file space that a power failure kept without its data with random bytes instead of zeros.
	Garbage bool

	// Misdirect is the probability that a power failure moves one kept write of a file that is not a WAL to a wrong offset, 1 to 4 blocks of 4096 bytes away.
	// The intended range keeps its older bytes.
	// [FS.Heal] undoes the damage.
	Misdirect Ratio

	// DropCache is the probability that a failed sync also drops the dropped changes from the cache view, so reads return the durable bytes.
	DropCache Ratio

	// RevertFailedSync makes a failed sync behave as on Btrfs: no unsynced change of the file reaches storage, and the cache view becomes the durable view again.
	// Without it, a failed sync behaves as on ext4 and XFS: some writes reach storage, every truncate reaches storage, and the cache keeps the new bytes unless DropCache drops them.
	// Rebello et al., "Can Applications Recover from fsync Failures?" (USENIX ATC 2020), §3.3 and Table 1.
	RevertFailedSync bool

	// KeepChange is the probability that a power failure keeps one unsynced sector, truncate, file length, or name change.
	// Zero means one half.
	KeepChange Ratio

	// InOrder is the probability that a power failure keeps a complete prefix of the unsynced changes of each file and loses the rest, as an ordered write-back would.
	InOrder Ratio

	// SyncLie injects a durability defect.
	// Only tests that check the checker use it.
	SyncLie SyncLie

	// OnEvent, when not nil, receives every event.
	// Tests use it to find the first difference between two runs.
	OnEvent func(Event)
}

// Stats counts the faults and crash decisions of one FS.
type Stats struct {
	InjectedFaults   int // injected errors of all kinds
	DroppedSyncs     int // failed syncs that dropped unsynced changes for good
	DroppedCaches    int // failed syncs that also dropped those changes from the cache view, so a running process loses them
	RevertedSyncs    int // failed syncs that reverted the file to its durable bytes, as Btrfs does
	ProcessCrashes   int // restarts that kept the cache view
	PowerFailures    int // restarts that kept only the durable view and a random part of the unsynced changes
	ScheduledCrashes int // crashes inside a call
	TornWrites       int // writes that a power failure kept only in part
	LostWrites       int // writes that a power failure lost completely
	LostTruncates    int // truncates that a power failure lost
	LostNameChanges  int // name changes that a power failure lost

	NoSpaceErrors     int // writes and truncates that failed with ENOSPC
	ReadCorruptions   int // reads that returned a flipped bit
	TornSectors       int // sectors that a power failure kept in part
	GarbageFills      int // extensions that a power failure filled with random bytes
	MisdirectedWrites int // writes that a power failure moved to a wrong offset
	BitRot            int // calls of Corrupt that changed stored bytes

	// MappingViolations counts truncates that cut a live mapping.
	// A read of the cut part stops a real process with SIGBUS, so each one is a bug of the caller.
	MappingViolations int
}

// Event is one call or decision of the FS, for failure reports.
type Event struct {
	Mutation uint64 // number of mutating calls before this event
	Op       string
	Name     string
	Offset   int64
	Length   int
	Result   string
}

func (event Event) String() string {
	return fmt.Sprintf("fs#%d %s %s off=%d len=%d -> %s", event.Mutation, event.Op, filepath.Base(event.Name), event.Offset, event.Length, event.Result)
}

const maxRecentEvents = 64

// FS is a simulated file system.
// It implements [fileio.FS].
// Its methods are safe for concurrent use.
type FS struct {
	mu           sync.Mutex
	config       Config
	rand         *Rand
	names        map[string]*inode // cache view of the directory entries
	durableNames map[string]*inode // durable view of the directory entries
	links        map[string]string // symbolic links, from absolute name to target; they are always durable
	generation   uint64            // a restart increments it, so earlier handles fail
	mutations    uint64            // mutating calls so far; a scheduled crash counts them
	crashAt      uint64            // mutation that crashes; zero means none
	crashed      bool
	crashEvent   Event
	faults       bool   // enables the injected errors of Config
	noSyncFaults bool   // turns off the injected sync errors of Config, also while faults are enabled
	volatile     bool   // the FS keeps no durable view and no unsynced changes, so a power failure is not possible
	fullUntil    uint64 // a disk-full episode lasts until this mutation
	damage       []damage
	stats        Stats
	recent       []Event
	traceHash    uint64
}

// inode is the content of one file.
// Several names share one inode after a hard link.
type inode struct {
	perm     os.FileMode
	data     []byte     // cache view
	durable  []byte     // durable view
	pending  []change   // unsynced changes, oldest first
	mappings []*mapping // live mappings of the running process
}

// mapping is one live memory mapping of a file.
type mapping struct {
	data    []byte
	follows bool // the mapping shows later writes
}

// poison fills bytes that a real process cannot read any more, after an unmap or past a truncate.
// A later read returns garbage, so the checks find a reader that uses them.
func poison(data []byte) {
	for index := range data {
		data[index] = 0xEE
	}
}

// change is one unsynced write or truncate.
type change struct {
	truncate bool
	offset   int64 // write offset, or the new size of a truncate
	data     []byte
}

// New returns an empty FS.
// The seed selects every fault and crash decision.
// Injected errors start disabled.
// Call [FS.SetFaults] to enable them.
func New(seed uint64, config Config) *FS {
	if config.SectorSize <= 0 {
		config.SectorSize = 512
	}
	if config.KeepChange == Never {
		config.KeepChange = Ratio{Num: 1, Den: 2}
	}
	return &FS{
		config:       config,
		rand:         NewRand(seed),
		names:        make(map[string]*inode),
		durableNames: make(map[string]*inode),
		links:        make(map[string]string),
		traceHash:    14695981039346656037,
	}
}

var _ fileio.FS = (*FS)(nil)

// CloneCache returns a new FS whose files and names are the cache view of fsys.
// It is the storage that a process crash would leave now.
// The clone has no injected errors, so a check can open a second database on it without a change to fsys.
// The clone keeps no durable view, so it holds one copy of each file.
// A power failure on the clone panics, and FileData on the clone returns no durable view.
func (fsys *FS) CloneCache(seed uint64) *FS {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	clone := New(seed, Config{SectorSize: fsys.config.SectorSize, WorkingDir: fsys.config.WorkingDir})
	clone.volatile = true
	copies := make(map[*inode]*inode)
	for _, name := range slices.Sorted(maps.Keys(fsys.names)) {
		node := fsys.names[name]
		copied, ok := copies[node]
		if !ok {
			copied = &inode{perm: node.perm, data: slices.Clone(node.data)}
			copies[node] = copied
		}
		clone.names[name] = copied
		clone.durableNames[name] = copied
	}
	maps.Copy(clone.links, fsys.links)
	return clone
}

// SetFaults enables or disables the injected errors of the Config.
// Crashes do not depend on it.
// With faults disabled, no call uses the random source for an error decision, so checks with faults disabled do not change later decisions.
func (fsys *FS) SetFaults(enabled bool) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fsys.faults = enabled
}

// SetSyncFaults enables or disables the injected sync errors of the Config, while the other injected errors keep their state.
// A failed sync can drop unsynced changes for good, so a caller turns sync errors off while it cannot predict that loss.
func (fsys *FS) SetSyncFaults(enabled bool) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fsys.noSyncFaults = !enabled
}

// ScheduleCrash makes the n-th next mutating call crash the process, for n >= 1.
// That call can take full, partial, or no effect.
// Then every call fails with [ErrCrashed].
// Writes, truncates, syncs, file creation, removal, and directory syncs are mutating calls.
func (fsys *FS) ScheduleCrash(n int) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fsys.crashAt = fsys.mutations + uint64(max(n, 1))
}

// CancelCrash removes a scheduled crash that has not happened.
func (fsys *FS) CancelCrash() {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fsys.crashAt = 0
}

// Crash stops the simulated process now.
// Every call fails with [ErrCrashed] until [FS.Restart].
func (fsys *FS) Crash() {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fsys.crashAt = 0
	if !fsys.crashed {
		fsys.crashed = true
		fsys.crashEvent = Event{Mutation: fsys.mutations, Op: "crash"}
		fsys.record(fsys.crashEvent)
	}
}

// Crashed reports whether the simulated process crashed.
// It also returns the call that crashed.
func (fsys *FS) Crashed() (bool, Event) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	return fsys.crashed, fsys.crashEvent
}

// Restart starts a new simulated process.
// Every earlier file handle fails from now on.
// With powerFailure false, the files keep their cache views, as after a process crash.
// With powerFailure true, each file keeps its durable view and a random part of its unsynced changes, and the result becomes durable.
func (fsys *FS) Restart(powerFailure bool) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if powerFailure && fsys.volatile {
		panic("disk: a power failure on a clone of CloneCache, which keeps no durable view")
	}
	fsys.generation++
	fsys.crashed = false
	fsys.crashAt = 0
	// The process ends, and its mappings with it.
	for _, node := range fsys.names {
		for _, live := range node.mappings {
			poison(live.data)
		}
		node.mappings = nil
	}
	if !powerFailure {
		fsys.stats.ProcessCrashes++
		fsys.record(Event{Mutation: fsys.mutations, Op: "restart", Result: "process crash"})
		return
	}
	fsys.stats.PowerFailures++
	fsys.record(Event{Mutation: fsys.mutations, Op: "restart", Result: "power failure"})

	// Sort the names so that the decisions do not depend on map order.
	names := slices.Sorted(maps.Keys(fsys.names))
	for name := range fsys.durableNames {
		if _, ok := fsys.names[name]; !ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	survivors := make(map[string]*inode, len(names))
	for _, name := range names {
		current, durable := fsys.names[name], fsys.durableNames[name]
		chosen := current
		if current != durable && !fsys.rand.Chance(fsys.config.KeepChange) {
			chosen = durable
			fsys.stats.LostNameChanges++
			fsys.record(Event{Mutation: fsys.mutations, Op: "lose-name-change", Name: name})
		}
		if chosen != nil {
			survivors[name] = chosen
		}
	}
	// Hard links share an inode, which a power failure decides once.
	decided := make(map[*inode]bool)
	for _, name := range slices.Sorted(maps.Keys(survivors)) {
		if node := survivors[name]; !decided[node] {
			decided[node] = true
			fsys.powerFail(name, node)
		}
	}
	fsys.names = survivors
	fsys.durableNames = maps.Clone(survivors)
}

// powerFail replaces the content of node with its durable view and a random part of its unsynced changes.
// The process has ended, so the result reuses the array of the cache view, and the new durable view reuses the array of the old one.
func (fsys *FS) powerFail(name string, node *inode) {
	result := append(node.data[:0], node.durable...)
	if len(node.pending) > 0 {
		if fsys.rand.Chance(fsys.config.InOrder) {
			cut := fsys.rand.IntN(len(node.pending) + 1)
			for index, pending := range node.pending {
				switch {
				case index < cut:
					result = applyChange(result, pending)
				case index == cut && !pending.truncate:
					result = fsys.keepPrefix(name, result, pending)
				default:
					fsys.countLost(name, pending)
				}
			}
		} else {
			// A sector is atomic, so it keeps a prefix of its own writes.
			// stopped holds each sector that lost a write.
			stopped := make(map[int64]bool)
			for _, pending := range node.pending {
				result = fsys.keepPart(name, result, pending, stopped)
			}
		}
		if !strings.HasSuffix(name, "-wal") && fsys.rand.Chance(fsys.config.Misdirect) {
			result = fsys.misdirect(name, node, result)
		}
	}
	node.data = result
	node.durable = append(node.durable[:0], result...)
	node.pending = nil
}

// misdirect moves one unsynced write in the result of a power failure to a wrong offset, like a firmware bug.
// The intended range gets its durable bytes from before the write, and the wrong range gets the write.
// It records the damage, so Heal can restore the correct result.
// It changes result in place and returns it.
func (fsys *FS) misdirect(name string, node *inode, result []byte) []byte {
	var candidates []change
	for _, pending := range node.pending {
		if !pending.truncate && pending.offset+int64(len(pending.data)) <= int64(len(result)) {
			candidates = append(candidates, pending)
		}
	}
	if len(candidates) == 0 {
		return result
	}
	write := candidates[fsys.rand.IntN(len(candidates))]
	shift := int64(fsys.rand.Range(1, 4)) * 4096
	if fsys.rand.Bool() {
		shift = -shift
	}
	target := write.offset + shift
	if target < 0 || target+int64(len(write.data)) > int64(len(result)) {
		return result
	}
	end := write.offset + int64(len(write.data))
	// Only the intended range and the wrong range change, so a copy of the bytes from low to high is enough for the damage runs.
	low, high := min(write.offset, target), max(end, target+int64(len(write.data)))
	correct := slices.Clone(result[low:high])
	older := make([]byte, len(write.data))
	if write.offset < int64(len(node.durable)) {
		copy(older, node.durable[write.offset:min(end, int64(len(node.durable)))])
	}
	copy(result[write.offset:end], older)
	copy(result[target:], write.data)
	if bytes.Equal(correct, result[low:high]) {
		return result
	}
	fsys.damage = append(fsys.damage, damage{node: node, runs: diffRuns(correct, result[low:high], low)})
	fsys.stats.MisdirectedWrites++
	fsys.record(Event{Mutation: fsys.mutations, Op: "misdirect-write", Name: name, Offset: write.offset, Length: len(write.data), Result: fmt.Sprintf("landed at %d", target)})
	return result
}

// damage is a change of the stored bytes of a file that is not a correct result: bit rot or a misdirected write.
// A later write or truncate replaces the damage in its range, so it drops the runs there.
type damage struct {
	node *inode
	runs []damageRun
}

// damageRun is one range of damaged bytes and its correct bytes.
type damageRun struct {
	offset  int64
	correct []byte
}

// diffRuns returns the ranges where damaged differs from correct, with the correct bytes.
// Both have one length, and both start at the file offset base.
func diffRuns(correct, damaged []byte, base int64) []damageRun {
	var runs []damageRun
	for index := 0; index < len(correct); {
		if correct[index] == damaged[index] {
			index++
			continue
		}
		start := index
		for index < len(correct) && correct[index] != damaged[index] {
			index++
		}
		runs = append(runs, damageRun{offset: base + int64(start), correct: slices.Clone(correct[start:index])})
	}
	return runs
}

// forgetDamage cuts from the damage runs of node the bytes that a write of length bytes at offset replaces, or that a truncate to offset removes (length -1).
// The parts of a run outside that range stay damaged.
// The caller must hold mu.
func (fsys *FS) forgetDamage(node *inode, offset int64, length int) {
	end := int64(math.MaxInt64)
	if length >= 0 {
		end = offset + int64(length)
	}
	for index := range fsys.damage {
		if fsys.damage[index].node != node {
			continue
		}
		var kept []damageRun
		for _, run := range fsys.damage[index].runs {
			runEnd := run.offset + int64(len(run.correct))
			if runEnd <= offset || end <= run.offset {
				kept = append(kept, run)
				continue
			}
			if run.offset < offset {
				kept = append(kept, damageRun{offset: run.offset, correct: run.correct[:offset-run.offset]})
			}
			if end < runEnd {
				kept = append(kept, damageRun{offset: end, correct: run.correct[end-run.offset:]})
			}
		}
		fsys.damage[index].runs = kept
	}
}

// Corrupt flips bits random bits of the stored bytes of name between start and end, as bit rot does.
// It changes the durable and the cache view, and records the damage for Heal.
// It reports whether it changed the file.
func (fsys *FS) Corrupt(name string, start, end int64, bits int) bool {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	node, ok := fsys.names[fsys.follow(name)]
	if !ok || len(node.pending) != 0 {
		return false
	}
	end = min(end, int64(len(node.durable)))
	if start < 0 || start >= end {
		return false
	}
	// Only the bytes from start to end change, so a copy of them is enough for the damage runs.
	correct := slices.Clone(node.durable[start:end])
	for range bits {
		offset := start + int64(fsys.rand.Uint64N(uint64(end-start)))
		node.durable[offset] ^= 1 << fsys.rand.IntN(8)
	}
	if bytes.Equal(correct, node.durable[start:end]) {
		return false
	}
	// No other slice holds the array of the cache view, so the copy can reuse it.
	node.data = append(node.data[:0], node.durable...)
	refreshMappings(node)
	fsys.damage = append(fsys.damage, damage{node: node, runs: diffRuns(correct, node.durable[start:end], start)})
	fsys.stats.BitRot++
	fsys.record(Event{Mutation: fsys.mutations, Op: "bit-rot", Name: name, Offset: start, Length: int(end - start), Result: fmt.Sprintf("%d bits", bits)})
	return true
}

// Durable reports whether storage holds the cache view: no file has unsynced changes, and no name change waits for a directory sync.
// A power failure then keeps every file exactly as the running process sees it.
func (fsys *FS) Durable() bool {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if len(fsys.names) != len(fsys.durableNames) {
		return false
	}
	for name, node := range fsys.names {
		if fsys.durableNames[name] != node || len(node.pending) != 0 {
			return false
		}
	}
	return true
}

// Damaged reports whether some damage is not healed yet and no write replaced it.
func (fsys *FS) Damaged() bool {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	for _, damaged := range fsys.damage {
		if len(damaged.runs) != 0 {
			return true
		}
	}
	return false
}

// Heal restores the correct bytes of each damage run that no later write or truncate replaced.
// It undoes the newest damage first, because two damages can change the same bytes.
func (fsys *FS) Heal() {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	for _, damaged := range slices.Backward(fsys.damage) {
		node := damaged.node
		for _, run := range damaged.runs {
			for _, data := range [][]byte{node.data, node.durable} {
				if run.offset+int64(len(run.correct)) <= int64(len(data)) {
					copy(data[run.offset:], run.correct)
				}
			}
		}
		refreshMappings(node)
		fsys.record(Event{Mutation: fsys.mutations, Op: "heal", Length: len(damaged.runs)})
	}
	fsys.damage = nil
}

func (fsys *FS) countLost(name string, pending change) {
	if pending.truncate {
		fsys.stats.LostTruncates++
		fsys.record(Event{Mutation: fsys.mutations, Op: "lose-truncate", Name: name, Offset: pending.offset})
		return
	}
	fsys.stats.LostWrites++
	fsys.record(Event{Mutation: fsys.mutations, Op: "lose-write", Name: name, Offset: pending.offset, Length: len(pending.data)})
}

// applyChange applies one complete change to data and returns the result.
func applyChange(data []byte, pending change) []byte {
	if pending.truncate {
		return resize(data, pending.offset)
	}
	end := pending.offset + int64(len(pending.data))
	if end > int64(len(data)) {
		data = resize(data, end)
	}
	copy(data[pending.offset:], pending.data)
	return data
}

// keepPrefix keeps the sectors of a write before a random sector boundary.
// A power failure in the middle of an ordered write-back does this.
func (fsys *FS) keepPrefix(name string, data []byte, pending change) []byte {
	boundaries := sectorBoundaries(pending.offset, len(pending.data), fsys.config.SectorSize)
	keep := boundaries[fsys.rand.IntN(len(boundaries))]
	if keep == len(pending.data) {
		return applyChange(data, pending)
	}
	if keep == 0 {
		fsys.countLost(name, pending)
		return data
	}
	fsys.stats.TornWrites++
	fsys.record(Event{Mutation: fsys.mutations, Op: "tear-write", Name: name, Offset: pending.offset, Length: keep})
	return applyChange(data, change{offset: pending.offset, data: pending.data[:keep]})
}

// keepPart decides each sector of a write, or a complete truncate, independently.
// A sector in stopped lost an earlier write, so it also loses this one.
func (fsys *FS) keepPart(name string, data []byte, pending change, stopped map[int64]bool) []byte {
	keepRatio := fsys.config.KeepChange
	if pending.truncate {
		if fsys.rand.Chance(keepRatio) {
			return resize(data, pending.offset)
		}
		fsys.countLost(name, pending)
		return data
	}
	end := pending.offset + int64(len(pending.data))
	// The length of an extended file can reach storage before its data.
	if end > int64(len(data)) && fsys.rand.Chance(keepRatio) {
		data = fsys.extend(data, end)
	}
	kept := 0
	sector := int64(fsys.config.SectorSize)
	for start := pending.offset; start < end; {
		stop := min(end, (start/sector+1)*sector)
		index := start / sector
		if !stopped[index] && !fsys.rand.Chance(keepRatio) {
			stopped[index] = true
			if fsys.config.TornSectors && stop-start > 1 && fsys.rand.Bool() {
				// A torn sector keeps a byte prefix of this write, and then nothing more.
				torn := start + int64(fsys.rand.Range(1, int(stop-start)-1))
				if torn > int64(len(data)) {
					data = fsys.extend(data, torn)
				}
				copy(data[start:], pending.data[start-pending.offset:torn-pending.offset])
				kept += int(torn - start)
				fsys.stats.TornSectors++
			}
		}
		if !stopped[index] {
			if stop > int64(len(data)) {
				data = fsys.extend(data, stop)
			}
			copy(data[start:], pending.data[start-pending.offset:stop-pending.offset])
			kept += int(stop - start)
		}
		start = stop
	}
	switch {
	case kept == 0:
		fsys.countLost(name, pending)
	case kept < len(pending.data):
		fsys.stats.TornWrites++
		fsys.record(Event{Mutation: fsys.mutations, Op: "tear-write", Name: name, Offset: pending.offset, Length: kept})
	}
	return data
}

// extend makes data size bytes long.
// The new space holds zeros, as ext4 and XFS give after a crash, or random bytes with Config.Garbage.
func (fsys *FS) extend(data []byte, size int64) []byte {
	old := len(data)
	data = resize(data, size)
	if fsys.config.Garbage && int(size) > old {
		fsys.rand.Fill(data[old:])
		fsys.stats.GarbageFills++
	}
	return data
}

// sectorBoundaries returns each possible kept length of a write: zero, each sector boundary inside the write, and the full length.
func sectorBoundaries(offset int64, length, sectorSize int) []int {
	boundaries := []int{0}
	sector := int64(sectorSize)
	for next := (offset/sector + 1) * sector; next < offset+int64(length); next += sector {
		boundaries = append(boundaries, int(next-offset))
	}
	if length > 0 {
		boundaries = append(boundaries, length)
	}
	return boundaries
}

func resize(data []byte, size int64) []byte {
	if size <= int64(len(data)) {
		return data[:size]
	}
	return append(data, make([]byte, size-int64(len(data)))...)
}

// Stats returns the counters of this FS.
func (fsys *FS) Stats() Stats {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	return fsys.stats
}

// TraceHash returns a hash of all events so far.
// Two runs with the same seed and calls have the same hash.
func (fsys *FS) TraceHash() uint64 {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	return fsys.traceHash
}

// RecentEvents returns the last events, oldest first.
func (fsys *FS) RecentEvents() []Event {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	return slices.Clone(fsys.recent)
}

// Mutations returns the number of mutating calls so far.
func (fsys *FS) Mutations() uint64 {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	return fsys.mutations
}

// FileData returns a copy of the cache view and the durable view of name.
func (fsys *FS) FileData(name string) (cache, durable []byte, ok bool) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	node, ok := fsys.names[fsys.follow(name)]
	if !ok {
		return nil, nil, false
	}
	return slices.Clone(node.data), slices.Clone(node.durable), true
}

// FileSize returns the size of the cache view of the file at name, and whether the file exists.
// It copies nothing, so the checks after every step can use it on large files.
func (fsys *FS) FileSize(name string) (int64, bool) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	node, ok := fsys.names[fsys.follow(name)]
	if !ok {
		return 0, false
	}
	return int64(len(node.data)), true
}

func (fsys *FS) record(event Event) {
	if fsys.config.OnEvent != nil {
		fsys.config.OnEvent(event)
	}
	if len(fsys.recent) == maxRecentEvents {
		copy(fsys.recent, fsys.recent[1:])
		fsys.recent = fsys.recent[:maxRecentEvents-1]
	}
	fsys.recent = append(fsys.recent, event)
	for _, value := range []uint64{event.Mutation, uint64(event.Offset), uint64(event.Length)} {
		fsys.traceHash = (fsys.traceHash ^ value) * 1099511628211
	}
	for _, text := range []string{event.Op, filepath.Base(event.Name), event.Result} {
		for index := range len(text) {
			fsys.traceHash = (fsys.traceHash ^ uint64(text[index])) * 1099511628211
		}
	}
}

// mutate counts one mutating call.
// It reports true when this call is the scheduled crash.
// The caller must hold mu and must not be crashed.
func (fsys *FS) mutate(op, name string, offset int64, length int) bool {
	fsys.mutations++
	if fsys.crashAt == 0 || fsys.mutations < fsys.crashAt {
		return false
	}
	fsys.crashAt = 0
	fsys.crashed = true
	fsys.stats.ScheduledCrashes++
	fsys.crashEvent = Event{Mutation: fsys.mutations, Op: op, Name: name, Offset: offset, Length: length, Result: "crash"}
	return true
}

// inject reports whether this call returns an injected error.
// The caller must hold mu.
func (fsys *FS) inject(ratio Ratio) bool {
	if !fsys.faults || !fsys.rand.Chance(ratio) {
		return false
	}
	fsys.stats.InjectedFaults++
	return true
}

func injectedError(op, name string) error {
	return &fs.PathError{Op: op, Path: name, Err: fmt.Errorf("%w: %w", ErrInjected, syscall.EIO)}
}

func crashedError(op, name string) error {
	return &fs.PathError{Op: op, Path: name, Err: ErrCrashed}
}

// OpenFile opens name.
// It supports os.O_RDONLY, os.O_RDWR, os.O_WRONLY, os.O_CREATE, and os.O_EXCL.
func (fsys *FS) OpenFile(name string, flag int, perm os.FileMode) (fileio.File, error) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	_, link := fsys.links[fsys.abs(name)]
	name = fsys.follow(name)
	if fsys.crashed {
		return nil, crashedError("open", name)
	}
	if flag&^(os.O_RDONLY|os.O_RDWR|os.O_WRONLY|os.O_CREATE|os.O_EXCL) != 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errors.ErrUnsupported}
	}
	if fsys.inject(fsys.config.OpenFault) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "open", Name: name, Result: "injected"})
		return nil, injectedError("open", name)
	}
	node, ok := fsys.names[name]
	// As in open(2), O_CREATE with O_EXCL fails for an existing name, and also for a symbolic link at the name.
	if flag&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL && (ok || link) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "open", Name: name, Result: "exist"})
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrExist}
	}
	if !ok {
		if flag&os.O_CREATE == 0 {
			fsys.record(Event{Mutation: fsys.mutations, Op: "open", Name: name, Result: "not exist"})
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		if fsys.mutate("create", name, 0, 0) {
			// The name can exist or not after the crash.
			if fsys.rand.Bool() {
				fsys.create(name, perm)
			}
			fsys.record(fsys.crashEvent)
			return nil, crashedError("open", name)
		}
		node = fsys.create(name, perm)
		fsys.record(Event{Mutation: fsys.mutations, Op: "create", Name: name, Result: "ok"})
	} else {
		fsys.record(Event{Mutation: fsys.mutations, Op: "open", Name: name, Result: "ok"})
	}
	return &file{
		fsys:       fsys,
		name:       name,
		node:       node,
		generation: fsys.generation,
		writable:   flag&(os.O_RDWR|os.O_WRONLY) != 0,
	}, nil
}

func (fsys *FS) create(name string, perm os.FileMode) *inode {
	node := &inode{perm: perm.Perm()}
	fsys.names[name] = node
	return node
}

// Remove removes name.
// Open handles keep the file content.
func (fsys *FS) Remove(name string) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	name = fsys.abs(name)
	if fsys.crashed {
		return crashedError("remove", name)
	}
	if _, ok := fsys.names[name]; !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	if fsys.mutate("remove", name, 0, 0) {
		if fsys.rand.Bool() {
			delete(fsys.names, name)
		}
		fsys.record(fsys.crashEvent)
		return crashedError("remove", name)
	}
	if fsys.inject(fsys.config.RemoveFault) {
		if fsys.rand.Bool() {
			delete(fsys.names, name)
		}
		fsys.record(Event{Mutation: fsys.mutations, Op: "remove", Name: name, Result: "injected"})
		return injectedError("remove", name)
	}
	delete(fsys.names, name)
	fsys.record(Event{Mutation: fsys.mutations, Op: "remove", Name: name, Result: "ok"})
	return nil
}

// Discard removes name from the cache view and from the durable view, so no crash or power failure can bring it back.
// It is not a call of the simulated process: it counts no mutation, records no event, and takes no random decision.
// The checks use it for their own files, such as backups that they do not need any more.
func (fsys *FS) Discard(name string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	name = fsys.abs(name)
	delete(fsys.names, name)
	delete(fsys.durableNames, name)
}

// EvalSymlinks returns the absolute name of the file that name names after it follows symbolic links.
// The file must exist.
func (fsys *FS) EvalSymlinks(name string) (string, error) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	resolved := fsys.follow(name)
	if fsys.crashed {
		return "", crashedError("lstat", name)
	}
	if _, ok := fsys.names[resolved]; !ok {
		return "", &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrNotExist}
	}
	return resolved, nil
}

// abs returns the absolute, clean form of name.
func (fsys *FS) abs(name string) string {
	if !filepath.IsAbs(name) {
		dir := fsys.config.WorkingDir
		if dir == "" {
			dir = "/"
		}
		name = filepath.Join(dir, name)
	}
	return filepath.Clean(name)
}

// follow returns the absolute name after it follows the symbolic links of the last name element.
// The caller must hold mu.
func (fsys *FS) follow(name string) string {
	name = fsys.abs(name)
	for range 8 {
		target, ok := fsys.links[name]
		if !ok {
			return name
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(name), target)
		}
		name = filepath.Clean(target)
	}
	return name
}

// Abs returns the absolute form of name. A relative name starts at Config.WorkingDir.
func (fsys *FS) Abs(name string) (string, error) {
	return fsys.abs(name), nil
}

// PageSize returns the page size of the simulated operating system: the cache page that a crash can split a write at.
// It does not read the page size of the host, so a seed gives the same run on every host.
func (fsys *FS) PageSize() int {
	return cachePageSize
}

// Symlink makes name a symbolic link to target, like os.Symlink.
// Symbolic links are always durable; Blackout makes them only before a run.
// Remove and Link do not check for symbolic links, because no caller removes one or makes a hard link at its name.
func (fsys *FS) Symlink(target, name string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fsys.links[fsys.abs(name)] = target
}

// Lstat returns the file information of name without following a symbolic link.
func (fsys *FS) Lstat(name string) (os.FileInfo, error) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	name = fsys.abs(name)
	if fsys.crashed {
		return nil, crashedError("lstat", name)
	}
	if fsys.inject(fsys.config.StatFault) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "lstat", Name: name, Result: "injected"})
		return nil, injectedError("lstat", name)
	}
	if _, ok := fsys.links[name]; ok {
		return fileInfo{name: filepath.Base(name), mode: os.ModeSymlink | 0o777}, nil
	}
	node, ok := fsys.names[name]
	if !ok {
		return nil, &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrNotExist}
	}
	return fileInfo{name: filepath.Base(name), size: int64(len(node.data)), mode: node.perm}, nil
}

// CreateTemp creates a new file in dir whose name is pattern with its last "*" replaced by a random number.
func (fsys *FS) CreateTemp(dir, pattern string) (fileio.File, string, error) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	dir = fsys.abs(dir)
	prefix, suffix := pattern, ""
	if index := strings.LastIndex(pattern, "*"); index >= 0 {
		prefix, suffix = pattern[:index], pattern[index+1:]
	}
	var name string
	for {
		name = filepath.Join(dir, fmt.Sprintf("%s%d%s", prefix, fsys.rand.Uint64N(1_000_000_000), suffix))
		if _, ok := fsys.names[name]; !ok {
			break
		}
	}
	if fsys.crashed {
		return nil, "", crashedError("open", name)
	}
	if fsys.inject(fsys.config.OpenFault) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "create", Name: name, Result: "injected"})
		return nil, "", injectedError("open", name)
	}
	if fsys.mutate("create", name, 0, 0) {
		if fsys.rand.Bool() {
			fsys.create(name, 0o600)
		}
		fsys.record(fsys.crashEvent)
		return nil, "", crashedError("open", name)
	}
	node := fsys.create(name, 0o600)
	fsys.record(Event{Mutation: fsys.mutations, Op: "create", Name: name, Result: "ok"})
	return &file{fsys: fsys, name: name, node: node, generation: fsys.generation, writable: true}, name, nil
}

// Link makes newname a second name of the file oldname.
// Like any new name, it is durable only after a directory sync.
func (fsys *FS) Link(oldname, newname string) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	oldname, newname = fsys.follow(oldname), fsys.abs(newname)
	if fsys.crashed {
		return crashedError("link", newname)
	}
	node, ok := fsys.names[oldname]
	if !ok {
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: fs.ErrNotExist}
	}
	if _, ok := fsys.names[newname]; ok {
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: fs.ErrExist}
	}
	if fsys.mutate("link", newname, 0, 0) {
		if fsys.rand.Bool() {
			fsys.names[newname] = node
		}
		fsys.record(fsys.crashEvent)
		return crashedError("link", newname)
	}
	if fsys.inject(fsys.config.OpenFault) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "link", Name: newname, Result: "injected"})
		return injectedError("link", newname)
	}
	fsys.names[newname] = node
	fsys.record(Event{Mutation: fsys.mutations, Op: "link", Name: newname, Result: "ok"})
	return nil
}

// Chmod changes the permission bits of name.
func (fsys *FS) Chmod(name string, mode os.FileMode) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	name = fsys.follow(name)
	if fsys.crashed {
		return crashedError("chmod", name)
	}
	node, ok := fsys.names[name]
	if !ok {
		return &fs.PathError{Op: "chmod", Path: name, Err: fs.ErrNotExist}
	}
	node.perm = mode.Perm()
	return nil
}

// Perm returns the permission bits of the file that name names.
func (fsys *FS) Perm(name string) (os.FileMode, bool) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	node, ok := fsys.names[fsys.follow(name)]
	if !ok {
		return 0, false
	}
	return node.perm, true
}

// Names returns the name of every file in the cache view, in order.
func (fsys *FS) Names() []string {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	return slices.Sorted(maps.Keys(fsys.names))
}

// SyncDir makes the current names in directory dir durable.
func (fsys *FS) SyncDir(dir string) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	dir = fsys.abs(dir)
	if fsys.crashed {
		return crashedError("sync", dir)
	}
	names := fsys.namesIn(dir)
	if fsys.mutate("syncdir", dir, 0, len(names)) {
		if fsys.rand.Bool() {
			fsys.persistNames(names, Always)
		}
		fsys.record(fsys.crashEvent)
		return crashedError("sync", dir)
	}
	if fsys.inject(fsys.config.SyncDirFault) {
		fsys.persistNames(names, Ratio{Num: 1, Den: 2})
		fsys.record(Event{Mutation: fsys.mutations, Op: "syncdir", Name: dir, Result: "injected"})
		return injectedError("sync", dir)
	}
	if fsys.config.SyncLie != SyncLieDirectory {
		fsys.persistNames(names, Always)
	}
	fsys.record(Event{Mutation: fsys.mutations, Op: "syncdir", Name: dir, Length: len(names), Result: "ok"})
	return nil
}

// namesIn returns, in sorted order, each name in directory dir that the cache view or the durable view holds.
// The caller must hold mu.
func (fsys *FS) namesIn(dir string) []string {
	var names []string
	for name := range fsys.names {
		if filepath.Dir(name) == dir {
			names = append(names, name)
		}
	}
	for name := range fsys.durableNames {
		if _, ok := fsys.names[name]; !ok && filepath.Dir(name) == dir {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// persistNames makes each name change durable with the probability keep.
// The caller must hold mu.
func (fsys *FS) persistNames(names []string, keep Ratio) {
	for _, name := range names {
		if !fsys.rand.Chance(keep) {
			continue
		}
		if node, ok := fsys.names[name]; ok {
			fsys.durableNames[name] = node
		} else {
			delete(fsys.durableNames, name)
		}
	}
}

// file is an open handle.
// It implements [fileio.File].
type file struct {
	fsys       *FS
	name       string
	node       *inode
	generation uint64
	writable   bool
	closed     bool
}

var (
	_ fileio.File   = (*file)(nil)
	_ fileio.Mapper = (*file)(nil)
)

// check returns the error of a call on a closed handle, a handle from before a restart, or a crashed process.
// The caller must hold mu.
func (f *file) check(op string) error {
	if f.closed {
		return &fs.PathError{Op: op, Path: f.name, Err: fs.ErrClosed}
	}
	if f.fsys.crashed || f.generation != f.fsys.generation {
		return crashedError(op, f.name)
	}
	return nil
}

func (f *file) ReadAt(p []byte, offset int64) (int, error) {
	fsys := f.fsys
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if err := f.check("read"); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: syscall.EINVAL}
	}
	if fsys.inject(fsys.config.ReadFault) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "read", Name: f.name, Offset: offset, Length: len(p), Result: "injected"})
		return 0, injectedError("read", f.name)
	}
	if offset >= int64(len(f.node.data)) {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n := copy(p, f.node.data[offset:])
	if n > 0 && !strings.HasSuffix(f.name, "-wal") && fsys.faults && fsys.rand.Chance(fsys.config.ReadCorruption) {
		bit := fsys.rand.IntN(n * 8)
		p[bit/8] ^= 1 << (bit % 8)
		fsys.stats.ReadCorruptions++
		fsys.record(Event{Mutation: fsys.mutations, Op: "read", Name: f.name, Offset: offset, Length: len(p), Result: fmt.Sprintf("bit %d flipped", bit)})
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// noSpace starts or continues a disk-full episode for a call that grows a file to size.
// It reports whether the call must fail with ENOSPC.
// The caller must hold mu.
func (fsys *FS) noSpace(node *inode, size int64) bool {
	if !fsys.faults || size <= int64(len(node.data)) {
		return false
	}
	if fsys.mutations >= fsys.fullUntil {
		if !fsys.rand.Chance(fsys.config.FullFault) {
			return false
		}
		fsys.fullUntil = fsys.mutations + uint64(fsys.rand.Range(1, 30))
	}
	fsys.stats.NoSpaceErrors++
	fsys.stats.InjectedFaults++
	return true
}

// noSpaceError is the error of a call that needs space during a disk-full episode.
// It matches syscall.ENOSPC and ErrInjected.
type noSpaceError struct{}

func (noSpaceError) Error() string { return "disk: no space left on device (injected)" }

func (noSpaceError) Is(target error) bool { return target == ErrInjected || target == syscall.ENOSPC }

func (f *file) WriteAt(p []byte, offset int64) (int, error) {
	fsys := f.fsys
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if err := f.check("write"); err != nil {
		return 0, err
	}
	if !f.writable {
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: syscall.EBADF}
	}
	if offset < 0 {
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: syscall.EINVAL}
	}
	if fsys.mutate("write", f.name, offset, len(p)) {
		// A process crash can stop a write after any page of the cache.
		boundaries := sectorBoundaries(offset, len(p), cachePageSize)
		n := boundaries[fsys.rand.IntN(len(boundaries))]
		fsys.write(f.node, p[:n], offset)
		fsys.crashEvent.Result = fmt.Sprintf("crash after %d bytes", n)
		fsys.record(fsys.crashEvent)
		return n, crashedError("write", f.name)
	}
	if fsys.inject(fsys.config.WriteFault) {
		boundaries := sectorBoundaries(offset, len(p), cachePageSize)
		n := boundaries[fsys.rand.IntN(len(boundaries))]
		fsys.write(f.node, p[:n], offset)
		fsys.record(Event{Mutation: fsys.mutations, Op: "write", Name: f.name, Offset: offset, Length: len(p), Result: fmt.Sprintf("injected after %d bytes", n)})
		return n, injectedError("write", f.name)
	}
	if fsys.noSpace(f.node, offset+int64(len(p))) {
		// The part inside the current length needs no new space.
		n := int(max(0, min(int64(len(p)), int64(len(f.node.data))-offset)))
		fsys.write(f.node, p[:n], offset)
		fsys.record(Event{Mutation: fsys.mutations, Op: "write", Name: f.name, Offset: offset, Length: len(p), Result: fmt.Sprintf("no space after %d bytes", n)})
		return n, &fs.PathError{Op: "write", Path: f.name, Err: noSpaceError{}}
	}
	fsys.write(f.node, p, offset)
	fsys.record(Event{Mutation: fsys.mutations, Op: "write", Name: f.name, Offset: offset, Length: len(p), Result: "ok"})
	return len(p), nil
}

// write changes the cache view and records the unsynced change.
// The caller must hold mu.
func (fsys *FS) write(node *inode, p []byte, offset int64) {
	if len(p) == 0 {
		return
	}
	if fsys.volatile {
		node.data = applyChange(node.data, change{offset: offset, data: p})
	} else {
		pending := change{offset: offset, data: slices.Clone(p)}
		node.data = applyChange(node.data, pending)
		node.pending = append(node.pending, pending)
	}
	fsys.forgetDamage(node, offset, len(p))
	for _, live := range node.mappings {
		if live.follows && offset < int64(len(live.data)) {
			copy(live.data[offset:], p)
		}
	}
}

// truncate changes the cache view and records the unsynced change.
// The caller must hold mu.
func (fsys *FS) truncate(node *inode, size int64) {
	node.data = resize(node.data, size)
	if !fsys.volatile {
		node.pending = append(node.pending, change{truncate: true, offset: size})
	}
	fsys.forgetDamage(node, size, -1)
	for _, live := range node.mappings {
		if int64(len(live.data)) > size {
			fsys.stats.MappingViolations++
			fsys.record(Event{Mutation: fsys.mutations, Op: "truncate-under-mapping", Offset: size, Length: len(live.data)})
			poison(live.data[size:])
		}
	}
}

// refreshMappings copies the cache view into each mapping that follows writes.
// The caller must hold mu.
func refreshMappings(node *inode) {
	for _, live := range node.mappings {
		if !live.follows {
			continue
		}
		copied := copy(live.data, node.data)
		poison(live.data[copied:])
	}
}

// Map maps the first length bytes of the file for reads. It implements [fileio.Mapper].
func (f *file) Map(length int) ([]byte, error) {
	fsys := f.fsys
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if err := f.check("map"); err != nil {
		return nil, err
	}
	if fsys.config.DisableMap {
		return nil, &fs.PathError{Op: "map", Path: f.name, Err: errors.ErrUnsupported}
	}
	if fsys.inject(fsys.config.MapFault) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "map", Name: f.name, Length: length, Result: "injected"})
		return nil, injectedError("map", f.name)
	}
	if length <= 0 || length > len(f.node.data) {
		return nil, &fs.PathError{Op: "map", Path: f.name, Err: syscall.EINVAL}
	}
	live := &mapping{data: slices.Clone(f.node.data[:length]), follows: !fsys.config.StaleMappings}
	f.node.mappings = append(f.node.mappings, live)
	fsys.record(Event{Mutation: fsys.mutations, Op: "map", Name: f.name, Length: length, Result: "ok"})
	return live.data, nil
}

// Unmap releases a mapping that Map returned. It implements [fileio.Mapper].
func (f *file) Unmap(data []byte) error {
	fsys := f.fsys
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	for index, live := range f.node.mappings {
		if len(live.data) == len(data) && unsafe.SliceData(live.data) == unsafe.SliceData(data) {
			poison(live.data)
			f.node.mappings = slices.Delete(f.node.mappings, index, index+1)
			fsys.record(Event{Mutation: fsys.mutations, Op: "unmap", Name: f.name, Length: len(data), Result: "ok"})
			return nil
		}
	}
	return &fs.PathError{Op: "unmap", Path: f.name, Err: syscall.EINVAL}
}

// MappingFollowsWrites reports whether a mapping shows later writes. It implements [fileio.Mapper].
func (f *file) MappingFollowsWrites() bool {
	return !f.fsys.config.StaleMappings
}

func (f *file) Truncate(size int64) error {
	fsys := f.fsys
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if err := f.check("truncate"); err != nil {
		return err
	}
	if !f.writable {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: syscall.EBADF}
	}
	if size < 0 {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: syscall.EINVAL}
	}
	if fsys.mutate("truncate", f.name, size, 0) {
		if fsys.rand.Bool() {
			fsys.truncate(f.node, size)
		}
		fsys.record(fsys.crashEvent)
		return crashedError("truncate", f.name)
	}
	if fsys.inject(fsys.config.TruncateFault) {
		result := "injected, not applied"
		if fsys.rand.Bool() {
			fsys.truncate(f.node, size)
			result = "injected, applied"
		}
		fsys.record(Event{Mutation: fsys.mutations, Op: "truncate", Name: f.name, Offset: size, Result: result})
		return injectedError("truncate", f.name)
	}
	if fsys.noSpace(f.node, size) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "truncate", Name: f.name, Offset: size, Result: "no space"})
		return &fs.PathError{Op: "truncate", Path: f.name, Err: noSpaceError{}}
	}
	fsys.truncate(f.node, size)
	fsys.record(Event{Mutation: fsys.mutations, Op: "truncate", Name: f.name, Offset: size, Result: "ok"})
	return nil
}

// Sync makes the unsynced changes of the file durable.
func (f *file) Sync() error {
	fsys := f.fsys
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if err := f.check("sync"); err != nil {
		return err
	}
	node := f.node
	if fsys.mutate("sync", f.name, 0, len(node.pending)) {
		// The process can crash after the sync completes but before it returns.
		if fsys.rand.Bool() {
			syncChanges(node)
		}
		fsys.record(fsys.crashEvent)
		return crashedError("sync", f.name)
	}
	if !fsys.noSyncFaults && fsys.inject(fsys.config.SyncFault) {
		if fsys.config.RevertFailedSync {
			// Like Btrfs after a writeback error: the file goes back to its durable data and metadata, and the cache shows those bytes again.
			if len(node.pending) > 0 {
				fsys.stats.DroppedSyncs++
			}
			node.pending = nil
			result := "injected"
			if !bytes.Equal(node.data, node.durable) {
				// What reads see changes, as after a dropped cache.
				fsys.stats.RevertedSyncs++
				fsys.stats.DroppedCaches++
				result = "injected, reverted"
				// No other slice holds the array of the cache view, so the copy can reuse it.
				node.data = append(node.data[:0], node.durable...)
				refreshMappings(node)
			}
			fsys.record(Event{Mutation: fsys.mutations, Op: "sync", Name: f.name, Result: result})
			return injectedError("sync", f.name)
		}
		// Like Linux after a writeback error: some writes reach storage, the kernel marks the others clean, and a later sync does not write them.
		// A truncate changes metadata, and the file-system journal keeps it, so it reaches storage.
		for _, pending := range node.pending {
			if pending.truncate || fsys.rand.Bool() {
				node.durable = applyChange(node.durable, pending)
			}
		}
		if len(node.pending) > 0 {
			fsys.stats.DroppedSyncs++
		}
		node.pending = nil
		result := "injected"
		if fsys.rand.Chance(fsys.config.DropCache) {
			if !bytes.Equal(node.data, node.durable) {
				fsys.stats.DroppedCaches++
				result = "injected, cache dropped"
			}
			// No other slice holds the array of the cache view, so the copy can reuse it.
			node.data = append(node.data[:0], node.durable...)
			refreshMappings(node)
		}
		fsys.record(Event{Mutation: fsys.mutations, Op: "sync", Name: f.name, Result: result})
		return injectedError("sync", f.name)
	}
	lie := fsys.config.SyncLie == SyncLieAllFiles || fsys.config.SyncLie == SyncLieMainFile && !strings.HasSuffix(f.name, "-wal")
	if !lie {
		syncChanges(node)
	}
	fsys.record(Event{Mutation: fsys.mutations, Op: "sync", Name: f.name, Result: "ok"})
	return nil
}

// syncChanges applies the unsynced changes to the durable view.
// The caller must hold mu.
func syncChanges(node *inode) {
	for _, pending := range node.pending {
		node.durable = applyChange(node.durable, pending)
	}
	node.pending = nil
}

func (f *file) Stat() (os.FileInfo, error) {
	fsys := f.fsys
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if err := f.check("stat"); err != nil {
		return nil, err
	}
	if fsys.inject(fsys.config.StatFault) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "stat", Name: f.name, Result: "injected"})
		return nil, injectedError("stat", f.name)
	}
	return fileInfo{name: filepath.Base(f.name), size: int64(len(f.node.data)), mode: f.node.perm}, nil
}

func (f *file) Close() error {
	fsys := f.fsys
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	return nil
}

type fileInfo struct {
	name string
	size int64
	mode os.FileMode
}

func (info fileInfo) Name() string       { return info.name }
func (info fileInfo) Size() int64        { return info.size }
func (info fileInfo) Mode() os.FileMode  { return info.mode }
func (info fileInfo) ModTime() time.Time { return time.Time{} }
func (info fileInfo) IsDir() bool        { return false }
func (info fileInfo) Sys() any           { return nil }
