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
// The model has these limits: no memory mapping, no file locks, no hard links or renames, and one level of directories.
package disk

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Issaminu/kvlite/internal/fileio"
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

	// Probabilities of an injected error, for each call while faults are enabled.
	ReadFault     Ratio
	WriteFault    Ratio // A failed write can still change a prefix of its bytes.
	SyncFault     Ratio // A failed sync can make some changes durable and drop the others for good.
	TruncateFault Ratio // A failed truncate can still take effect.
	StatFault     Ratio
	OpenFault     Ratio
	RemoveFault   Ratio // A failed remove can still take effect.
	SyncDirFault  Ratio // A failed directory sync can make some names durable.

	// DropCache is the probability that a failed sync also drops the dropped changes from the cache view, so reads return the durable bytes.
	DropCache Ratio

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
	ProcessCrashes   int // restarts that kept the cache view
	PowerFailures    int // restarts that kept only the durable view and a random part of the unsynced changes
	ScheduledCrashes int // crashes inside a call
	TornWrites       int // writes that a power failure kept only in part
	LostWrites       int // writes that a power failure lost completely
	LostTruncates    int // truncates that a power failure lost
	LostNameChanges  int // name changes that a power failure lost
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
	generation   uint64            // a restart increments it, so earlier handles fail
	nextInodeID  uint64
	mutations    uint64 // mutating calls so far; a scheduled crash counts them
	crashAt      uint64 // mutation that crashes; zero means none
	crashed      bool
	crashEvent   Event
	faults       bool // enables the injected errors of Config
	stats        Stats
	recent       []Event
	traceHash    uint64
}

// inode is the content of one file.
// Several names never share one inode.
type inode struct {
	id      uint64
	perm    os.FileMode
	data    []byte   // cache view
	durable []byte   // durable view
	pending []change // unsynced changes, oldest first
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
		traceHash:    14695981039346656037,
	}
}

var _ fileio.FS = (*FS)(nil)

// CloneCache returns a new FS whose files and names are the cache view of fsys, all durable.
// It is the storage that a process crash would leave now.
// The clone has no injected errors, so a check can open a second database on it without a change to fsys.
func (fsys *FS) CloneCache(seed uint64) *FS {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	clone := New(seed, Config{SectorSize: fsys.config.SectorSize})
	for name, node := range fsys.names {
		clone.nextInodeID++
		copied := &inode{id: clone.nextInodeID, perm: node.perm, data: slices.Clone(node.data), durable: slices.Clone(node.data)}
		clone.names[name] = copied
		clone.durableNames[name] = copied
	}
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
	fsys.generation++
	fsys.crashed = false
	fsys.crashAt = 0
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
	for _, name := range slices.Sorted(maps.Keys(survivors)) {
		fsys.powerFail(name, survivors[name])
	}
	fsys.names = survivors
	fsys.durableNames = maps.Clone(survivors)
}

// powerFail replaces the content of node with its durable view and a random part of its unsynced changes.
func (fsys *FS) powerFail(name string, node *inode) {
	result := slices.Clone(node.durable)
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
	}
	node.data = result
	node.durable = slices.Clone(result)
	node.pending = nil
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
// The new space holds zeros, as ext4 and XFS give after a crash.
func (fsys *FS) extend(data []byte, size int64) []byte {
	return resize(data, size)
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
	node, ok := fsys.names[name]
	if !ok {
		return nil, nil, false
	}
	return slices.Clone(node.data), slices.Clone(node.durable), true
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
// It supports os.O_RDONLY, os.O_RDWR, os.O_WRONLY, and os.O_CREATE.
func (fsys *FS) OpenFile(name string, flag int, perm os.FileMode) (fileio.File, error) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if fsys.crashed {
		return nil, crashedError("open", name)
	}
	if flag&^(os.O_RDONLY|os.O_RDWR|os.O_WRONLY|os.O_CREATE) != 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errors.ErrUnsupported}
	}
	if fsys.inject(fsys.config.OpenFault) {
		fsys.record(Event{Mutation: fsys.mutations, Op: "open", Name: name, Result: "injected"})
		return nil, injectedError("open", name)
	}
	node, ok := fsys.names[name]
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
	fsys.nextInodeID++
	node := &inode{id: fsys.nextInodeID, perm: perm.Perm()}
	fsys.names[name] = node
	return node
}

// Remove removes name.
// Open handles keep the file content.
func (fsys *FS) Remove(name string) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
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

// EvalSymlinks returns name when it exists.
// The FS has no symbolic links.
func (fsys *FS) EvalSymlinks(name string) (string, error) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if fsys.crashed {
		return "", crashedError("lstat", name)
	}
	if _, ok := fsys.names[name]; !ok {
		return "", &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrNotExist}
	}
	return name, nil
}

// SyncDir makes the current names in directory dir durable.
func (fsys *FS) SyncDir(dir string) error {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
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

var _ fileio.File = (*file)(nil)

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
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

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
	pending := change{offset: offset, data: slices.Clone(p)}
	node.data = applyChange(node.data, pending)
	node.pending = append(node.pending, pending)
}

// truncate changes the cache view and records the unsynced change.
// The caller must hold mu.
func (fsys *FS) truncate(node *inode, size int64) {
	node.data = resize(node.data, size)
	node.pending = append(node.pending, change{truncate: true, offset: size})
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
	if fsys.inject(fsys.config.SyncFault) {
		// Like Linux after a writeback error: some changes reach storage, the kernel marks the others clean, and a later sync does not write them.
		for _, pending := range node.pending {
			if fsys.rand.Bool() {
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
			node.data = slices.Clone(node.durable)
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
