package blackout

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/Issaminu/kvlite/internal/blackout/disk"
)

// Stats counts what one or more runs reached.
// The coverage test requires that the default seeds reach every important case.
type Stats struct {
	Steps                [operationCount]int
	Commits              int // acknowledged commits that changed data
	Aborts               int // transactions that their callback or a fault ended before the commit
	Uncertain            int // commits whose result was unknown
	UncertainKept        int // unknown results that recovery showed
	UncertainLost        int // unknown results that recovery did not show
	LostUnsyncedCommits  int // acknowledged commits that a power failure legally lost
	CrashesNow           int // crashes between operations
	CrashesInCalls       map[string]int
	CrashesInOpen        int
	FailedOpens          int
	FailedCloses         int
	StoppedHandles       int // handles that ErrWALWriteFailed stopped
	Recoveries           int // opens whose state the checks compared with the model
	Mutations            [mutationKindCount]int
	Scans                [scanKindCount]int
	Contracts            [contractCount]int
	PageChecks           int            // page walks
	StatsChecks          int            // DB.Stats checks
	ImageChecks          int            // opens of the process-crash image
	MemoryChecks         int            // comparisons of the memory database
	DamageProbes         int            // first opens after damage
	DamageDetected       int            // probes that failed, so KVLite found the damage
	DamageHarmless       int            // probes that returned an allowed state
	ReadOnlyRecoveries   int            // read-only opens after a crash that showed an allowed state
	Forms                [formCount]int // opens with explicit, nil, and zero Options
	OptionChanges        int            // opens whose options differ from the previous open
	TaintsCleared        int            // SyncNone sessions that a later syncing session made durable
	InvalidOpens         int            // checks that Open refuses invalid options
	LargeWALSessions     int            // sessions without an automatic checkpoint
	BatchSizeOneSessions int            // SyncFull or other sessions with MaxWriteBatchSize 1
	HeldChecks           int            // returned slices that were still valid at the end of their transaction
	ReaderGroups         int            // steps with several read transactions at once
	ReaderTurns          int            // turns of those readers
	Disk                 disk.Stats
}

// Add adds other to stats.
func (stats *Stats) Add(other Stats) {
	for op := range stats.Steps {
		stats.Steps[op] += other.Steps[op]
	}
	for kind := range stats.Mutations {
		stats.Mutations[kind] += other.Mutations[kind]
	}
	for kind := range stats.Scans {
		stats.Scans[kind] += other.Scans[kind]
	}
	for kind := range stats.Contracts {
		stats.Contracts[kind] += other.Contracts[kind]
	}
	stats.PageChecks += other.PageChecks
	stats.StatsChecks += other.StatsChecks
	stats.ImageChecks += other.ImageChecks
	stats.MemoryChecks += other.MemoryChecks
	stats.DamageProbes += other.DamageProbes
	stats.DamageDetected += other.DamageDetected
	stats.DamageHarmless += other.DamageHarmless
	stats.ReadOnlyRecoveries += other.ReadOnlyRecoveries
	for form := range stats.Forms {
		stats.Forms[form] += other.Forms[form]
	}
	stats.OptionChanges += other.OptionChanges
	stats.TaintsCleared += other.TaintsCleared
	stats.InvalidOpens += other.InvalidOpens
	stats.LargeWALSessions += other.LargeWALSessions
	stats.BatchSizeOneSessions += other.BatchSizeOneSessions
	stats.HeldChecks += other.HeldChecks
	stats.ReaderGroups += other.ReaderGroups
	stats.ReaderTurns += other.ReaderTurns
	stats.Commits += other.Commits
	stats.Aborts += other.Aborts
	stats.Uncertain += other.Uncertain
	stats.UncertainKept += other.UncertainKept
	stats.UncertainLost += other.UncertainLost
	stats.LostUnsyncedCommits += other.LostUnsyncedCommits
	stats.CrashesNow += other.CrashesNow
	if stats.CrashesInCalls == nil {
		stats.CrashesInCalls = make(map[string]int)
	}
	for kind, count := range other.CrashesInCalls {
		stats.CrashesInCalls[kind] += count
	}
	stats.CrashesInOpen += other.CrashesInOpen
	stats.FailedOpens += other.FailedOpens
	stats.FailedCloses += other.FailedCloses
	stats.StoppedHandles += other.StoppedHandles
	stats.Disk.InjectedFaults += other.Disk.InjectedFaults
	stats.Disk.DroppedSyncs += other.Disk.DroppedSyncs
	stats.Disk.DroppedCaches += other.Disk.DroppedCaches
	stats.Disk.NoSpaceErrors += other.Disk.NoSpaceErrors
	stats.Disk.ReadCorruptions += other.Disk.ReadCorruptions
	stats.Disk.TornSectors += other.Disk.TornSectors
	stats.Disk.GarbageFills += other.Disk.GarbageFills
	stats.Disk.MisdirectedWrites += other.Disk.MisdirectedWrites
	stats.Disk.BitRot += other.Disk.BitRot
	stats.Recoveries += other.Recoveries
	stats.Disk.ProcessCrashes += other.Disk.ProcessCrashes
	stats.Disk.PowerFailures += other.Disk.PowerFailures
	stats.Disk.ScheduledCrashes += other.Disk.ScheduledCrashes
	stats.Disk.TornWrites += other.Disk.TornWrites
	stats.Disk.LostWrites += other.Disk.LostWrites
	stats.Disk.LostTruncates += other.Disk.LostTruncates
	stats.Disk.LostNameChanges += other.Disk.LostNameChanges
}

func (stats Stats) String() string {
	var steps []string
	for op, count := range stats.Steps {
		steps = append(steps, fmt.Sprintf("%s=%d", operationNames[op], count))
	}
	var mutations, scans, contracts []string
	for kind, count := range stats.Mutations {
		mutations = append(mutations, fmt.Sprintf("%s=%d", mutationNames[kind], count))
	}
	for kind, count := range stats.Scans {
		scans = append(scans, fmt.Sprintf("%q=%d", scanNames[kind], count))
	}
	for kind, count := range stats.Contracts {
		contracts = append(contracts, fmt.Sprintf("%q=%d", contractNames[kind], count))
	}
	var crashes []string
	for _, kind := range slices.Sorted(maps.Keys(stats.CrashesInCalls)) {
		crashes = append(crashes, fmt.Sprintf("%s=%d", kind, stats.CrashesInCalls[kind]))
	}
	return fmt.Sprintf("steps: %s\nchanges: %s\nscans: %s\ncontracts: %s\nchecks: pages=%d stats=%d image=%d memory=%d damage-probes=%d (detected %d, harmless %d)\ncommits=%d aborts=%d uncertain=%d (kept %d, lost %d) lost-unsynced=%d recoveries=%d stopped=%d failed-opens=%d failed-closes=%d crashes-now=%d crashes-in-open=%d\ncrashes inside calls: %s\ndisk: %+v",
		strings.Join(steps, " "), strings.Join(mutations, " "), strings.Join(scans, " "), strings.Join(contracts, " "),
		stats.PageChecks, stats.StatsChecks, stats.ImageChecks, stats.MemoryChecks, stats.DamageProbes, stats.DamageDetected, stats.DamageHarmless, stats.Commits, stats.Aborts, stats.Uncertain, stats.UncertainKept, stats.UncertainLost, stats.LostUnsyncedCommits,
		stats.Recoveries, stats.StoppedHandles, stats.FailedOpens, stats.FailedCloses, stats.CrashesNow, stats.CrashesInOpen, strings.Join(crashes, " "), stats.Disk)
}
