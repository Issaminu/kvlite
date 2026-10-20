package blackout

import "github.com/Issaminu/kvlite/internal/blackout/disk"

// Stats counts what one or more runs reached.
// The coverage test requires that the default seeds reach every important case.
type Stats struct {
	Steps                [operationCount]int
	Commits              int // acknowledged commits that changed data
	Aborts               int // transactions that their callback or a fault ended before the commit
	UncertainKept        int // unknown results that recovery showed
	UncertainLost        int // unknown results that recovery did not show
	LostUnsyncedCommits  int // acknowledged commits that a power failure legally lost
	CrashesNow           int // crashes between operations
	CrashesInCalls       map[string]int
	CrashesInOpen        int
	FailedCloses         int
	Recoveries           int // opens whose state the checks compared with the model
	Mutations            [mutationKindCount]int
	Scans                [scanKindCount]int
	Contracts            [contractCount]int
	ImageChecks          int            // opens of the process-crash image
	MemoryChecks         int            // comparisons of the memory database
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
	ReaderTurns          int            // reader turns in steps with several read transactions at once
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
	stats.ImageChecks += other.ImageChecks
	stats.MemoryChecks += other.MemoryChecks
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
	stats.ReaderTurns += other.ReaderTurns
	stats.Commits += other.Commits
	stats.Aborts += other.Aborts
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
	stats.FailedCloses += other.FailedCloses
	stats.Disk.InjectedFaults += other.Disk.InjectedFaults
	stats.Disk.DroppedSyncs += other.Disk.DroppedSyncs
	stats.Disk.DroppedCaches += other.Disk.DroppedCaches
	stats.Disk.RevertedSyncs += other.Disk.RevertedSyncs
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
