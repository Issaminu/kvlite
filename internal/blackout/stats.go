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
	Steps               [operationCount]int
	Commits             int // acknowledged commits that changed data
	Aborts              int // transactions that their callback or a fault ended before the commit
	Uncertain           int // commits whose result was unknown
	UncertainKept       int // unknown results that recovery showed
	UncertainLost       int // unknown results that recovery did not show
	LostUnsyncedCommits int // acknowledged commits that a power failure legally lost
	CrashesNow          int // crashes between operations
	CrashesInCalls      map[string]int
	CrashesInOpen       int
	FailedOpens         int
	FailedCloses        int
	StoppedHandles      int // handles that ErrWALWriteFailed stopped
	Recoveries          int // opens whose state the checks compared with the model
	Disk                disk.Stats
}

// Add adds other to stats.
func (stats *Stats) Add(other Stats) {
	for op := range stats.Steps {
		stats.Steps[op] += other.Steps[op]
	}
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
	var crashes []string
	for _, kind := range slices.Sorted(maps.Keys(stats.CrashesInCalls)) {
		crashes = append(crashes, fmt.Sprintf("%s=%d", kind, stats.CrashesInCalls[kind]))
	}
	return fmt.Sprintf("steps: %s\ncommits=%d aborts=%d uncertain=%d (kept %d, lost %d) lost-unsynced=%d recoveries=%d stopped=%d failed-opens=%d failed-closes=%d crashes-now=%d crashes-in-open=%d\ncrashes inside calls: %s\ndisk: %+v",
		strings.Join(steps, " "), stats.Commits, stats.Aborts, stats.Uncertain, stats.UncertainKept, stats.UncertainLost, stats.LostUnsyncedCommits,
		stats.Recoveries, stats.StoppedHandles, stats.FailedOpens, stats.FailedCloses, stats.CrashesNow, stats.CrashesInOpen, strings.Join(crashes, " "), stats.Disk)
}
