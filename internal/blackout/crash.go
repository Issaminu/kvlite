package blackout

import "github.com/Issaminu/kvlite/internal/blackout/disk"

// crasher is the crash generator. It plays the machine.
// It decides when the process crashes, inside which call, and whether a crash is a power failure.
type crasher struct {
	rand    *disk.Rand
	config  *Config
	enabled bool // false in the liveness phase
}

func newCrasher(seed uint64, config *Config) *crasher {
	return &crasher{rand: disk.NewRand(streamSeed(seed, streamCrash)), config: config, enabled: true}
}

// crashEvent is what the crash generator does at the start of a step.
type crashEvent uint8

const (
	crashNone     crashEvent = iota
	crashNow                 // crash before the step's operation
	crashSchedule            // crash inside one of the next file calls
)

// beforeStep decides whether the process crashes now or inside a later call.
// For a scheduled crash it also returns the number of mutating file calls before it.
func (crash *crasher) beforeStep() (crashEvent, int) {
	if !crash.enabled {
		return crashNone, 0
	}
	if crash.rand.Chance(crash.config.CrashNow) {
		return crashNow, 0
	}
	if crash.rand.Chance(crash.config.ScheduleCrash) {
		return crashSchedule, crash.rand.Range(1, 40)
	}
	return crashNone, 0
}

// powerFailure decides whether a crash also cuts the power.
func (crash *crasher) powerFailure(chance disk.Ratio) bool {
	return crash.config.PowerFailures && crash.rand.Chance(chance)
}

// duringRecovery decides whether the process crashes again while Open recovers.
// For such a crash it returns the number of mutating file calls before it.
func (crash *crasher) duringRecovery() (bool, int) {
	if !crash.enabled || crash.config.ScheduleCrash == disk.Never || !crash.rand.Chance(disk.OneIn(5)) {
		return false, 0
	}
	return true, crash.rand.Range(1, 20)
}
