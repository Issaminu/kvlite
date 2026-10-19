package blackout

// Damaged storage holds bytes that no correct storage gives: bit rot at rest, or a write that a power failure moved to a wrong offset.
// KVLite cannot always repair damage, but it must never return wrong data.
// So the first open after damage is a probe: it must fail, or return an allowed state.
// Then the disk heals the damage, and the normal open checks the state with the usual rules.

// corruptAtRest flips random bits of the stored main file between a stop and the next open, sometimes.
// Without data-page checksums, KVLite cannot find damaged key and value bytes, so the bits then hit only the metadata and first allocation pages, which always have checksums.
// The WAL gets no bit rot: like SQLite, KVLite ends recovery at the first bad record, so damage to a synced record loses later commits by design.
func (runner *runner) corruptAtRest() {
	if !runner.faults || !runner.crash.rand.Chance(runner.config.CorruptAtRest) {
		return
	}
	end := int64(3 * runner.config.PageSize)
	if runner.config.fixedChecksums() {
		end = 1 << 62
	}
	bits := runner.crash.rand.Range(1, 8)
	if runner.fsys.Corrupt(runner.realPath, 0, end, bits) {
		runner.event("bit rot at rest: %d bits", bits)
	}
}

// probeDamage opens the damaged storage once and requires an error or an allowed state, never wrong data.
// It then heals the damage.
func (runner *runner) probeDamage(afterCrash bool) {
	runner.stats.DamageProbes++
	runner.fsys.SetFaults(false)
	defer runner.fsys.SetFaults(runner.faults)
	runner.fsys.CancelCrash()
	db, err := runner.openDB(runner.fsys, false)
	if err == nil {
		var state dbState
		state, err = runner.readState(db)
		if err == nil {
			err = runner.target.CheckPages(db)
		}
		if err == nil {
			lowest, allowBranches := runner.allowedStates(afterCrash)
			if _, _, found := runner.model.find(state, lowest, allowBranches); !found {
				runner.fail("damaged storage returned wrong data: the open succeeded, but its state matches no allowed state\ndifference from the latest state:\n%s", stateDiff(runner.model.state, state))
			} else {
				runner.stats.DamageHarmless++
				runner.event("damage probe: the open returned an allowed state")
			}
		}
		// The probe handle ends like a crashed process, so it writes nothing more.
		runner.target.Abandon(db)
	}
	if err != nil {
		runner.stats.DamageDetected++
		runner.event("damage probe: KVLite found the damage: %v", err)
	}
	runner.fsys.Heal()
}

// allowedStates returns the lowest state and whether a branch is allowed for the next verified open.
// It does not change the run.
func (runner *runner) allowedStates(afterCrash bool) (int, bool) {
	droppedCache := runner.fsys.Stats().DroppedCaches > runner.droppedCaches
	lowest := runner.model.latest
	if afterCrash && runner.powerLost || droppedCache {
		lowest = runner.model.floor
	}
	// After ErrWALWriteFailed, Close keeps the WAL, so the commit whose result is unknown can come back without a crash.
	return lowest, afterCrash || droppedCache || runner.uncertainOpen
}
