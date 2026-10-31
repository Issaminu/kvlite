package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// killGrace is the time between the SIGQUIT of a worker that is too slow and the kill of its process group.
	killGrace = 10 * time.Second
	// gitTimeout is the time limit of the git commands of follow mode.
	gitTimeout = 2 * time.Minute
)

// errStopped is the error of a shrink run that the supervisor stopped.
var errStopped = errors.New("the supervisor stops")

// supervisor runs seeds, records the failures, and shrinks new errors.
type supervisor struct {
	opts       options
	lanes      []lane
	errors     *errorLog
	logger     *log.Logger
	start      time.Time
	slots      chan struct{}      // one token for each running worker
	stop       context.Context    // done when no new run may start
	stopCancel context.CancelFunc // cancels stop
	hard       context.Context    // done when the running workers must die
	shrinkWake chan struct{}      // gets a value when a shrink job is queued
	workers    sync.WaitGroup     // runs of new seeds
	background sync.WaitGroup     // the shrinker and the calls to GitHub
	// memory returns the memory of the system and the available part, as systemMemory does; tests replace it.
	memory func() (total, available int64)
	issues *issueTracker // nil without -issues

	mu         sync.Mutex
	current    *build
	counts     counters
	shrinks    []shrinkJob
	shrinking  *shrinkJob // the running shrink, or nil
	buildError string
	totals     *totals   // all testing in -out over every restart; nil before prepare
	lastTick   time.Time // the last time that the totals counted the run time of the supervisor
}

// shrinkJob is a request to shrink the first seed of a new error.
type shrinkJob struct {
	build *build
	text  string // the signature text of the error
	seed  uint64
	steps int
	hint  int
}

func newSupervisor(opts options, lanes []lane, stop context.Context, stopCancel context.CancelFunc, hard context.Context) *supervisor {
	s := &supervisor{
		opts:       opts,
		lanes:      lanes,
		errors:     newErrorLog(),
		logger:     log.New(os.Stderr, "blackout ", log.LstdFlags),
		start:      time.Now(),
		slots:      make(chan struct{}, opts.workers),
		stop:       stop,
		stopCancel: stopCancel,
		hard:       hard,
		shrinkWake: make(chan struct{}, 1),
		memory:     systemMemory,
		counts:     counters{Failures: make(map[string]int)},
	}
	if opts.issues != "" {
		s.issues = newIssueTracker(opts.issues, os.Getenv("GITHUB_TOKEN"))
	}
	return s
}

// run compiles the test binary and runs seeds until the stop context is done.
func (s *supervisor) run() error {
	if err := s.prepare(); err != nil {
		return err
	}
	var follow *follower
	var ticks <-chan time.Time
	if s.opts.follow {
		follow = &follower{git: commandGit(s.opts.repo), remote: s.opts.remote, branch: s.opts.branch}
		ticker := time.NewTicker(s.opts.followInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	current, err := s.firstBuild(follow, ticks)
	if err != nil {
		return err
	}
	s.serve(current, follow, ticks)
	return nil
}

// prepare checks that the supervisor can read the RSS of a process, makes the output directories, and keeps the status of an earlier supervisor.
func (s *supervisor) prepare() error {
	if s.opts.maxRSS > 0 {
		if _, err := residentBytes(os.Getpid()); err != nil {
			return fmt.Errorf("-max-rss needs the RSS of the workers, but the supervisor cannot read it: %w", err)
		}
	}
	temporary := filepath.Join(s.opts.out, "tmp")
	// Remove the working directories that an earlier supervisor left when it died.
	if err := os.RemoveAll(temporary); err != nil {
		return err
	}
	dirs := []string{temporary, filepath.Join(s.opts.out, "bin")}
	if s.opts.reports {
		dirs = append(dirs, filepath.Join(s.opts.out, "reports"))
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	loaded, err := loadTotals(s.opts.out, time.Now())
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.totals, s.lastTick = loaded, time.Now()
	s.mu.Unlock()
	return s.archiveStatus()
}

// archiveStatus renames status.json to status-COMMIT.json.
// So a new run of the supervisor, or a new commit, starts a new status and keeps the old one.
func (s *supervisor) archiveStatus() error {
	path := filepath.Join(s.opts.out, "status.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var old status
	if err := json.Unmarshal(data, &old); err != nil || old.Commit == "" {
		old.Commit = "unknown"
	}
	return os.Rename(path, filepath.Join(s.opts.out, "status-"+old.Commit+".json"))
}

// serve runs seeds with the test binary of current until the stop context is done.
// In follow mode, it changes to the test binary of each new commit.
// At the end, it waits for the running workers and writes the final status.
func (s *supervisor) serve(current *build, follow *follower, ticks <-chan time.Time) {
	s.setBuild(current, nil)
	s.pruneBinaries()
	limit := "no memory limit"
	if s.opts.maxRSS > 0 {
		limit = fmt.Sprintf("a memory limit of %d MiB of RSS for each worker", s.opts.maxRSS>>20)
	}
	s.logf("compiled %s; %d workers with %s; budget %d MiB; lanes %+v", current.Binary, s.opts.workers, limit, s.opts.memoryBudget>>20, s.lanes)
	if s.opts.memoryBudget > 0 && (s.opts.maxRSS == 0 || int64(s.opts.workers)*s.opts.maxRSS > s.opts.memoryBudget) {
		s.logf("warning: the memory limits of the workers together are more than the memory budget")
	}
	if s.opts.duration > 0 {
		time.AfterFunc(s.opts.duration, s.stopCancel)
	}
	reportDone := make(chan struct{})
	var report sync.WaitGroup
	report.Go(func() {
		s.reportLoop(reportDone)
	})
	s.background.Go(func() {
		s.shrinker()
	})

	for s.runSeeds(current, follow, ticks) {
		s.logf("a new commit is checked out; wait for the running workers")
		s.workers.Wait()
		next, err := s.compile(s.stop)
		if err != nil {
			if s.stop.Err() != nil {
				break
			}
			s.logf("%v\ncontinue with the old test binary", err)
			s.setBuild(current, err)
			continue
		}
		// The status belongs to one commit: keep the status of the old commit, and start a new one.
		s.writeStatus()
		if err := s.archiveStatus(); err != nil {
			s.logf("keep the status of the old commit: %v", err)
		}
		s.mu.Lock()
		s.counts = counters{Failures: make(map[string]int)}
		s.start = time.Now()
		s.shrinks = nil
		s.mu.Unlock()
		s.errors.reset()
		current = next
		s.setBuild(current, nil)
		s.pruneBinaries()
		s.logf("compiled %s", current.Binary)
	}
	s.logf("stop: wait for the running workers")
	s.workers.Wait()
	s.background.Wait()
	close(reportDone)
	report.Wait()
	s.writeStatus()
	s.logger.Print("final: ", s.status().summary())
}

// firstBuild compiles the first test binary.
// In follow mode, a failed compile waits for a new commit, so that a fix on the remote branch reaches the runner.
func (s *supervisor) firstBuild(follow *follower, ticks <-chan time.Time) (*build, error) {
	current, err := s.compile(s.stop)
	if err == nil || follow == nil {
		return current, err
	}
	if head, headErr := follow.git(s.stop, "rev-parse", "HEAD"); headErr == nil {
		follow.tried = head
	}
	for err != nil {
		s.logf("%v\nwait for a new commit on %s/%s", err, s.opts.remote, s.opts.branch)
		for {
			select {
			case <-s.stop.Done():
				return nil, err
			case <-ticks:
			}
			if s.checkFollow(follow, "") {
				break
			}
		}
		current, err = s.compile(s.stop)
	}
	return current, nil
}

// compile compiles the test binary of the current checkout.
func (s *supervisor) compile(ctx context.Context) (*build, error) {
	return compileWorker(ctx, s.opts.goTool, s.opts.repo, filepath.Join(s.opts.out, "bin"))
}

// runSeeds starts new seeds until the stop context is done or follow mode checks out a new commit.
// It returns true for a new commit.
func (s *supervisor) runSeeds(current *build, follow *follower, ticks <-chan time.Time) bool {
	for {
		select {
		case <-s.stop.Done():
			return false
		case <-ticks:
			if s.checkFollow(follow, current.FullCommit) {
				return true
			}
			continue
		case s.slots <- struct{}{}:
		}
		if s.stop.Err() != nil {
			<-s.slots
			return false
		}
		chosen := pickLane(s.lanes, rand.IntN(totalWeight(s.lanes)))
		seed := newSeed()
		s.workers.Go(func() {
			outcome, err := s.execute(current, seed, chosen.Steps)
			s.handleSeed(current, outcome, err)
		})
	}
}

// checkFollow runs one check of follow mode and reports whether it checked out a new commit.
func (s *supervisor) checkFollow(follow *follower, built string) bool {
	ctx, cancel := context.WithTimeout(s.stop, gitTimeout)
	defer cancel()
	commit, err := follow.check(ctx, built)
	if err != nil {
		s.logf("follow: %v", err)
		return false
	}
	if commit == "" {
		return false
	}
	s.logf("follow: checked out %s/%s at %s", s.opts.remote, s.opts.branch, commit)
	return true
}

// newSeed returns a random seed that is not zero.
// Seed zero makes the test binary run its range of seeds.
func newSeed() uint64 {
	var buffer [8]byte
	for {
		if _, err := cryptorand.Read(buffer[:]); err != nil {
			panic(err)
		}
		if seed := binary.LittleEndian.Uint64(buffer[:]); seed != 0 {
			return seed
		}
	}
}

// execute runs one seed in a worker slot that the caller holds, and then frees the slot.
// It starts the worker only when the system has the memory for it.
func (s *supervisor) execute(current *build, seed uint64, steps int) (runOutcome, error) {
	defer func() { <-s.slots }()
	if !s.waitForMemory() {
		return runOutcome{Kind: kindCanceled, Seed: seed, Steps: steps}, nil
	}
	started := time.Now()
	outcome, err := runWorker(s.hard, runSpec{
		Binary:      current.Binary,
		Seed:        seed,
		Steps:       steps,
		Timeout:     s.opts.timeout,
		KillGrace:   killGrace,
		MemoryLimit: s.opts.memoryLimit,
		MaxRSS:      s.opts.maxRSS,
		TempDir:     filepath.Join(s.opts.out, "tmp"),
	})
	if err != nil {
		// The supervisor cannot start a worker, for example because the disk is full.
		// A pause with the slot held keeps the loop from spinning.
		time.Sleep(time.Second)
		return outcome, err
	}
	s.countRun(outcome, time.Since(started))
	return outcome, nil
}

// countRun adds a run to the totals.
// A run that the supervisor stopped adds only its worker time.
func (s *supervisor) countRun(outcome runOutcome, duration time.Duration) {
	if s.stopped(outcome) {
		outcome.Kind = kindCanceled
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.totals != nil {
		s.totals.add(outcome, duration)
	}
}

// waitForMemory waits until the system has the memory for one more worker, as memoryAllowsWorker says.
// It reports false when the stop context is done first.
func (s *supervisor) waitForMemory() bool {
	for waited := false; ; waited = true {
		total, available := s.memory()
		if memoryAllowsWorker(total, available, s.opts.maxRSS) {
			if waited {
				s.logf("memory: %d MiB is available again, so a new worker starts", available>>20)
			}
			return true
		}
		if !waited {
			s.logf("memory: only %d MiB of %d MiB is available, so a new worker waits until %d MiB is available", available>>20, total>>20, (s.opts.maxRSS+total/10)>>20)
		}
		select {
		case <-time.After(memoryPollInterval):
		case <-s.stop.Done():
			return false
		}
	}
}

// stopped reports whether the supervisor caused the result of a run.
// A stop of the service can send SIGTERM to the workers too, and that result says nothing about the code.
func (s *supervisor) stopped(outcome runOutcome) bool {
	return outcome.Kind == kindCanceled || (outcome.Signaled && s.stop.Err() != nil)
}

// handleSeed counts the result of a new seed and records a failure.
func (s *supervisor) handleSeed(current *build, outcome runOutcome, err error) {
	if err != nil {
		s.logf("seed %d: %v", outcome.Seed, err)
		return
	}
	if s.stopped(outcome) {
		return
	}
	s.mu.Lock()
	s.counts.add(outcome)
	s.mu.Unlock()
	switch outcome.Kind {
	case kindPass:
	case kindMemory:
		s.logf("seed %d (%d steps): %s; this is not a KVLite failure", outcome.Seed, outcome.Steps, outcome.FirstLine)
	default:
		s.recordFailure(current, outcome)
	}
}

// recordFailure adds a failed run to the status and returns the text of its error.
// With -reports, it writes the full report of the run to reports/SEED.txt.
// For a new error, it queues a shrink, and with -issues it reports the error to GitHub.
func (s *supervisor) recordFailure(current *build, outcome runOutcome) string {
	text := signatureText(outcome.Kind, outcome.FirstLine, outcome.Output)
	isNew := s.errors.add(text, seedFrom(outcome))
	if s.opts.reports {
		path := filepath.Join(s.opts.out, "reports", fmt.Sprintf("%d.txt", outcome.Seed))
		if err := os.WriteFile(path, []byte(outcome.Report), 0o644); err != nil {
			s.logf("write the report of seed %d: %v", outcome.Seed, err)
		}
	}
	marker := ""
	if isNew {
		marker = " [new error]"
	}
	s.logf("seed %d (%d steps): %s%s", outcome.Seed, outcome.Steps, outcome.FirstLine, marker)
	if isNew {
		s.queueShrink(shrinkJob{build: current, text: text, seed: outcome.Seed, steps: outcome.Steps, hint: failedStep(outcome.FirstLine) + 1})
		s.fileIssue(current, outcome, text)
	}
	return text
}

// fileIssue reports a new error to GitHub in the background, so a slow or failed call never stops the runs.
func (s *supervisor) fileIssue(current *build, outcome runOutcome, text string) {
	if s.issues == nil {
		return
	}
	s.background.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), issueTimeout)
		defer cancel()
		if err := s.issues.report(ctx, current, s.platform(current), text, outcome); err != nil {
			s.logf("GitHub issue of seed %d: %v", outcome.Seed, err)
		}
	})
}

// queueShrink adds a shrink job to the queue.
func (s *supervisor) queueShrink(job shrinkJob) {
	s.mu.Lock()
	s.shrinks = append(s.shrinks, job)
	s.mu.Unlock()
	select {
	case s.shrinkWake <- struct{}{}:
	default:
	}
}

// shrinker runs the shrink jobs one at a time until the stop context is done.
func (s *supervisor) shrinker() {
	for {
		job, ok := s.nextShrink()
		if !ok {
			return
		}
		s.shrink(job)
		s.mu.Lock()
		s.shrinking = nil
		s.mu.Unlock()
	}
}

// nextShrink waits for a shrink job.
// It returns false when the stop context is done.
func (s *supervisor) nextShrink() (shrinkJob, bool) {
	for {
		if s.stop.Err() != nil {
			return shrinkJob{}, false
		}
		s.mu.Lock()
		if len(s.shrinks) > 0 {
			job := s.shrinks[0]
			s.shrinks = s.shrinks[1:]
			s.shrinking = &job
			s.mu.Unlock()
			return job, true
		}
		s.mu.Unlock()
		select {
		case <-s.shrinkWake:
		case <-s.stop.Done():
		}
	}
}

// shrink searches for the smallest step count that fails with the error of job.
// Each try waits for a free worker slot, like a new seed.
func (s *supervisor) shrink(job shrinkJob) {
	try := func(ctx context.Context, seed uint64, steps int) (string, error) {
		select {
		case s.slots <- struct{}{}:
		case <-ctx.Done():
			return "", errStopped
		}
		if ctx.Err() != nil {
			<-s.slots
			return "", errStopped
		}
		outcome, err := s.execute(job.build, seed, steps)
		if err != nil {
			return "", err
		}
		if s.stopped(outcome) {
			return "", errStopped
		}
		switch outcome.Kind {
		case kindPass:
			return "", nil
		case kindMemory:
			// The result of the run is unknown, so the search stops.
			return "", errors.New(outcome.FirstLine)
		}
		if s.currentBuild() != job.build {
			// The runner changed to a new commit, and the status of the old commit is closed.
			return "", errStopped
		}
		text := signatureText(outcome.Kind, outcome.FirstLine, outcome.Output)
		if text != job.text {
			// A different failure is still a failure of the code, so it goes into the status.
			return s.recordFailure(job.build, outcome), nil
		}
		s.errors.shorten(text, seedFrom(outcome))
		return text, nil
	}
	steps, err := shrinkSteps(s.stop, try, job.seed, job.steps, job.hint, job.text)
	if err != nil {
		s.logf("shrink of seed %d stopped at %d steps: %v", job.seed, steps, err)
		return
	}
	s.logf("shrank seed %d from %d to %d steps: %s", job.seed, job.steps, steps, job.text)
	if s.issues != nil && steps < job.steps {
		ctx, cancel := context.WithTimeout(context.Background(), issueTimeout)
		defer cancel()
		if err := s.issues.shrunk(ctx, job.build, job.text, job.seed, steps); err != nil {
			s.logf("GitHub issue of seed %d: %v", job.seed, err)
		}
	}
}

// pruneBinaries removes the test binaries that neither the current build nor a shrink uses.
// A worker that still runs a removed binary continues, because the system keeps the file until the process exits.
func (s *supervisor) pruneBinaries() {
	s.mu.Lock()
	keep := map[string]bool{s.current.Binary: true}
	for _, job := range s.shrinks {
		keep[job.build.Binary] = true
	}
	if s.shrinking != nil {
		keep[s.shrinking.build.Binary] = true
	}
	s.mu.Unlock()
	binaries, err := filepath.Glob(filepath.Join(s.opts.out, "bin", "blackout-*.test"))
	if err != nil {
		return
	}
	for _, binary := range binaries {
		if !keep[binary] {
			if err := os.Remove(binary); err != nil {
				s.logf("remove an old test binary: %v", err)
			}
		}
	}
}

// setBuild changes the current test binary and the last compile error.
func (s *supervisor) setBuild(current *build, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current = current
	s.buildError = ""
	if err != nil {
		s.buildError = err.Error()
	}
}

// status returns the current status.
func (s *supervisor) status() status {
	errors := s.errors.snapshot()
	s.mu.Lock()
	defer s.mu.Unlock()
	current := newStatus(s.current, s.platform(s.current), s.counts, errors, s.start, time.Now())
	current.BuildError = s.buildError
	if s.totals != nil {
		copied := *s.totals
		current.Totals = &copied
	}
	return current
}

// platform returns the platform of a build: the operating system and the architecture.
func (s *supervisor) platform(current *build) string {
	return current.GOOS + "/" + current.GOARCH
}

// currentBuild returns the test binary of the current commit.
func (s *supervisor) currentBuild() *build {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

// writeStatus adds the run time of the supervisor to the totals, and writes totals.json and status.json.
func (s *supervisor) writeStatus() {
	s.mu.Lock()
	var snapshot *totals
	if s.totals != nil {
		now := time.Now()
		s.totals.addWall(now.Sub(s.lastTick))
		s.lastTick = now
		copied := *s.totals
		snapshot = &copied
	}
	s.mu.Unlock()
	if snapshot != nil {
		if err := snapshot.save(s.opts.out); err != nil {
			s.logf("write the totals: %v", err)
		}
	}
	if err := writeJSONFile(filepath.Join(s.opts.out, "status.json"), s.status()); err != nil {
		s.logf("write the status: %v", err)
	}
}

// reportLoop writes status.json and prints a summary line until done is closed.
func (s *supervisor) reportLoop(done <-chan struct{}) {
	statusTicker := time.NewTicker(statusInterval)
	defer statusTicker.Stop()
	summaryTicker := time.NewTicker(summaryInterval)
	defer summaryTicker.Stop()
	s.writeStatus()
	for {
		select {
		case <-done:
			return
		case <-statusTicker.C:
			s.writeStatus()
		case <-summaryTicker.C:
			s.logger.Print(s.status().summary())
		}
	}
}

// logf prints a line on stderr with the label of the current commit.
func (s *supervisor) logf(format string, args ...any) {
	s.mu.Lock()
	label := "(no binary)"
	if s.current != nil {
		label = s.current.label()
	}
	s.mu.Unlock()
	s.logger.Printf("%s: %s", label, fmt.Sprintf(format, args...))
}
