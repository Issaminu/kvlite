package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// killGrace is the time between the SIGQUIT of a worker that is too slow and the kill of its process group.
	killGrace = 10 * time.Second
	// gitTimeout is the time limit of the git commands of follow mode.
	gitTimeout = 2 * time.Minute
	// alertTimeout is the time limit of the alert command.
	alertTimeout = time.Minute
	// shrinkLane is the lane name of a failure that a shrink found with a different signature.
	shrinkLane = "shrink"
)

// errStopped is the error of a shrink run that the supervisor stopped.
var errStopped = errors.New("the supervisor stops")

// supervisor runs seeds, records the failures, and shrinks new signatures.
type supervisor struct {
	opts       options
	lanes      []lane
	failures   *failureLog
	logger     *log.Logger
	machine    string
	pageSize   int
	start      time.Time
	slots      chan struct{}      // one token for each running worker
	stop       context.Context    // done when no new run may start
	stopCancel context.CancelFunc // cancels stop
	hard       context.Context    // done when the running workers must die
	shrinkWake chan struct{}      // gets a value when a shrink job is queued
	workers    sync.WaitGroup     // runs of new seeds
	background sync.WaitGroup     // the shrinker and the alert commands

	mu         sync.Mutex
	current    *build
	counts     counters
	running    int
	shrinks    []shrinkJob
	shrinking  *shrinkJob // the running shrink, or nil
	buildError string
}

// shrinkJob is a request to shrink the first example of a new signature.
type shrinkJob struct {
	build *build
	id    string
	seed  uint64
	steps int
	hint  int
}

func newSupervisor(opts options, lanes []lane, stop context.Context, stopCancel context.CancelFunc, hard context.Context) *supervisor {
	machine, err := os.Hostname()
	if err != nil {
		machine = "unknown"
	}
	return &supervisor{
		opts:       opts,
		lanes:      lanes,
		logger:     log.New(os.Stderr, "blackout ", log.LstdFlags),
		machine:    machine,
		pageSize:   os.Getpagesize(),
		start:      time.Now(),
		slots:      make(chan struct{}, opts.workers),
		stop:       stop,
		stopCancel: stopCancel,
		hard:       hard,
		shrinkWake: make(chan struct{}, 1),
		counts:     counters{Failures: make(map[string]int)},
	}
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

// prepare makes the output directories and opens the failure log.
func (s *supervisor) prepare() error {
	temporary := filepath.Join(s.opts.out, "tmp")
	// Remove the working directories that an earlier supervisor left when it died.
	if err := os.RemoveAll(temporary); err != nil {
		return err
	}
	for _, dir := range []string{temporary, filepath.Join(s.opts.out, "bin")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	var err error
	s.failures, err = openFailureLog(s.opts.out, s.opts.examples)
	return err
}

// serve runs seeds with the test binary of current until the stop context is done.
// In follow mode, it changes to the test binary of each new commit.
// At the end, it waits for the running workers and writes the final status.
func (s *supervisor) serve(current *build, follow *follower, ticks <-chan time.Time) {
	s.setBuild(current, nil)
	s.pruneBinaries()
	s.logf("compiled %s with %s; %d workers; lanes %s", current.Binary, current.GoVersion, s.opts.workers, s.opts.lanes)
	if s.opts.duration > 0 {
		time.AfterFunc(s.opts.duration, s.stopCancel)
	}
	reportDone := make(chan struct{})
	var report sync.WaitGroup
	report.Add(1)
	go func() {
		defer report.Done()
		s.reportLoop(reportDone)
	}()
	s.background.Add(1)
	go func() {
		defer s.background.Done()
		s.shrinker()
	}()

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
	return compileWorker(ctx, s.opts.goTool, s.opts.repo, filepath.Join(s.opts.out, "bin"), excludedPath(s.opts.repo, s.opts.out))
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
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			outcome, err := s.execute(current, seed, chosen.Steps)
			s.handleSeed(current, chosen.Name, outcome, err)
		}()
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
func (s *supervisor) execute(current *build, seed uint64, steps int) (runOutcome, error) {
	s.mu.Lock()
	s.running++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running--
		s.mu.Unlock()
		<-s.slots
	}()
	outcome, err := runWorker(s.hard, runSpec{
		Binary:      current.Binary,
		Seed:        seed,
		Steps:       steps,
		Timeout:     s.opts.timeout,
		KillGrace:   killGrace,
		MemoryLimit: s.opts.memoryLimit,
		TempDir:     filepath.Join(s.opts.out, "tmp"),
	})
	if err != nil {
		// The supervisor cannot start a worker, for example because the disk is full.
		// A pause with the slot held keeps the loop from spinning.
		time.Sleep(time.Second)
	}
	return outcome, err
}

// stopped reports whether the supervisor caused the result of a run.
// A stop of the service can send SIGTERM to the workers too, and that result says nothing about the code.
func (s *supervisor) stopped(outcome runOutcome) bool {
	return outcome.Kind == kindCanceled || (outcome.Signaled && s.stop.Err() != nil)
}

// handleSeed counts the result of a new seed and records a failure.
func (s *supervisor) handleSeed(current *build, laneName string, outcome runOutcome, err error) {
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
	if outcome.Kind != kindPass {
		s.recordFailure(current, laneName, outcome)
	}
}

// recordFailure writes a failure to the log and returns its signature ID.
// For a new signature, it queues a shrink and runs the alert command.
func (s *supervisor) recordFailure(current *build, laneName string, outcome runOutcome) string {
	text := signatureText(outcome.Kind, outcome.FirstLine, outcome.Output)
	entry := failureEntry{
		Time:          time.Now().UTC().Format(time.RFC3339),
		Seed:          outcome.Seed,
		Lane:          laneName,
		Steps:         outcome.Steps,
		Kind:          outcome.Kind,
		FirstLine:     outcome.FirstLine,
		Signature:     signatureID(text),
		SignatureText: text,
		Commit:        current.Commit,
		Dirty:         current.Dirty,
		GoVersion:     current.GoVersion,
		GOOS:          current.GOOS,
		GOARCH:        current.GOARCH,
		PageSize:      s.pageSize,
		Machine:       s.machine,
		Seconds:       outcome.Elapsed.Seconds(),
		Exit:          outcome.Exit,
		Command:       outcome.Command,
		Replay:        replayCommand(current.Commit, outcome.Seed, outcome.Steps),
	}
	if result := outcome.Result; result != nil {
		entry.GoVersion, entry.GOOS, entry.GOARCH, entry.PageSize = result.GoVersion, result.GOOS, result.GOARCH, result.PageSize
	}
	record, isNew, err := s.failures.record(entry, outcome.Report, outcome.RawResult)
	if err != nil {
		s.logf("write the failure log: %v", err)
	}
	marker := ""
	if isNew {
		marker = ", new"
	}
	s.logf("seed %d (lane %s, %d steps): %s [signature %s%s]", outcome.Seed, laneName, outcome.Steps, outcome.FirstLine, entry.Signature, marker)
	if isNew {
		s.queueShrink(shrinkJob{build: current, id: entry.Signature, seed: outcome.Seed, steps: outcome.Steps, hint: failedStep(outcome.FirstLine) + 1})
		s.alert(record)
	}
	return entry.Signature
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

// shrink searches for the smallest step count that fails with the signature of job.
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
		if outcome.Kind == kindPass {
			return "", nil
		}
		id := signatureID(signatureText(outcome.Kind, outcome.FirstLine, outcome.Output))
		if id != job.id {
			// A different failure is still a failure of the code, so it goes into the log.
			return s.recordFailure(job.build, shrinkLane, outcome), nil
		}
		run := shortestRun{Seed: seed, Steps: steps, Commit: job.build.Commit, Replay: replayCommand(job.build.Commit, seed, steps)}
		if err := s.failures.shorten(id, run); err != nil {
			s.logf("write signature %s: %v", id, err)
		}
		return id, nil
	}
	steps, err := shrinkSteps(s.stop, try, job.seed, job.steps, job.hint, job.id)
	if err != nil {
		s.logf("shrink of signature %s stopped at %d steps: %v", job.id, steps, err)
		return
	}
	if err := s.failures.markShrunk(job.id); err != nil {
		s.logf("write signature %s: %v", job.id, err)
	}
	s.logf("shrank signature %s from %d to %d steps: %s", job.id, job.steps, steps, replayCommand(job.build.Commit, job.seed, steps))
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

// alert runs the alert command for a new signature and does not wait for it.
func (s *supervisor) alert(record signatureRecord) {
	if s.opts.alertCommand == "" {
		return
	}
	s.background.Add(1)
	go func() {
		defer s.background.Done()
		ctx, cancel := context.WithTimeout(context.Background(), alertTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, s.opts.alertCommand, record.ID, record.FirstLine)
		cmd.Env = append(os.Environ(), "BLACKOUT_SIGNATURE_DIR="+s.failures.signatureDir(record.ID))
		if output, err := cmd.CombinedOutput(); err != nil {
			s.logf("alert command for signature %s: %v: %s", record.ID, err, strings.TrimSpace(string(output)))
		}
	}()
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
	records := s.failures.snapshot()
	s.mu.Lock()
	defer s.mu.Unlock()
	current := newStatus(s.current, s.counts, records, s.start, time.Now())
	current.Machine = s.machine
	current.Workers = s.opts.workers
	current.Running = s.running
	current.Stopping = s.stop.Err() != nil
	current.ShrinksQueued = len(s.shrinks)
	if s.shrinking != nil {
		current.Shrinking = s.shrinking.id
	}
	current.BuildError = s.buildError
	return current
}

// writeStatus writes status.json.
func (s *supervisor) writeStatus() {
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
