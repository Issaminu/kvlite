package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// The kinds of a run result.
const (
	kindPass    = "pass"
	kindFail    = "fail"
	kindPanic   = "panic"
	kindTimeout = "timeout"
	kindError   = "error"
	// kindMemory is a run that the supervisor killed because its RSS passed -max-rss.
	// It is not a failure of KVLite: the supervisor counts it apart from the failures and makes no error of it.
	kindMemory = "memory"
	// kindCanceled is a run that the supervisor stopped.
	// It says nothing about the code, so the supervisor does not count it.
	kindCanceled = "canceled"
)

// maxOutput is the maximum number of bytes of worker output that the supervisor reads.
// A panic and its stacks come first, so the start of the output is the important part.
const maxOutput = 8 << 20

// workerResult is the JSON that the test binary writes with -blackout.json.
// It mirrors blackoutResult in the root package's blackout_test.go.
type workerResult struct {
	Seed          uint64 `json:"seed"`
	Steps         int    `json:"steps"`
	Passed        bool   `json:"passed"`
	Failure       string `json:"failure"`
	Report        string `json:"report"`
	Config        string `json:"config"`
	TraceHash     string `json:"trace_hash"`
	GoVersion     string `json:"go_version"`
	GOOS          string `json:"goos"`
	GOARCH        string `json:"goarch"`
	PageSize      int    `json:"page_size"`
	Commits       int    `json:"commits"`
	Recoveries    int    `json:"recoveries"`
	Crashes       int    `json:"crashes"`
	PowerFailures int    `json:"power_failures"`
}

// runSpec describes one run of one seed.
type runSpec struct {
	Binary      string        // path of the test binary
	Seed        uint64        // seed to run
	Steps       int           // safety-phase steps
	Timeout     time.Duration // time limit of the run
	KillGrace   time.Duration // time between SIGQUIT and the kill of the process group
	MemoryLimit string        // value of GOMEMLIMIT
	MaxRSS      int64         // RSS in bytes above which the supervisor kills the process group; zero means no limit
	TempDir     string        // parent of the working directory of the run
	Env         []string      // more environment variables, for tests
}

// runOutcome is the result of one run.
type runOutcome struct {
	Kind      string
	Seed      uint64
	Steps     int
	FirstLine string        // first line of the failure, empty for a pass
	Report    string        // full report of a failed check, or the captured output
	Output    string        // captured output of the worker
	Result    *workerResult // nil when the worker wrote no JSON result
	Exit      string        // exit status of the process
	Signaled  bool          // a signal that the supervisor did not send stopped the process
	RSS       int64         // the RSS sample that passed MaxRSS, for kindMemory
}

// workerArgs returns the arguments of the test binary for one run.
func workerArgs(seed uint64, steps int, resultPath string) []string {
	return []string{
		"-test.run", "^TestBlackout$",
		"-blackout.seed=" + strconv.FormatUint(seed, 10),
		"-blackout.steps=" + strconv.Itoa(steps),
		"-blackout.json=" + resultPath,
	}
}

// workerEnv returns the environment variables that the supervisor sets for each worker.
func workerEnv(memoryLimit, dir string) []string {
	return []string{"GOMAXPROCS=1", "GOMEMLIMIT=" + memoryLimit, "GOTRACEBACK=all", "TMPDIR=" + dir}
}

// runWorker runs one seed in a new process and classifies the result.
// A cancel of ctx kills the process and gives kindCanceled.
// An error means that the supervisor could not run the worker; it is not a result of the code.
func runWorker(ctx context.Context, spec runSpec) (runOutcome, error) {
	outcome := runOutcome{Seed: spec.Seed, Steps: spec.Steps}
	dir, err := os.MkdirTemp(spec.TempDir, fmt.Sprintf("seed-%d-", spec.Seed))
	if err != nil {
		return outcome, fmt.Errorf("make the run directory: %w", err)
	}
	defer os.RemoveAll(dir)
	resultPath := filepath.Join(dir, "result.json")
	outputPath := filepath.Join(dir, "output.txt")
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return outcome, fmt.Errorf("make the output file: %w", err)
	}
	args := workerArgs(spec.Seed, spec.Steps, resultPath)
	env := append(workerEnv(spec.MemoryLimit, dir), spec.Env...)

	cmd := exec.Command(spec.Binary, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = outputFile
	cmd.Stderr = outputFile
	setProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		outputFile.Close()
		return outcome, fmt.Errorf("start the worker: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(spec.Timeout)
	defer timer.Stop()
	var samples <-chan time.Time // without a limit, it stays nil, and a nil channel is never ready
	if spec.MaxRSS > 0 {
		ticker := time.NewTicker(rssInterval)
		defer ticker.Stop()
		samples = ticker.C
	}
	var waitErr error
	stoppedAs := "" // the kind of the stop, when the supervisor stopped the process
wait:
	for {
		select {
		case waitErr = <-done:
			break wait
		case <-timer.C:
			stoppedAs = kindTimeout
			waitErr = stopProcess(cmd, done, spec.KillGrace)
			break wait
		case <-ctx.Done():
			stoppedAs = kindCanceled
			killProcess(cmd)
			waitErr = <-done
			break wait
		case <-samples:
			// ps fails when the process has exited, and then the next select gets the result.
			rss, err := residentBytes(cmd.Process.Pid)
			if err == nil && rss > spec.MaxRSS {
				// The kill does not wait for stacks, because the memory can grow fast.
				stoppedAs, outcome.RSS = kindMemory, rss
				killProcess(cmd)
				waitErr = <-done
				break wait
			}
		}
	}
	outputFile.Close()
	if stoppedAs == kindCanceled {
		outcome.Kind = kindCanceled
		return outcome, nil
	}
	outcome.Output = readCapped(outputPath)
	outcome.Result = readResult(resultPath)
	if cmd.ProcessState != nil {
		outcome.Exit = cmd.ProcessState.String()
		outcome.Signaled = stoppedAs == "" && cmd.ProcessState.ExitCode() == -1
	}
	outcome.Kind = classify(outcome.Result, waitErr, outcome.Output, stoppedAs)
	outcome.FirstLine, outcome.Report = describe(outcome, spec)
	return outcome, nil
}

// stopProcess sends SIGQUIT, so that the Go runtime prints every stack, and kills the process group after grace.
func stopProcess(cmd *exec.Cmd, done <-chan error, grace time.Duration) error {
	interruptProcess(cmd)
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		killProcess(cmd)
		return <-done
	}
}

// classify returns the kind of a run.
// stoppedAs is kindTimeout or kindMemory when the supervisor stopped the process for that cause, and empty otherwise.
func classify(result *workerResult, waitErr error, output string, stoppedAs string) string {
	switch {
	case stoppedAs != "":
		return stoppedAs
	case result != nil && !result.Passed:
		return kindFail
	case result != nil && waitErr == nil:
		return kindPass
	case crashMessage(output) != "":
		return kindPanic
	default:
		return kindError
	}
}

// describe returns the first line and the report of a run.
func describe(outcome runOutcome, spec runSpec) (firstLine, report string) {
	switch outcome.Kind {
	case kindPass:
		return "", ""
	case kindFail:
		report = outcome.Result.Report
		if report == "" {
			report = outcome.Output
		}
		return truncate(outcome.Result.Failure, maxFirstLine), report
	case kindPanic:
		firstLine = crashMessage(outcome.Output)
	case kindTimeout:
		firstLine = fmt.Sprintf("timeout: the run did not stop in %v", spec.Timeout)
	case kindMemory:
		firstLine = fmt.Sprintf("memory: the worker used %d MiB of RSS, more than the limit of %d MiB", outcome.RSS>>20, spec.MaxRSS>>20)
	default:
		firstLine = outcome.Exit
		if line := firstLineOf(outcome.Output); line != "" {
			firstLine += ": " + line
		}
	}
	return truncate(firstLine, maxFirstLine), outcome.Output
}

// readCapped reads at most maxOutput bytes of the file at path.
func readCapped(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	data, _ := io.ReadAll(io.LimitReader(file, maxOutput))
	return string(data)
}

// readResult reads the JSON result of a worker, or returns nil when there is no valid result.
func readResult(path string) *workerResult {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var result workerResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil
	}
	return &result
}
