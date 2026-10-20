//go:build unix

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// helperEnv selects the behavior of the test binary when it plays a worker.
const helperEnv = "BLACKOUT_SUPERVISOR_HELPER"

// TestMain lets the test binary play a worker: the standard helper-process pattern.
func TestMain(m *testing.M) {
	if mode := os.Getenv(helperEnv); mode != "" {
		helperMain(mode)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// helperMain plays a worker with the behavior that mode names.
func helperMain(mode string) {
	var seed uint64
	var steps int
	var resultPath string
	for _, arg := range os.Args[1:] {
		if value, found := strings.CutPrefix(arg, "-blackout.seed="); found {
			seed, _ = strconv.ParseUint(value, 10, 64)
		} else if value, found := strings.CutPrefix(arg, "-blackout.steps="); found {
			steps, _ = strconv.Atoi(value)
		} else if value, found := strings.CutPrefix(arg, "-blackout.json="); found {
			resultPath = value
		}
	}
	writeResult := func(result workerResult) {
		data, _ := json.Marshal(result)
		os.WriteFile(resultPath, data, 0o644)
	}
	switch mode {
	case "pass":
		writeResult(workerResult{Passed: true})
		fmt.Println("PASS")
	case "fail":
		failure := fmt.Sprintf("blackout seed %d failed at step %d: Open: page %d is bad", seed, steps-1, steps*2)
		writeResult(workerResult{Failure: failure, Report: failure + "\nconfig: test"})
		fmt.Println("--- FAIL: TestBlackout")
		os.Exit(1)
	case "fail-from-5":
		// Runs of 5 or more steps fail at step 4, like a failure in the safety phase.
		if steps < 5 {
			writeResult(workerResult{Passed: true})
			return
		}
		failure := fmt.Sprintf("blackout seed %d failed at step 4: Open: page %d is bad", seed, seed%1000)
		writeResult(workerResult{Failure: failure, Report: failure + "\nconfig: test"})
		os.Exit(1)
	case "panic":
		go helperPanic()
		time.Sleep(time.Minute)
	case "hang":
		helperHang()
	case "ignore-quit":
		signal.Ignore(syscall.SIGQUIT)
		helperHang()
	case "error":
		fmt.Println("something odd")
		os.Exit(3)
	case "grow":
		// The helper grows to 384 MiB in about 2 seconds, so a sample sees it above the limit of runHelper.
		// It passes if nothing kills it.
		var chunks [][]byte
		for range 192 {
			chunk := make([]byte, 2<<20)
			for index := 0; index < len(chunk); index += 4096 {
				chunk[index] = 1
			}
			chunks = append(chunks, chunk)
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(3 * time.Second)
		runtime.KeepAlive(chunks)
		writeResult(workerResult{Passed: true})
	case "self-kill":
		// The helper plays a worker that the out-of-memory killer of the system stops.
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
		time.Sleep(time.Minute)
	case "priority":
		// The helper reports its CPU priority as a failure, so the test can read it from the first line.
		priority, err := syscall.Getpriority(syscall.PRIO_PROCESS, 0)
		if runtime.GOOS == "linux" {
			// The system call of Linux returns 20 minus the nice value.
			priority = 20 - priority
		}
		failure := fmt.Sprintf("blackout seed %d failed at step 0: nice %d, %v", seed, priority, err)
		writeResult(workerResult{Failure: failure})
		os.Exit(1)
	}
}

//go:noinline
func helperPanic() {
	panic("helper panic")
}

//go:noinline
func helperHang() {
	time.Sleep(time.Minute)
}

// runHelper runs the test binary as a worker in mode, and change can modify the run first.
func runHelper(t *testing.T, ctx context.Context, mode string, timeout time.Duration, change ...func(*runSpec)) runOutcome {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	temporary := t.TempDir()
	spec := runSpec{
		Binary:      executable,
		Seed:        42,
		Steps:       10,
		Timeout:     timeout,
		KillGrace:   2 * time.Second, // under full CPU load, the runtime needs time to print every stack after SIGQUIT
		MemoryLimit: "256MiB",
		MaxRSS:      128 << 20,
		TempDir:     temporary,
		Env:         []string{helperEnv + "=" + mode},
	}
	for _, change := range change {
		change(&spec)
	}
	outcome, err := runWorker(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if left, _ := os.ReadDir(temporary); len(left) != 0 {
		t.Errorf("the run left %d files in its temporary directory", len(left))
	}
	return outcome
}

func TestRunWorkerClassifiesHelperProcesses(t *testing.T) {
	t.Run("pass", func(t *testing.T) {
		t.Parallel()
		outcome := runHelper(t, context.Background(), "pass", 30*time.Second)
		if outcome.Kind != kindPass || outcome.Result == nil || outcome.FirstLine != "" {
			t.Fatalf("outcome: %+v", outcome)
		}
	})
	t.Run("fail", func(t *testing.T) {
		t.Parallel()
		outcome := runHelper(t, context.Background(), "fail", 30*time.Second)
		if outcome.Kind != kindFail || outcome.FirstLine != "blackout seed 42 failed at step 9: Open: page 20 is bad" || !strings.Contains(outcome.Report, "config: test") {
			t.Fatalf("outcome: %+v", outcome)
		}
		if got := signatureText(outcome.Kind, outcome.FirstLine, outcome.Output); got != "fail: Open: page N is bad" {
			t.Fatalf("signature: %q", got)
		}
	})
	t.Run("panic", func(t *testing.T) {
		t.Parallel()
		outcome := runHelper(t, context.Background(), "panic", 30*time.Second)
		if outcome.Kind != kindPanic || outcome.FirstLine != "panic: helper panic" || outcome.Result != nil {
			t.Fatalf("outcome: kind %s, first line %q", outcome.Kind, outcome.FirstLine)
		}
		want := "panic: " + modulePath + "/internal/blackout/supervisor.helperPanic"
		if got := signatureText(outcome.Kind, outcome.FirstLine, outcome.Output); got != want {
			t.Fatalf("signature: got %q, want %q", got, want)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		t.Parallel()
		// A short timeout can send SIGQUIT before the runtime of the helper starts, and then nothing prints the stacks.
		outcome := runHelper(t, context.Background(), "hang", 3*time.Second)
		if outcome.Kind != kindTimeout || !strings.Contains(outcome.Output, "SIGQUIT") {
			t.Fatalf("outcome: kind %s, output %q", outcome.Kind, truncate(outcome.Output, 300))
		}
		// SIGQUIT makes the Go runtime print the stacks, so the signature names the function that hangs.
		got := signatureText(outcome.Kind, outcome.FirstLine, outcome.Output)
		if !strings.HasPrefix(got, "timeout: "+modulePath+"/internal/blackout/supervisor.helperHang") {
			t.Fatalf("signature: %q", got)
		}
	})
	t.Run("timeout without a reaction to SIGQUIT", func(t *testing.T) {
		t.Parallel()
		start := time.Now()
		outcome := runHelper(t, context.Background(), "ignore-quit", 500*time.Millisecond)
		if outcome.Kind != kindTimeout || outcome.Signaled {
			t.Fatalf("outcome: kind %s, signaled %v", outcome.Kind, outcome.Signaled)
		}
		if elapsed := time.Since(start); elapsed > 10*time.Second {
			t.Fatalf("the kill took %v", elapsed)
		}
	})
	t.Run("error", func(t *testing.T) {
		t.Parallel()
		outcome := runHelper(t, context.Background(), "error", 30*time.Second)
		if outcome.Kind != kindError || outcome.FirstLine != "exit status 3: something odd" {
			t.Fatalf("outcome: kind %s, first line %q", outcome.Kind, outcome.FirstLine)
		}
	})
	t.Run("memory limit", func(t *testing.T) {
		t.Parallel()
		outcome := runHelper(t, context.Background(), "grow", 30*time.Second)
		if outcome.Kind != kindMemory || outcome.Signaled || !strings.HasPrefix(outcome.FirstLine, "memory: the worker used ") {
			t.Fatalf("outcome: kind %s, signaled %v, first line %q", outcome.Kind, outcome.Signaled, outcome.FirstLine)
		}
	})
	t.Run("RSS that the supervisor cannot read", func(t *testing.T) {
		t.Parallel()
		// Without samples, the limit does not work, so the supervisor must stop the worker and not let it run.
		failing := func(spec *runSpec) {
			spec.ReadRSS = func(int) (int64, error) { return 0, errors.New("no sample") }
		}
		outcome := runHelper(t, context.Background(), "hang", 30*time.Second, failing)
		if outcome.Kind != kindMemory || !strings.Contains(outcome.FirstLine, "could not read the RSS") {
			t.Fatalf("outcome: kind %s, first line %q", outcome.Kind, outcome.FirstLine)
		}
	})
	t.Run("SIGKILL from the system", func(t *testing.T) {
		t.Parallel()
		outcome := runHelper(t, context.Background(), "self-kill", 30*time.Second)
		if outcome.Kind != kindMemory || outcome.Signaled || !strings.Contains(outcome.FirstLine, "SIGKILL") {
			t.Fatalf("outcome: kind %s, signaled %v, first line %q", outcome.Kind, outcome.Signaled, outcome.FirstLine)
		}
	})
	t.Run("lowest CPU priority", func(t *testing.T) {
		t.Parallel()
		outcome := runHelper(t, context.Background(), "priority", 30*time.Second)
		if !strings.HasSuffix(outcome.FirstLine, "nice 19, <nil>") {
			t.Fatalf("outcome: kind %s, first line %q", outcome.Kind, outcome.FirstLine)
		}
	})
	t.Run("canceled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if outcome := runHelper(t, ctx, "hang", 30*time.Second); outcome.Kind != kindCanceled {
			t.Fatalf("outcome: kind %s", outcome.Kind)
		}
	})
}
