//go:build unix

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	// helperEnv selects the behavior of the test binary when it plays a worker.
	helperEnv = "BLACKOUT_SUPERVISOR_HELPER"
	// alertFileEnv names the file to which the test binary writes its arguments when it plays the alert command.
	alertFileEnv = "BLACKOUT_SUPERVISOR_ALERT_FILE"
)

// TestMain lets the test binary play a worker or the alert command: the standard helper-process pattern.
func TestMain(m *testing.M) {
	if dir := os.Getenv("BLACKOUT_SIGNATURE_DIR"); dir != "" {
		line := strings.Join(append(os.Args[1:], dir), " | ") + "\n"
		file, err := os.OpenFile(os.Getenv(alertFileEnv), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
		if err != nil {
			os.Exit(1)
		}
		file.WriteString(line)
		file.Close()
		os.Exit(0)
	}
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
		writeResult(workerResult{Seed: seed, Steps: steps, Passed: true, Recoveries: 3, Crashes: 2})
		fmt.Println("PASS")
	case "fail":
		failure := fmt.Sprintf("blackout seed %d failed at step %d: Open: page %d is bad", seed, steps-1, steps*2)
		writeResult(workerResult{Seed: seed, Steps: steps, Failure: failure, Report: failure + "\nconfig: test"})
		fmt.Println("--- FAIL: TestBlackout")
		os.Exit(1)
	case "fail-from-5":
		// Runs of 5 or more steps fail at step 4, like a failure in the safety phase.
		if steps < 5 {
			writeResult(workerResult{Seed: seed, Steps: steps, Passed: true, Recoveries: 1, Crashes: 1})
			return
		}
		failure := fmt.Sprintf("blackout seed %d failed at step 4: Open: page %d is bad", seed, seed%1000)
		writeResult(workerResult{Seed: seed, Steps: steps, Failure: failure, Report: failure + "\nconfig: test"})
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

func runHelper(t *testing.T, ctx context.Context, mode string, timeout time.Duration) runOutcome {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	temporary := t.TempDir()
	outcome, err := runWorker(ctx, runSpec{
		Binary:      executable,
		Seed:        42,
		Steps:       10,
		Timeout:     timeout,
		KillGrace:   200 * time.Millisecond,
		MemoryLimit: "256MiB",
		TempDir:     temporary,
		Env:         []string{helperEnv + "=" + mode},
	})
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
		if outcome.Kind != kindPass || outcome.Result == nil || outcome.Result.Recoveries != 3 || outcome.FirstLine != "" {
			t.Fatalf("outcome: %+v", outcome)
		}
		if !strings.Contains(outcome.Command, "GOMAXPROCS=1 GOMEMLIMIT=256MiB ") || !strings.Contains(outcome.Command, "-blackout.seed=42") {
			t.Fatalf("command line: %s", outcome.Command)
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
		outcome := runHelper(t, context.Background(), "hang", 500*time.Millisecond)
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
	t.Run("canceled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if outcome := runHelper(t, ctx, "hang", 30*time.Second); outcome.Kind != kindCanceled {
			t.Fatalf("outcome: kind %s", outcome.Kind)
		}
	})
}
