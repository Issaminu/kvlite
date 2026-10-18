//go:build unix

package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testWriter sends the log of the supervisor to the test log.
type testWriter struct{ t *testing.T }

func (writer testWriter) Write(data []byte) (int, error) {
	writer.t.Log(strings.TrimSuffix(string(data), "\n"))
	return len(data), nil
}

// TestSupervisorRecordsShrinksAndAlerts runs the loop of the supervisor with the test binary as the worker.
// Every run of 5 or more steps fails at step 4, so the loop must log each seed, shrink the signature to 5 steps, and alert once.
func TestSupervisorRecordsShrinksAndAlerts(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	alerts := filepath.Join(t.TempDir(), "alerts.txt")
	t.Setenv(helperEnv, "fail-from-5")
	t.Setenv(alertFileEnv, alerts)
	opts := options{
		out:          out,
		workers:      2,
		timeout:      30 * time.Second,
		memoryLimit:  "256MiB",
		lanes:        "default=1:steps=20",
		examples:     3,
		alertCommand: executable,
	}
	lanes, err := parseLanes(opts.lanes)
	if err != nil {
		t.Fatal(err)
	}
	stop, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()
	s := newSupervisor(opts, lanes, stop, stopCancel, context.Background())
	s.logger = log.New(testWriter{t}, "", log.Lmicroseconds)
	if err := s.prepare(); err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		s.serve(&build{Commit: "test123", Binary: executable, GoVersion: "go-test"}, nil, nil)
	}()

	deadline := time.Now().Add(20 * time.Second)
	for {
		records := s.failures.snapshot()
		if len(records) == 1 && records[0].Shrunk {
			break
		}
		if time.Now().After(deadline) {
			stopCancel()
			<-served
			t.Fatalf("the shrink did not complete: %+v", records)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopCancel()
	<-served

	var current status
	data, err := os.ReadFile(filepath.Join(out, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &current); err != nil {
		t.Fatal(err)
	}
	if current.SeedsRun == 0 || current.Failures[kindFail] != current.SeedsRun || current.Passes != 0 || current.Running != 0 {
		t.Fatalf("status: %s", data)
	}
	if len(current.Signatures) != 1 || current.Signatures[0].ShortestSteps != 5 || current.Signatures[0].Text != "fail: Open: page N is bad" {
		t.Fatalf("status signatures: %+v", current.Signatures)
	}
	id := current.Signatures[0].ID

	// The shrink runs with the same signature do not go into the log, so the log has one line for each new seed.
	logged, err := os.ReadFile(filepath.Join(out, "failures.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(logged), "\n"); lines != current.SeedsRun {
		t.Fatalf("failures.jsonl has %d lines for %d failed seeds", lines, current.SeedsRun)
	}
	record := readSignature(t, out, id)
	if record.Shortest.Steps != 5 || !record.Shrunk || record.Examples != min(3, record.Count) || record.Count != current.SeedsRun {
		t.Fatalf("signature.json: %+v", record)
	}
	if !strings.Contains(record.Shortest.Replay, "git checkout test123 && go test -run 'TestBlackout$' -blackout.seed=") {
		t.Fatalf("replay command: %s", record.Shortest.Replay)
	}

	alerted, err := os.ReadFile(alerts)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(alerted)), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], id+" | blackout seed ") || !strings.HasSuffix(lines[0], filepath.Join(out, "signatures", id)) {
		t.Fatalf("alerts: %q", lines)
	}
	if left, _ := os.ReadDir(filepath.Join(out, "tmp")); len(left) != 0 {
		t.Fatalf("the runs left %d working directories", len(left))
	}
}
