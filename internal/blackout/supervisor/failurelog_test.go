package main

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testEntry(seed uint64, steps int, id string) failureEntry {
	return failureEntry{
		Time:          "2026-10-18T12:00:00Z",
		Seed:          seed,
		Lane:          "default",
		Steps:         steps,
		Kind:          kindFail,
		FirstLine:     "blackout seed 1 failed at step 2: Open: bad",
		Signature:     id,
		SignatureText: "fail: Open: bad",
		Commit:        "abc1234",
		Dirty:         true,
		Replay:        replayCommand("abc1234", seed, steps),
	}
}

func readSignature(t *testing.T, dir, id string) signatureRecord {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "signatures", id, "signature.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record signatureRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

func TestFailureLogWritesEntriesAndExamples(t *testing.T) {
	dir := t.TempDir()
	failures, err := openFailureLog(dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"seed": 18446744073709551615, "passed": false}`)
	record, isNew, err := failures.record(testEntry(math.MaxUint64, 300, "aaaaaaaaaaaa"), "full report", raw)
	if err != nil || !isNew || record.Count != 1 {
		t.Fatalf("first record: got %+v, new %v, error %v", record, isNew, err)
	}
	if _, isNew, _ := failures.record(testEntry(5, 300, "aaaaaaaaaaaa"), "report 2", nil); isNew {
		t.Fatal("the second failure of a signature is new")
	}

	file, err := os.Open(filepath.Join(dir, "failures.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var lines []string
	for scanner := bufio.NewScanner(file); scanner.Scan(); {
		lines = append(lines, scanner.Text())
	}
	if len(lines) != 2 {
		t.Fatalf("failures.jsonl has %d lines, want 2", len(lines))
	}
	// A large seed must survive JSON readers that use float64.
	if !strings.Contains(lines[0], `"seed":"18446744073709551615"`) || !strings.Contains(lines[0], `"dirty":true`) {
		t.Fatalf("first line: %s", lines[0])
	}
	var entry failureEntry
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil || entry.Seed != math.MaxUint64 {
		t.Fatalf("decode the first line: seed %d, error %v", entry.Seed, err)
	}
	if !strings.Contains(entry.Replay, "git checkout abc1234 && go test -run 'TestBlackout$' -blackout.seed=18446744073709551615 -blackout.steps=300 -blackout.trace=200 -v .") {
		t.Fatalf("replay command: %s", entry.Replay)
	}

	example := filepath.Join(dir, "signatures", "aaaaaaaaaaaa", "seed-18446744073709551615")
	report, err := os.ReadFile(filepath.Join(example, "report.txt"))
	if err != nil || string(report) != "full report" {
		t.Fatalf("report.txt: %q, %v", report, err)
	}
	var result exampleResult
	data, err := os.ReadFile(filepath.Join(example, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Seed != math.MaxUint64 || !strings.Contains(string(result.WorkerResult), "18446744073709551615") {
		t.Fatalf("result.json: %s", data)
	}
	if record := readSignature(t, dir, "aaaaaaaaaaaa"); record.Count != 2 || record.Examples != 2 || record.Text != "fail: Open: bad" {
		t.Fatalf("signature.json: %+v", record)
	}
}

func TestFailureLogCapsExamples(t *testing.T) {
	dir := t.TempDir()
	failures, err := openFailureLog(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	for seed := uint64(1); seed <= 5; seed++ {
		if _, _, err := failures.record(testEntry(seed, 300, "bbbbbbbbbbbb"), "report", nil); err != nil {
			t.Fatal(err)
		}
	}
	examples, err := filepath.Glob(filepath.Join(dir, "signatures", "bbbbbbbbbbbb", "seed-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(examples) != 2 {
		t.Fatalf("example folders: got %d, want 2: %v", len(examples), examples)
	}
	if record := readSignature(t, dir, "bbbbbbbbbbbb"); record.Count != 5 || record.Examples != 2 {
		t.Fatalf("signature.json: count %d, examples %d; want 5 and 2", record.Count, record.Examples)
	}
}

func TestFailureLogShortestRun(t *testing.T) {
	dir := t.TempDir()
	failures, err := openFailureLog(dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	id := "cccccccccccc"
	failures.record(testEntry(1, 3000, id), "report", nil)
	failures.record(testEntry(2, 300, id), "report", nil)
	if got := readSignature(t, dir, id).Shortest; got.Seed != 2 || got.Steps != 300 {
		t.Fatalf("shortest after a shorter failure: %+v", got)
	}
	if err := failures.shorten(id, shortestRun{Seed: 1, Steps: 88, Commit: "abc1234", Replay: replayCommand("abc1234", 1, 88)}); err != nil {
		t.Fatal(err)
	}
	failures.shorten(id, shortestRun{Seed: 3, Steps: 120})
	if err := failures.markShrunk(id); err != nil {
		t.Fatal(err)
	}
	record := readSignature(t, dir, id)
	if record.Shortest.Seed != 1 || record.Shortest.Steps != 88 || !record.Shrunk || !strings.Contains(record.Shortest.Replay, "-blackout.steps=88") {
		t.Fatalf("signature.json after the shrink: %+v", record)
	}

	// A new supervisor process loads the signature, so the signature is not new again.
	again, err := openFailureLog(dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	loaded, isNew, err := again.record(testEntry(4, 300, id), "report", nil)
	if err != nil || isNew || loaded.Count != 3 || loaded.Shortest.Steps != 88 {
		t.Fatalf("record after a restart: %+v, new %v, error %v", loaded, isNew, err)
	}
}
