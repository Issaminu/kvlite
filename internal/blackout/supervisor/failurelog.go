package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// failureEntry is one line of failures.jsonl.
// Seeds are strings, because many JSON readers lose the precision of integers larger than 2^53.
type failureEntry struct {
	Time          string  `json:"time"`
	Seed          uint64  `json:"seed,string"`
	Lane          string  `json:"lane"`
	Steps         int     `json:"steps"`
	Kind          string  `json:"kind"`
	FirstLine     string  `json:"first_line"`
	Signature     string  `json:"signature"`
	SignatureText string  `json:"signature_text"`
	Commit        string  `json:"commit"`
	Dirty         bool    `json:"dirty"`
	GoVersion     string  `json:"go_version"`
	GOOS          string  `json:"goos"`
	GOARCH        string  `json:"goarch"`
	PageSize      int     `json:"page_size"`
	Machine       string  `json:"machine"`
	Seconds       float64 `json:"seconds"`
	Exit          string  `json:"exit,omitempty"`
	Command       string  `json:"command"`
	Replay        string  `json:"replay"`
}

// exampleResult is the result.json file of an example.
type exampleResult struct {
	failureEntry
	// WorkerResult is the JSON result of the worker, when the worker wrote one.
	WorkerResult json.RawMessage `json:"worker_result,omitempty"`
}

// shortestRun is the run with the fewest steps that fails with a signature.
type shortestRun struct {
	Seed   uint64 `json:"seed,string"`
	Steps  int    `json:"steps"`
	Commit string `json:"commit"`
	Replay string `json:"replay"`
}

// signatureRecord is the signature.json file of a signature.
type signatureRecord struct {
	ID        string      `json:"id"`
	Kind      string      `json:"kind"`
	Text      string      `json:"text"`
	FirstLine string      `json:"first_line"` // first line of the first example
	Count     int         `json:"count"`
	FirstSeen string      `json:"first_seen"`
	LastSeen  string      `json:"last_seen"`
	Examples  int         `json:"examples"` // number of seed-N folders
	Shortest  shortestRun `json:"shortest"`
	Shrunk    bool        `json:"shrunk"` // the shrink of the first example completed
}

// failureLog writes failures.jsonl and the signatures directory.
// It is safe for concurrent use.
type failureLog struct {
	dir         string
	maxExamples int

	mu         sync.Mutex
	signatures map[string]*signatureRecord
}

// openFailureLog makes the log in dir and loads the signatures of earlier runs of the supervisor.
// A signature from an earlier run is not new, so it does not cause a second alert or a second shrink.
func openFailureLog(dir string, maxExamples int) (*failureLog, error) {
	failures := &failureLog{dir: dir, maxExamples: maxExamples, signatures: make(map[string]*signatureRecord)}
	if err := os.MkdirAll(filepath.Join(dir, "signatures"), 0o755); err != nil {
		return nil, err
	}
	paths, err := filepath.Glob(filepath.Join(dir, "signatures", "*", "signature.json"))
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var record signatureRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		failures.signatures[record.ID] = &record
	}
	return failures, nil
}

// replayCommand returns the command that replays a seed at a commit.
func replayCommand(commit string, seed uint64, steps int) string {
	return fmt.Sprintf("git checkout %s && go test -run 'TestBlackout$' -blackout.seed=%d -blackout.steps=%d -blackout.trace=200 -v .", commit, seed, steps)
}

// record appends entry to failures.jsonl and adds it to its signature.
// report and rawResult go into an example folder when the signature has fewer than maxExamples examples.
// It returns a copy of the signature and reports whether the signature is new.
func (failures *failureLog) record(entry failureEntry, report string, rawResult []byte) (signatureRecord, bool, error) {
	failures.mu.Lock()
	defer failures.mu.Unlock()
	appendErr := appendJSONLine(filepath.Join(failures.dir, "failures.jsonl"), entry)
	record, found := failures.signatures[entry.Signature]
	if !found {
		record = &signatureRecord{
			ID:        entry.Signature,
			Kind:      entry.Kind,
			Text:      entry.SignatureText,
			FirstLine: entry.FirstLine,
			FirstSeen: entry.Time,
			Shortest:  shortestRun{Seed: entry.Seed, Steps: entry.Steps, Commit: entry.Commit, Replay: entry.Replay},
		}
		failures.signatures[entry.Signature] = record
	}
	record.Count++
	record.LastSeen = entry.Time
	if entry.Steps < record.Shortest.Steps {
		record.Shortest = shortestRun{Seed: entry.Seed, Steps: entry.Steps, Commit: entry.Commit, Replay: entry.Replay}
	}
	var exampleErr error
	if record.Examples < failures.maxExamples {
		exampleErr = failures.writeExample(entry, report, rawResult)
		if exampleErr == nil {
			record.Examples++
		}
	}
	signatureErr := failures.writeSignature(record)
	return *record, !found, errors.Join(appendErr, exampleErr, signatureErr)
}

// shorten stores a shorter failing run of a signature.
// It does nothing when the signature already has a run with steps or fewer steps.
func (failures *failureLog) shorten(id string, run shortestRun) error {
	failures.mu.Lock()
	defer failures.mu.Unlock()
	record, found := failures.signatures[id]
	if !found || record.Shortest.Steps <= run.Steps {
		return nil
	}
	record.Shortest = run
	return failures.writeSignature(record)
}

// markShrunk records that the shrink of a signature completed.
func (failures *failureLog) markShrunk(id string) error {
	failures.mu.Lock()
	defer failures.mu.Unlock()
	record, found := failures.signatures[id]
	if !found {
		return nil
	}
	record.Shrunk = true
	return failures.writeSignature(record)
}

// snapshot returns a copy of each signature.
func (failures *failureLog) snapshot() []signatureRecord {
	failures.mu.Lock()
	defer failures.mu.Unlock()
	records := make([]signatureRecord, 0, len(failures.signatures))
	for _, record := range failures.signatures {
		records = append(records, *record)
	}
	return records
}

// signatureDir returns the directory of a signature.
func (failures *failureLog) signatureDir(id string) string {
	return filepath.Join(failures.dir, "signatures", id)
}

func (failures *failureLog) writeSignature(record *signatureRecord) error {
	return writeJSONFile(filepath.Join(failures.signatureDir(record.ID), "signature.json"), record)
}

func (failures *failureLog) writeExample(entry failureEntry, report string, rawResult []byte) error {
	dir := filepath.Join(failures.signatureDir(entry.Signature), fmt.Sprintf("seed-%d", entry.Seed))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.txt"), []byte(report), 0o644); err != nil {
		return err
	}
	return writeJSONFile(filepath.Join(dir, "result.json"), exampleResult{failureEntry: entry, WorkerResult: rawResult})
}

// appendJSONLine appends value to path as one line of JSON.
func appendJSONLine(path string, value any) error {
	data, err := encodeJSON(value, "")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	return errors.Join(err, file.Close())
}

// writeJSONFile writes value to path through a temporary file, so that a reader never sees a partial file.
func writeJSONFile(path string, value any) error {
	data, err := encodeJSON(value, "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

// encodeJSON encodes value as JSON with a final newline.
// It does not escape the characters &, <, and >, so that a replay command stays easy to copy.
func encodeJSON(value any, indent string) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", indent)
	err := encoder.Encode(value)
	return buffer.Bytes(), err
}
