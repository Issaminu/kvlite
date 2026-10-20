package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// errorRecord is one error in status.json: one signature with every seed that failed with it.
type errorRecord struct {
	Error string       `json:"error"` // the signature text: the kind, then the message with each number as N
	Count int          `json:"count"`
	Seeds []seedRecord `json:"seeds"`
}

// seedRecord is one failed run.
// Seeds are strings, because many JSON readers lose the precision of integers larger than 2^53.
type seedRecord struct {
	Seed     uint64 `json:"seed,string"`
	Steps    int    `json:"steps"`               // the safety-phase steps of the run; a replay needs them
	FailedAt *int   `json:"failed_at,omitempty"` // the step of a failed check; a panic, a timeout, or an error has none
	Message  string `json:"message"`
}

// seedFrom returns the record of a failed run.
func seedFrom(outcome runOutcome) seedRecord {
	record := seedRecord{Seed: outcome.Seed, Steps: outcome.Steps, Message: outcome.FirstLine}
	if step := failedStep(outcome.FirstLine); step >= 0 {
		record.FailedAt = &step
		record.Message = reportPrefix.ReplaceAllString(outcome.FirstLine, "")
	}
	return record
}

// errorLog holds the errors of the current commit.
// It is safe for concurrent use.
type errorLog struct {
	mu     sync.Mutex
	errors map[string]*errorRecord // by signature text
}

func newErrorLog() *errorLog {
	return &errorLog{errors: make(map[string]*errorRecord)}
}

// reset removes every error, for a new commit.
func (log *errorLog) reset() {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.errors = make(map[string]*errorRecord)
}

const (
	// maxErrors is the number of errors that the log keeps; more errors go into one record with the text overflowError.
	maxErrors = 100
	// maxSeeds is the number of seeds that the log keeps for each error; more seeds only increase the count.
	maxSeeds = 100
	// overflowError is the text of the record that collects the errors after the first maxErrors.
	overflowError = "other: more errors than the log keeps; the log of the supervisor names them"
)

// add records a failed run with the error text and reports whether the error is new.
// The log keeps at most maxErrors errors and maxSeeds seeds of each error, so its memory and status.json stay small in a run without end.
func (log *errorLog) add(text string, seed seedRecord) bool {
	log.mu.Lock()
	defer log.mu.Unlock()
	record, found := log.errors[text]
	if !found && len(log.errors) >= maxErrors {
		// The overflow record is not an error with one cause, so it is never new and gets no shrink.
		text, found = overflowError, true
		if record = log.errors[text]; record == nil {
			record = &errorRecord{Error: text}
			log.errors[text] = record
		}
	}
	if !found {
		record = &errorRecord{Error: text}
		log.errors[text] = record
	}
	record.Count++
	if len(record.Seeds) < maxSeeds {
		record.Seeds = append(record.Seeds, seed)
	}
	return !found
}

// shorten replaces the run of a seed with a shorter failing run of the same seed, and moves it to the front.
func (log *errorLog) shorten(text string, run seedRecord) {
	log.mu.Lock()
	defer log.mu.Unlock()
	record, found := log.errors[text]
	if !found {
		return
	}
	index := slices.IndexFunc(record.Seeds, func(seed seedRecord) bool { return seed.Seed == run.Seed })
	if index < 0 || record.Seeds[index].Steps <= run.Steps {
		return
	}
	record.Seeds = slices.Insert(slices.Delete(record.Seeds, index, index+1), 0, run)
}

// snapshot returns a copy of each error, the most frequent first.
func (log *errorLog) snapshot() []errorRecord {
	log.mu.Lock()
	defer log.mu.Unlock()
	records := make([]errorRecord, 0, len(log.errors))
	for _, record := range log.errors {
		copied := *record
		copied.Seeds = slices.Clone(record.Seeds)
		records = append(records, copied)
	}
	slices.SortFunc(records, func(a, b errorRecord) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.Error, b.Error))
	})
	return records
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
// It does not escape the characters &, <, and >, so that messages stay easy to read.
func encodeJSON(value any, indent string) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", indent)
	err := encoder.Encode(value)
	return buffer.Bytes(), err
}
