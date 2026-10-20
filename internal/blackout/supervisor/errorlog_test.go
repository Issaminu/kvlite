package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestErrorLogKeepsEverySeed(t *testing.T) {
	log := newErrorLog()
	if !log.add("fail: Open: bad", seedRecord{Seed: 1, Steps: 300}) {
		t.Fatal("the first failure of an error is not new")
	}
	for seed := uint64(2); seed <= 50; seed++ {
		if log.add("fail: Open: bad", seedRecord{Seed: seed, Steps: 300}) {
			t.Fatalf("failure %d of an error is new", seed)
		}
	}
	log.add("timeout: x", seedRecord{Seed: 99, Steps: 3000})
	records := log.snapshot()
	if len(records) != 2 || records[0].Error != "fail: Open: bad" || records[0].Count != 50 || len(records[0].Seeds) != 50 {
		t.Fatalf("snapshot: %+v", records)
	}
}

// TestErrorLogStaysBounded requires that the log keeps at most maxErrors errors and maxSeeds seeds of each, with exact counts, so that a run without end does not grow.
func TestErrorLogStaysBounded(t *testing.T) {
	log := newErrorLog()
	for seed := range uint64(3 * maxSeeds) {
		log.add("fail: Open: bad", seedRecord{Seed: seed + 1, Steps: 300})
	}
	for index := range maxErrors + 5 {
		if log.add(fmt.Sprintf("fail: error %c%c", 'a'+index/26, 'a'+index%26), seedRecord{Seed: 1, Steps: 300}) && index >= maxErrors-1 {
			t.Fatalf("error %d is new, but the log is full", index)
		}
	}
	records := log.snapshot()
	if len(records) != maxErrors+1 || records[0].Error != "fail: Open: bad" || records[0].Count != 3*maxSeeds || len(records[0].Seeds) != maxSeeds {
		t.Fatalf("%d records; the first: %s, count %d, %d seeds", len(records), records[0].Error, records[0].Count, len(records[0].Seeds))
	}
	for _, record := range records {
		if record.Error == overflowError && record.Count == 6 {
			return
		}
	}
	t.Fatalf("no overflow record with the 6 errors after the first %d", maxErrors)
}

func TestErrorLogShortenMovesTheSeedToTheFront(t *testing.T) {
	log := newErrorLog()
	log.add("fail: Open: bad", seedRecord{Seed: 1, Steps: 300})
	log.add("fail: Open: bad", seedRecord{Seed: 2, Steps: 3000})
	log.shorten("fail: Open: bad", seedRecord{Seed: 2, Steps: 40})
	log.shorten("fail: Open: bad", seedRecord{Seed: 2, Steps: 90}) // longer than the stored run: no change
	seeds := log.snapshot()[0].Seeds
	if len(seeds) != 2 || seeds[0].Seed != 2 || seeds[0].Steps != 40 || seeds[1].Seed != 1 {
		t.Fatalf("seeds after the shrink: %+v", seeds)
	}
}

func TestSeedFromSplitsTheStepFromTheMessage(t *testing.T) {
	record := seedFrom(runOutcome{Seed: math.MaxUint64, Steps: 300, FirstLine: "blackout seed 18446744073709551615 failed at step 87: Open: bad"})
	if record.FailedAt == nil || *record.FailedAt != 87 || record.Message != "Open: bad" {
		t.Fatalf("failed check: %+v", record)
	}
	if record := seedFrom(runOutcome{Seed: 5, Steps: 300, FirstLine: "panic: boom"}); record.FailedAt != nil || record.Message != "panic: boom" {
		t.Fatalf("panic: %+v", record)
	}
	// A large seed must survive JSON readers that use float64, so it is a string.
	data, err := json.Marshal(record)
	if err != nil || !strings.Contains(string(data), `"seed":"18446744073709551615"`) {
		t.Fatalf("JSON: %s, %v", data, err)
	}
}
