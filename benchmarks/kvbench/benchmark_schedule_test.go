package kvbench

import (
	"bytes"
	"slices"
	"strconv"
	"testing"
)

func TestMixedScheduleUsesIndependentKeys(t *testing.T) {
	for _, readPercent := range []int{95, 50} {
		t.Run(strconv.Itoa(readPercent), func(t *testing.T) {
			benchmarkCase := benchmarkCase{
				operation:   benchmarkMixed,
				operations:  10_000,
				readPercent: readPercent,
				batchSize:   1,
			}
			schedule := makeOperationSchedule(benchmarkCase.operations, 10_000, benchmarkCase)
			readKeys := make(map[int]bool)
			writeKeys := make(map[int]bool)
			writeCount := 0
			for _, operation := range schedule {
				if operation.write {
					writeKeys[operation.pairIndex] = true
					writeCount++
				} else {
					readKeys[operation.pairIndex] = true
				}
			}
			wantWrites := benchmarkCase.operations * (100 - readPercent) / 100
			if writeCount != wantWrites {
				t.Fatalf("write count is %d, want %d", writeCount, wantWrites)
			}
			for key := range writeKeys {
				if readKeys[key] {
					return
				}
			}
			t.Fatal("read and write keys do not overlap")
		})
	}
}

func TestMissingKeysCoverEachSearchPosition(t *testing.T) {
	first := makeKey(1, 0)
	middle := makeKey(1, 50)
	next := makeKey(1, 51)
	last := makeKey(1, 99)
	if key := makeMissingKey("below", 0); bytes.Compare(key, first) >= 0 {
		t.Fatalf("below key %x is not below %x", key, first)
	}
	if key := makeMissingKey("between", 50); bytes.Compare(key, middle) <= 0 || bytes.Compare(key, next) >= 0 {
		t.Fatalf("between key %x is not between %x and %x", key, middle, next)
	}
	if key := makeMissingKey("above", 99); bytes.Compare(key, last) <= 0 {
		t.Fatalf("above key %x is not above %x", key, last)
	}
}

func TestScanChecksumDoesNotDependOnOrder(t *testing.T) {
	pairs := makeTextPairs(100, 128)
	checksum := uint64(0)
	for _, pair := range pairs {
		checksum = consumePair(checksum, pair.Key, pair.Value)
	}
	slices.Reverse(pairs)
	reversed := uint64(0)
	for _, pair := range pairs {
		reversed = consumePair(reversed, pair.Key, pair.Value)
	}
	if checksum != reversed {
		t.Fatalf("checksum changed with result order: %d != %d", checksum, reversed)
	}
}

func TestHotDistributionPlacesEightyPercentInHotSet(t *testing.T) {
	const records = 10_000
	const operations = 100_000
	hotReads := 0
	for operation := range operations {
		index := distributedIndex(operation, records, true)
		if index < 0 || index >= records {
			t.Fatalf("index %d is outside 0 through %d", index, records-1)
		}
		if index < records/5 {
			hotReads++
		}
	}
	if hotReads != operations*8/10 {
		t.Fatalf("hot reads are %d, want %d", hotReads, operations*8/10)
	}
}

func TestBatchCasesWriteEachRecordOnce(t *testing.T) {
	for _, benchmarkCase := range benchmarkCases() {
		if benchmarkCase.batchSize == 1 {
			continue
		}
		if got := benchmarkCase.operations * benchmarkCase.batchSize; got != benchmarkCase.records {
			t.Errorf("%s writes %d keys, want %d", benchmarkCase.name, got, benchmarkCase.records)
		}
	}
}
