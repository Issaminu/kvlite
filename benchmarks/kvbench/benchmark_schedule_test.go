package kvbench

import (
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
