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
	for _, profile := range []string{"large", "medium", "light"} {
		t.Run(profile, func(t *testing.T) {
			t.Setenv("KVBENCH_PROFILE", profile)
			t.Setenv("KVBENCH_WORKLOAD", "all")
			for _, benchmarkCase := range benchmarkCases() {
				if benchmarkCase.batchSize == 1 {
					continue
				}
				if got := benchmarkCase.operations * benchmarkCase.batchSize; got != benchmarkCase.records {
					t.Errorf("%s writes %d keys, want %d", benchmarkCase.name, got, benchmarkCase.records)
				}
			}
		})
	}
}

func TestProfileValue(t *testing.T) {
	t.Setenv("KVBENCH_PROFILE", "light")
	if got := profileValue(10_000, 1_000); got != 1_000 {
		t.Fatalf("light value is %d, want 1000", got)
	}
	t.Setenv("KVBENCH_PROFILE", "large")
	if got := profileValue(10_000, 1_000); got != 10_000 {
		t.Fatalf("large value is %d, want 10000", got)
	}
}

func TestProfileSizedValue(t *testing.T) {
	tests := []struct {
		profile string
		want    int
	}{
		{profile: "light", want: 1},
		{profile: "medium", want: 2},
		{profile: "large", want: 3},
	}
	for _, test := range tests {
		t.Run(test.profile, func(t *testing.T) {
			t.Setenv("KVBENCH_PROFILE", test.profile)
			if got := profileSizedValue(3, 2, 1); got != test.want {
				t.Fatalf("value is %d, want %d", got, test.want)
			}
		})
	}
}

func TestLatencyOperationCounts(t *testing.T) {
	tests := []struct {
		profile string
		read    int
		update  int
		mixed   int
		warmup  int
	}{
		{profile: "light", read: 10_000, update: 1_000, mixed: 1_000, warmup: 100},
		{profile: "medium", read: 100_000, update: 1_000, mixed: 10_000, warmup: 1_000},
		{profile: "large", read: 1_000_000, update: 10_000, mixed: 100_000, warmup: 10_000},
	}
	for _, test := range tests {
		t.Run(test.profile, func(t *testing.T) {
			t.Setenv("KVBENCH_PROFILE", test.profile)
			if got := latencyOperationCount(benchmarkRead); got != test.read {
				t.Errorf("read operations are %d, want %d", got, test.read)
			}
			if got := latencyOperationCount(benchmarkUpdate); got != test.update {
				t.Errorf("update operations are %d, want %d", got, test.update)
			}
			if got := latencyOperationCount(benchmarkMixed); got != test.mixed {
				t.Errorf("mixed operations are %d, want %d", got, test.mixed)
			}
			if got := latencyWarmupOperationCount(benchmarkRead); got != test.warmup {
				t.Errorf("read warm-up operations are %d, want %d", got, test.warmup)
			}
			if got := latencyWarmupOperationCount(benchmarkUpdate); got != 0 {
				t.Errorf("update warm-up operations are %d, want 0", got)
			}
		})
	}
}

func TestWorkloadSelectsMatchingCases(t *testing.T) {
	t.Setenv("KVBENCH_PROFILE", "medium")
	tests := []struct {
		workload string
		want     int
	}{
		{workload: "reads", want: 5},
		{workload: "writes", want: 8},
		{workload: "mixed", want: 2},
		{workload: "all", want: 15},
	}
	for _, test := range tests {
		t.Run(test.workload, func(t *testing.T) {
			t.Setenv("KVBENCH_WORKLOAD", test.workload)
			if got := len(benchmarkCases()); got != test.want {
				t.Fatalf("case count is %d, want %d", got, test.want)
			}
		})
	}
}

func TestLightProfileSelectsRepresentativeCoreCases(t *testing.T) {
	t.Setenv("KVBENCH_PROFILE", "light")
	t.Setenv("KVBENCH_WORKLOAD", "all")
	want := map[string]bool{
		"read/random/value=128/clients=1":             true,
		"update/random/value=128/clients=1":           true,
		"insert/random/value=128/clients=1":           true,
		"mixed/read=95/value=128/clients=8":           true,
		"update/random/value=128/clients=1/batch=100": true,
	}
	cases := benchmarkCases()
	if len(cases) != len(want) {
		t.Fatalf("light cases are %d, want %d", len(cases), len(want))
	}
	for _, benchmarkCase := range cases {
		if !want[benchmarkCase.name] {
			t.Errorf("unexpected light case %q", benchmarkCase.name)
		}
	}
}

func TestMediumProfileSelectsDecisionCoreCases(t *testing.T) {
	t.Setenv("KVBENCH_PROFILE", "medium")
	t.Setenv("KVBENCH_WORKLOAD", "all")
	want := map[string]bool{
		"read/random/value=128/clients=1":                       true,
		"read/random/value=3072/clients=1":                      true,
		"read/sequential/hits=100/value=128/clients=1":          true,
		"read/random/hits=0/misses=between/value=128/clients=1": true,
		"read/random/hits=100/value=128/clients=8":              true,
		"update/random/value=128/clients=1":                     true,
		"update/random/grow=32-1024/clients=1":                  true,
		"insert/random/value=128/clients=1":                     true,
		"update/random/value=128/clients=8":                     true,
		"insert/random/value=128/clients=8":                     true,
		"mixed/read=95/value=128/clients=8":                     true,
		"mixed/read=50/value=128/clients=8":                     true,
		"update/random/value=128/clients=1/batch=100":           true,
		"insert/random/value=128/clients=1/batch=100":           true,
		"update/random/value=128/clients=8/batch=100":           true,
	}
	cases := benchmarkCases()
	if len(cases) != len(want) {
		t.Fatalf("medium cases are %d, want %d", len(cases), len(want))
	}
	for _, benchmarkCase := range cases {
		if !want[benchmarkCase.name] {
			t.Errorf("unexpected medium case %q", benchmarkCase.name)
		}
	}
}
