package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCollect(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"environment.txt":    "Measured rounds: 2\nMeasured samples: 2\n",
		"durable.txt":        "BenchmarkRead/scale=light/kvlite-8 1 100 ns/op 10 B/op\nBenchmarkRead/scale=light/bbolt 1 50 ns/op 5 B/op\nBenchmarkRead/scale=light/kvlite 1 120 ns/op 12 B/op\nBenchmarkRead/scale=light/bbolt-8 1 60 ns/op 6 B/op\n",
		"no-commit-sync.txt": "BenchmarkWrite/scale=light/kvlite 1 200 ns/op\nBenchmarkWrite/scale=light/kvlite 1 220 ns/op\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	r, err := collect(dir)
	if err != nil {
		t.Fatal(err)
	}
	var cases bytes.Buffer
	if err := listCases(dir, &cases); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(cases.String(), "\n"); got != 3 || !strings.Contains(cases.String(), "\t^BenchmarkRead$/^scale=light$/^kvlite$\t2\n") {
		t.Fatalf("cases = %q", cases.String())
	}
	if len(r.Comparisons) != 1 || r.Comparisons[0].SlowerByPct != 100 {
		t.Fatalf("comparison = %+v", r.Comparisons)
	}
	if err := checkCase(filepath.Join(dir, "durable.txt"), "BenchmarkRead/scale=light/kvlite", 1); err == nil {
		t.Fatal("accepted two results for one benchmark case")
	}
	if err := writeReport(dir); err != nil {
		t.Fatal(err)
	}
	report, err := os.ReadFile(filepath.Join(dir, "report.md"))
	if err != nil || !strings.Contains(string(report), "Largest losses") {
		t.Fatalf("report = %q, error = %v", report, err)
	}
	files["durable.txt"] = strings.Replace(files["durable.txt"], "BenchmarkRead/scale=light/bbolt-8 1 60 ns/op 6 B/op\n", "", 1)
	if err := os.WriteFile(filepath.Join(dir, "durable.txt"), []byte(files["durable.txt"]), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := collect(dir); err == nil {
		t.Fatal("accepted an incomplete sample set")
	}
}

func TestProfileReport(t *testing.T) {
	dir := t.TempDir()
	caseDir := filepath.Join(dir, "profiles", "0001")
	if err := os.MkdirAll(caseDir, 0700); err != nil {
		t.Fatal(err)
	}
	name := "BenchmarkRead/scale=light/kvlite"
	index := "0001\tdurable\t" + name + "\tkvlite\t^BenchmarkRead$/^scale=light$/^kvlite$\t3\n"
	if err := os.WriteFile(filepath.Join(dir, "profiles", "index.tsv"), []byte(index), 0600); err != nil {
		t.Fatal(err)
	}
	top := "flat flat% sum% cum cum% name\n2ms 50.00% 50.00% 3ms 75.00% github.com/Issaminu/kvlite.Get\n"
	for _, file := range []string{"cpu-top.txt", "cpu-cum.txt", "alloc-top.txt", "sync-top.txt"} {
		if err := os.WriteFile(filepath.Join(caseDir, file), []byte(top), 0600); err != nil {
			t.Fatal(err)
		}
	}
	r := report{
		Environment: "Measured rounds: 3\n",
		Results:     []result{{Mode: "durable", Benchmark: name, Engine: "kvlite", Metric: "ns/op", Median: 42}},
		Comparisons: []comparison{{Mode: "durable", Case: "BenchmarkRead/scale=light", Peer: "bbolt", SlowerByPct: 30}},
	}
	profiles, err := readProfiles(dir, r)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 || profiles[0].CPUSelf[0].Mean != "2ms" || profiles[0].CPUStack[0].Mean != "3ms" || profiles[0].Repeats != 3 || profiles[0].MedianNSPerOp != 42 || len(profiles[0].PeerGaps) != 1 {
		t.Fatalf("profiles = %+v", profiles)
	}
	r.Profiles = profiles
	if text := markdown(r); !strings.Contains(text, "github.com/Issaminu/kvlite.Get (2ms, 50.00%)") || !strings.Contains(text, "+30.0% vs bbolt") {
		t.Fatalf("profile summary = %q", text)
	}
}
