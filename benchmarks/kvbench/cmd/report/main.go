package main

import (
	"bufio"
	"cmp"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

type result struct {
	Mode      string    `json:"mode"`
	Benchmark string    `json:"benchmark"`
	Engine    string    `json:"engine"`
	Metric    string    `json:"metric"`
	Samples   []float64 `json:"samples"`
	Mean      float64   `json:"mean"`
	Median    float64   `json:"median"`
	Min       float64   `json:"min"`
	Max       float64   `json:"max"`
}

type comparison struct {
	Mode          string  `json:"mode"`
	Case          string  `json:"case"`
	Peer          string  `json:"peer"`
	KVLiteMedian  float64 `json:"kvlite_median_ns_per_op"`
	PeerMedian    float64 `json:"peer_median_ns_per_op"`
	KVLiteMin     float64 `json:"kvlite_min_ns_per_op"`
	KVLiteMax     float64 `json:"kvlite_max_ns_per_op"`
	PeerMin       float64 `json:"peer_min_ns_per_op"`
	PeerMax       float64 `json:"peer_max_ns_per_op"`
	SlowerByPct   float64 `json:"kvlite_slower_by_percent"`
	RangesOverlap bool    `json:"ranges_overlap"`
}

type report struct {
	Environment     string        `json:"environment"`
	ExpectedSamples int           `json:"expected_samples"`
	Results         []result      `json:"results"`
	Comparisons     []comparison  `json:"comparisons"`
	Profiles        []caseProfile `json:"profiles,omitempty"`
}

type hotspot struct {
	Function string `json:"function"`
	Mean     string `json:"mean_per_repeat"`
	Percent  string `json:"percent"`
}

type caseProfile struct {
	ID            string    `json:"id"`
	Mode          string    `json:"mode"`
	Benchmark     string    `json:"benchmark"`
	Engine        string    `json:"engine"`
	Repeats       int       `json:"repeats"`
	MedianNSPerOp float64   `json:"median_ns_per_op"`
	PeerGaps      []peerGap `json:"peer_gaps,omitempty"`
	SampledCPUMs  float64   `json:"sampled_cpu_ms"`
	SparseCPU     bool      `json:"sparse_cpu"`
	LimitedCPU    bool      `json:"limited_cpu"`
	CPUSelf       []hotspot `json:"cpu_self"`
	CPUStack      []hotspot `json:"cpu_stack"`
	AllocSpace    []hotspot `json:"alloc_space"`
	AllocStack    []hotspot `json:"alloc_stack"`
}

type peerGap struct {
	Peer          string  `json:"peer"`
	SlowerByPct   float64 `json:"kvlite_slower_by_percent"`
	RangesOverlap bool    `json:"ranges_overlap"`
}

var cpuSuffix = regexp.MustCompile(`-[0-9]+$`)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: report report DIR | cases DIR | check-profile FILE NAME COUNT")
	}
	switch args[0] {
	case "report":
		if len(args) != 2 {
			return errors.New("report needs a result directory")
		}
		return writeReport(args[1])
	case "cases":
		if len(args) != 2 {
			return errors.New("cases needs a result directory")
		}
		return listCases(args[1], os.Stdout)
	case "check-profile":
		if len(args) != 4 {
			return errors.New("check-profile needs a file, name, and count")
		}
		expected, err := strconv.Atoi(args[3])
		if err != nil || expected < 1 {
			return errors.New("profile count must be a positive integer")
		}
		return checkCase(args[1], args[2], expected)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func readBenchmarks(path string) ([]result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var rows []result
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 || !strings.HasPrefix(fields[0], "Benchmark") {
			continue
		}
		name := cpuSuffix.ReplaceAllString(fields[0], "")
		parts := strings.Split(name, "/")
		engine := parts[len(parts)-1]
		if engine != "kvlite" && engine != "bbolt" && engine != "redis" {
			return nil, fmt.Errorf("%s: unknown engine in %q", path, name)
		}
		if _, err := strconv.Atoi(fields[1]); err != nil {
			return nil, fmt.Errorf("%s: invalid iteration count in %q", path, name)
		}
		if len(fields)%2 != 0 {
			return nil, fmt.Errorf("%s: incomplete metrics in %q", path, name)
		}
		for i := 2; i < len(fields); i += 2 {
			value, err := strconv.ParseFloat(fields[i], 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("%s: invalid %s in %q", path, fields[i+1], name)
			}
			rows = append(rows, result{Benchmark: name, Engine: engine, Metric: fields[i+1], Samples: []float64{value}})
		}
	}
	return rows, scanner.Err()
}

func checkCase(path, name string, expected int) error {
	rows, err := readBenchmarks(path)
	if err != nil {
		return err
	}
	count := 0
	for _, row := range rows {
		if row.Metric == "ns/op" && row.Benchmark == name {
			count++
		}
	}
	if count != expected {
		return fmt.Errorf("%s: expected %d %q results, found %d", path, expected, name, count)
	}
	return nil
}

func sampleCount(environment string) (int, error) {
	for _, line := range strings.Split(environment, "\n") {
		if value, ok := strings.CutPrefix(line, "Measured samples: "); ok {
			n, err := strconv.Atoi(strings.TrimSpace(value))
			if err == nil && n > 0 {
				return n, nil
			}
		}
	}
	return 0, errors.New("environment.txt has no valid Measured samples value")
}

func collect(dir string) (report, error) {
	env, err := os.ReadFile(filepath.Join(dir, "environment.txt"))
	if err != nil {
		return report{}, err
	}
	expected, err := sampleCount(string(env))
	if err != nil {
		return report{}, err
	}
	r := report{Environment: string(env), ExpectedSamples: expected}
	index := make(map[string]int)
	for _, mode := range []string{"durable", "no-commit-sync"} {
		rows, err := readBenchmarks(filepath.Join(dir, mode+".txt"))
		if err != nil {
			return report{}, err
		}
		for _, row := range rows {
			key := mode + "\x00" + row.Benchmark + "\x00" + row.Metric
			if i, ok := index[key]; ok {
				r.Results[i].Samples = append(r.Results[i].Samples, row.Samples[0])
			} else {
				row.Mode = mode
				index[key] = len(r.Results)
				r.Results = append(r.Results, row)
			}
		}
	}
	if len(r.Results) == 0 {
		return report{}, errors.New("no benchmark results found")
	}
	for i := range r.Results {
		row := &r.Results[i]
		if len(row.Samples) != expected {
			return report{}, fmt.Errorf("%s %s %s: found %d samples, expected %d", row.Mode, row.Benchmark, row.Metric, len(row.Samples), expected)
		}
		values := slices.Clone(row.Samples)
		slices.Sort(values)
		row.Min, row.Max = values[0], values[len(values)-1]
		row.Median = (values[(len(values)-1)/2] + values[len(values)/2]) / 2
		for _, value := range values {
			row.Mean += value
		}
		row.Mean /= float64(len(values))
	}
	slices.SortFunc(r.Results, func(a, b result) int {
		return strings.Compare(a.Mode+"\x00"+a.Benchmark+"\x00"+a.Metric, b.Mode+"\x00"+b.Benchmark+"\x00"+b.Metric)
	})
	byCase := make(map[string]map[string]result)
	for _, row := range r.Results {
		if row.Metric != "ns/op" {
			continue
		}
		caseName, ok := strings.CutSuffix(row.Benchmark, "/"+row.Engine)
		if !ok {
			return report{}, fmt.Errorf("invalid benchmark name %q", row.Benchmark)
		}
		key := row.Mode + "\x00" + caseName
		if byCase[key] == nil {
			byCase[key] = make(map[string]result)
		}
		byCase[key][row.Engine] = row
	}
	for key, engines := range byCase {
		kv, ok := engines["kvlite"]
		if !ok {
			continue
		}
		mode, caseName, _ := strings.Cut(key, "\x00")
		for _, peer := range []string{"bbolt", "redis"} {
			other, ok := engines[peer]
			if !ok || other.Median == 0 {
				continue
			}
			r.Comparisons = append(r.Comparisons, comparison{
				Mode: mode, Case: caseName, Peer: peer,
				KVLiteMedian: kv.Median, PeerMedian: other.Median,
				KVLiteMin: kv.Min, KVLiteMax: kv.Max, PeerMin: other.Min, PeerMax: other.Max,
				SlowerByPct:   (kv.Median/other.Median - 1) * 100,
				RangesOverlap: kv.Min <= other.Max && other.Min <= kv.Max,
			})
		}
	}
	slices.SortFunc(r.Comparisons, func(a, b comparison) int {
		return strings.Compare(a.Mode+"\x00"+a.Case+"\x00"+a.Peer, b.Mode+"\x00"+b.Case+"\x00"+b.Peer)
	})
	return r, nil
}

func writeCSV(path string, header []string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	err = w.Write(header)
	if err == nil {
		err = w.WriteAll(rows)
	}
	return errors.Join(err, f.Close())
}

func number(n float64) string { return strconv.FormatFloat(n, 'f', -1, 64) }

func writeReport(dir string) error {
	r, err := collect(dir)
	if err != nil {
		return err
	}
	r.Profiles, err = readProfiles(dir, r)
	if err != nil {
		return err
	}
	if r.Comparisons == nil {
		r.Comparisons = []comparison{}
	}
	if r.Profiles == nil {
		r.Profiles = []caseProfile{}
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), append(data, '\n'), 0644); err != nil {
		return err
	}
	var summary, metrics, comparisons [][]string
	for _, row := range r.Results {
		metrics = append(metrics, []string{row.Mode, row.Benchmark, row.Metric, strconv.Itoa(len(row.Samples)), number(row.Mean), number(row.Median), number(row.Min), number(row.Max)})
		if row.Metric == "ns/op" {
			summary = append(summary, []string{row.Mode, row.Benchmark, row.Engine, strconv.Itoa(len(row.Samples)), number(row.Mean), number(row.Median), number(row.Min), number(row.Max)})
		}
	}
	for _, row := range r.Comparisons {
		comparisons = append(comparisons, []string{row.Mode, row.Case, row.Peer, number(row.KVLiteMedian), number(row.PeerMedian), number(row.KVLiteMin), number(row.KVLiteMax), number(row.PeerMin), number(row.PeerMax), number(row.SlowerByPct), strconv.FormatBool(row.RangesOverlap)})
	}
	if err := writeCSV(filepath.Join(dir, "summary.csv"), []string{"mode", "benchmark", "engine", "samples", "mean_ns_per_op", "median_ns_per_op", "min_ns_per_op", "max_ns_per_op"}, summary); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dir, "metrics.csv"), []string{"mode", "benchmark", "metric", "samples", "mean", "median", "min", "max"}, metrics); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dir, "comparisons.csv"), []string{"mode", "case", "peer", "kvlite_median_ns_per_op", "peer_median_ns_per_op", "kvlite_min_ns_per_op", "kvlite_max_ns_per_op", "peer_min_ns_per_op", "peer_max_ns_per_op", "kvlite_slower_by_percent", "ranges_overlap"}, comparisons); err != nil {
		return err
	}
	text := markdown(r)
	if err := os.WriteFile(filepath.Join(dir, "report.md"), []byte(text), 0644); err != nil {
		return err
	}
	fmt.Print(text)
	return nil
}

func formatNS(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.2fs", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.2fms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2fµs", ns/1e3)
	default:
		return fmt.Sprintf("%.1fns", ns)
	}
}

func formatRange(median, minValue, maxValue float64) string {
	return fmt.Sprintf("%s (%s–%s)", formatNS(median), formatNS(minValue), formatNS(maxValue))
}

func writeGaps(b *strings.Builder, peer, label string, rows []comparison) {
	if len(rows) == 0 {
		return
	}
	slices.SortFunc(rows, func(a, c comparison) int {
		if label == "Slower" {
			return cmp.Compare(c.SlowerByPct, a.SlowerByPct)
		}
		return cmp.Compare(a.SlowerByPct, c.SlowerByPct)
	})
	fmt.Fprintf(b, "**Largest clear gaps: KVLite %s**\n\n| Case | Median gap | KVLite median (range) | %s median (range) |\n| --- | ---: | ---: | ---: |\n", strings.ToLower(label), peer)
	for _, row := range rows[:min(5, len(rows))] {
		gap := row.SlowerByPct
		if label == "Faster" {
			gap = -gap
		}
		gapText := fmt.Sprintf("%.1f%% %s", gap, strings.ToLower(label))
		if gap >= 90 && label == "Faster" && row.KVLiteMedian > 0 {
			gapText = fmt.Sprintf("%.1f× faster", row.PeerMedian/row.KVLiteMedian)
		} else if gap >= 100 && label == "Slower" {
			gapText = fmt.Sprintf("%.1f× slower", row.KVLiteMedian/row.PeerMedian)
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s |\n", row.Case, gapText, formatRange(row.KVLiteMedian, row.KVLiteMin, row.KVLiteMax), formatRange(row.PeerMedian, row.PeerMin, row.PeerMax))
	}
	b.WriteByte('\n')
}

func markdown(r report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# KVBench report\n\n%d measured samples per case.\n\n", r.ExpectedSamples)
	b.WriteString("Matched engine gaps use median time. Clear gaps have nonoverlapping observed ranges. This is a useful lead, not proof of a stable speed difference. Full values are in [summary.csv](summary.csv), [comparisons.csv](comparisons.csv), and `report.json`. KVLite checkpoints data during close, so close-after-writes includes more work than bbolt close.\n\n")
	for _, mode := range []string{"durable", "no-commit-sync"} {
		var timings []result
		for _, row := range r.Results {
			if row.Mode == mode && row.Metric == "ns/op" {
				timings = append(timings, row)
			}
		}
		if len(timings) == 0 {
			continue
		}
		caseWord := "cases"
		if len(timings) == 1 {
			caseWord = "case"
		}
		fmt.Fprintf(&b, "## %s\n\n%d timed %s.\n\n", mode, len(timings), caseWord)
		if len(timings) <= 10 && len(r.Comparisons) == 0 {
			b.WriteString("| Case | Median (range) |\n| --- | ---: |\n")
			for _, row := range timings {
				fmt.Fprintf(&b, "| %s | %s |\n", row.Benchmark, formatRange(row.Median, row.Min, row.Max))
			}
			b.WriteByte('\n')
		}
		for _, peer := range []string{"bbolt", "redis"} {
			var faster, slower []comparison
			matched, overlapping := 0, 0
			for _, row := range r.Comparisons {
				if row.Mode != mode || row.Peer != peer {
					continue
				}
				matched++
				if row.RangesOverlap || row.SlowerByPct == 0 {
					overlapping++
				} else if row.SlowerByPct > 0 {
					slower = append(slower, row)
				} else {
					faster = append(faster, row)
				}
			}
			if matched == 0 {
				continue
			}
			fmt.Fprintf(&b, "### KVLite against %s\n\n%d matched cases: %d clear faster, %d clear slower, %d with overlapping ranges or equal medians.\n\n", peer, matched, len(faster), len(slower), overlapping)
			writeGaps(&b, peer, "Slower", slower)
			writeGaps(&b, peer, "Faster", faster)
		}
	}
	if len(r.Profiles) > 0 {
		var leads []caseProfile
		sparse, limited := 0, 0
		for _, p := range r.Profiles {
			if p.Engine != "kvlite" {
				continue
			}
			if p.SparseCPU {
				sparse++
				continue
			}
			if p.LimitedCPU {
				limited++
			}
			for _, gap := range p.PeerGaps {
				if gap.SlowerByPct > 0 && !gap.RangesOverlap {
					leads = append(leads, p)
					break
				}
			}
		}
		slices.SortFunc(leads, func(a, c caseProfile) int {
			return cmp.Compare(c.SampledCPUMs, a.SampledCPUMs)
		})
		fmt.Fprintf(&b, "## Profile leads\n\n%d KVLite profiles have under 200ms of CPU samples; %d more have under 1s. These need more samples for detailed CPU rankings. The table shows up to five KVLite cases with a clear slower peer gap and at least 200ms of CPU samples. CPU totals cover all repeats. Function values are sampled estimates per repeat. Profiles also include setup and checks. Inclusive values contain called functions.\n\n", sparse, limited)
		if len(leads) > 0 {
			b.WriteString("| Case | Peer gap | Total CPU samples | CPU self per repeat | CPU inclusive per repeat | Allocation inclusive per repeat | Files |\n| --- | --- | ---: | --- | --- | --- | --- |\n")
		}
		for _, p := range leads[:min(5, len(leads))] {
			var gaps []string
			for _, gap := range p.PeerGaps {
				if gap.SlowerByPct > 0 && !gap.RangesOverlap {
					gaps = append(gaps, fmt.Sprintf("+%.1f%% vs %s", gap.SlowerByPct, gap.Peer))
				}
			}
			cpuTime := fmt.Sprintf("%.0fms", p.SampledCPUMs)
			cpuSelf, cpuStack := firstHotspot(p.CPUSelf), firstHotspot(p.CPUStack)
			if p.LimitedCPU {
				cpuTime += " (limited)"
			}
			fmt.Fprintf(&b, "| %s: %s | %s | %s | %s | %s | %s | [CPU](profiles/%s/cpu-top.txt), [allocation](profiles/%s/alloc-cum.txt), [trace](profiles/%s/trace.out) |\n", p.Mode, p.Benchmark, strings.Join(gaps, "<br>"), cpuTime, cpuSelf, cpuStack, firstHotspot(p.AllocStack), p.ID, p.ID, p.ID)
		}
		b.WriteString("\nThe [profile index](profiles/index.tsv) maps every case to its raw CPU, allocation, and trace files. The [test binary](profiles/kvbench.test) supports `go tool pprof`. `report.json` has up to three functions per profile metric. Redis profiles cover its Go client, not the Redis server.\n\n")
	}
	return b.String()
}

func listCases(dir string, output io.Writer) error {
	r, err := collect(dir)
	if err != nil {
		return err
	}
	rounds, err := roundCount(r.Environment)
	if err != nil {
		return err
	}
	count := 0
	for _, row := range r.Results {
		if row.Metric != "ns/op" {
			continue
		}
		parts := strings.Split(row.Benchmark, "/")
		filters := make([]string, len(parts))
		for i, part := range parts {
			filters[i] = "^" + regexp.QuoteMeta(part) + "$"
		}
		count++
		if _, err := fmt.Fprintf(output, "%04d\t%s\t%s\t%s\t%s\t%d\n", count, row.Mode, row.Benchmark, row.Engine, strings.Join(filters, "/"), rounds); err != nil {
			return err
		}
	}
	return nil
}

func roundCount(environment string) (int, error) {
	for _, line := range strings.Split(environment, "\n") {
		if value, ok := strings.CutPrefix(line, "Measured rounds: "); ok {
			n, err := strconv.Atoi(strings.TrimSpace(value))
			if err == nil && n > 0 {
				return n, nil
			}
		}
	}
	return 0, errors.New("environment.txt has no valid Measured rounds value")
}

func firstHotspot(h []hotspot) string {
	if len(h) == 0 {
		return "—"
	}
	name := h[0].Function
	for _, item := range []struct{ prefix, label string }{
		{"github.com/Issaminu/kvlite", "kvlite"},
		{"go.etcd.io/bbolt", "bbolt"},
		{"github.com/redis/go-redis/v9", "redis"},
	} {
		if strings.HasPrefix(name, item.prefix) {
			name = item.label + strings.TrimPrefix(name, item.prefix)
			break
		}
	}
	return name + " (" + h[0].Mean + ", " + h[0].Percent + ")"
}

func engineFunction(name, engine string) bool {
	switch engine {
	case "kvlite":
		return (strings.HasPrefix(name, "github.com/Issaminu/kvlite.") || strings.HasPrefix(name, "github.com/Issaminu/kvlite/")) && !strings.Contains(name, "/benchmarks/kvbench.")
	case "bbolt":
		return strings.HasPrefix(name, "go.etcd.io/bbolt.") || strings.HasPrefix(name, "go.etcd.io/bbolt/")
	case "redis":
		return strings.HasPrefix(name, "github.com/redis/go-redis/v9.") || strings.HasPrefix(name, "github.com/redis/go-redis/v9/")
	}
	return false
}

func sampledCPU(path string, repeats int) (float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		_, value, ok := strings.Cut(line, "Total samples = ")
		if !ok {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			break
		}
		duration, err := time.ParseDuration(fields[0])
		if err == nil {
			return float64(duration) * float64(repeats) / float64(time.Millisecond), nil
		}
		break
	}
	return 0, fmt.Errorf("%s: no sampled CPU duration", path)
}

func readTop(path string, cumulative bool, engine string) ([]hotspot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var top []hotspot
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 6 || fields[0] == "flat" || !strings.HasSuffix(fields[1], "%") || !strings.HasSuffix(fields[4], "%") || (engine != "" && !engineFunction(fields[5], engine)) {
			continue
		}
		value, percent := fields[0], fields[1]
		if cumulative {
			value, percent = fields[3], fields[4]
		}
		top = append(top, hotspot{Function: strings.Join(fields[5:], " "), Mean: value, Percent: percent})
		if len(top) == 3 {
			break
		}
	}
	return top, nil
}

func readProfiles(dir string, r report) ([]caseProfile, error) {
	index, err := os.ReadFile(filepath.Join(dir, "profiles", "index.tsv"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(index) == 0 {
		return nil, errors.New("profile index is empty")
	}
	rounds, err := roundCount(r.Environment)
	if err != nil {
		return nil, err
	}
	known := make(map[string]result)
	for _, row := range r.Results {
		if row.Metric == "ns/op" {
			known[row.Mode+"\x00"+row.Benchmark] = row
		}
	}
	var profiles []caseProfile
	for _, line := range strings.Split(strings.TrimSpace(string(index)), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 6 || fields[0] != fmt.Sprintf("%04d", len(profiles)+1) || known[fields[1]+"\x00"+fields[2]].Engine != fields[3] {
			return nil, fmt.Errorf("invalid profile index row %q", line)
		}
		repeats, err := strconv.Atoi(fields[5])
		if err != nil || repeats != rounds {
			return nil, fmt.Errorf("invalid profile repeat count in %q", line)
		}
		p := caseProfile{ID: fields[0], Mode: fields[1], Benchmark: fields[2], Engine: fields[3], Repeats: repeats, MedianNSPerOp: known[fields[1]+"\x00"+fields[2]].Median}
		if p.Engine == "kvlite" {
			caseName, _ := strings.CutSuffix(p.Benchmark, "/kvlite")
			for _, comparison := range r.Comparisons {
				if comparison.Mode == p.Mode && comparison.Case == caseName {
					p.PeerGaps = append(p.PeerGaps, peerGap{Peer: comparison.Peer, SlowerByPct: comparison.SlowerByPct, RangesOverlap: comparison.RangesOverlap})
				}
			}
		}
		p.SampledCPUMs, err = sampledCPU(filepath.Join(dir, "profiles", p.ID, "cpu-top.txt"), repeats)
		if err != nil {
			return nil, err
		}
		p.SparseCPU = p.SampledCPUMs < 200
		p.LimitedCPU = !p.SparseCPU && p.SampledCPUMs < 1000
		for _, view := range []struct {
			file       string
			cumulative bool
			target     *[]hotspot
		}{
			{"cpu-top.txt", false, &p.CPUSelf},
			{"cpu-cum.txt", true, &p.CPUStack},
			{"alloc-top.txt", false, &p.AllocSpace},
			{"alloc-cum.txt", true, &p.AllocStack},
		} {
			engine := p.Engine
			if view.file == "alloc-top.txt" {
				engine = ""
			}
			*view.target, err = readTop(filepath.Join(dir, "profiles", p.ID, view.file), view.cumulative, engine)
			if err != nil {
				return nil, err
			}
		}
		profiles = append(profiles, p)
	}
	if len(profiles) != len(known) {
		return nil, fmt.Errorf("found %d profiles for %d timed cases", len(profiles), len(known))
	}
	return profiles, nil
}
