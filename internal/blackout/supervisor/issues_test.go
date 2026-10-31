package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeGitHub plays the three calls of the GitHub API that the issue tracker makes.
type fakeGitHub struct {
	mu       sync.Mutex
	open     map[string]int // signature key → number of an open issue that a search finds
	issues   []fakeIssue
	comments map[int][]string
}

type fakeIssue struct {
	Title  string   `json:"title"`
	Body   string   `json:"body"`
	Labels []string `json:"labels"`
}

func (fake *fakeGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer test-token" {
		http.Error(w, "bad token", http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/search/issues":
		query := r.URL.Query().Get("q")
		items := []map[string]int{}
		for key, number := range fake.open {
			if strings.Contains(query, key) && strings.Contains(query, "repo:owner/name") && strings.Contains(query, "is:open") {
				items = append(items, map[string]int{"number": number})
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	case r.Method == http.MethodPost && r.URL.Path == "/repos/owner/name/issues":
		var issue fakeIssue
		json.NewDecoder(r.Body).Decode(&issue)
		fake.issues = append(fake.issues, issue)
		json.NewEncoder(w).Encode(map[string]int{"number": 100 + len(fake.issues)})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/repos/owner/name/issues/") && strings.HasSuffix(r.URL.Path, "/comments"):
		var number int
		fmt.Sscanf(r.URL.Path, "/repos/owner/name/issues/%d/comments", &number)
		var comment struct{ Body string }
		json.NewDecoder(r.Body).Decode(&comment)
		fake.comments[number] = append(fake.comments[number], comment.Body)
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, "{}")
	default:
		http.Error(w, "unexpected call "+r.Method+" "+r.URL.Path, http.StatusNotFound)
	}
}

func TestIssueTrackerFilesOneIssueForEachError(t *testing.T) {
	fake := &fakeGitHub{open: make(map[string]int), comments: make(map[int][]string)}
	server := httptest.NewServer(fake)
	defer server.Close()
	tracker := newIssueTracker("owner/name", "test-token")
	tracker.api = server.URL
	ctx := context.Background()
	first := &build{Commit: "abc1234", FullCommit: "abc1234aaaa", GOOS: "linux", GOARCH: "arm64"}
	platform := "linux/arm64"
	text := "fail: recovery: lost commit N"
	failed := runOutcome{Kind: kindFail, Seed: 42, Steps: 300, FirstLine: "blackout seed 42 failed at step 7: recovery: lost commit 3", Report: "blackout seed 42 failed at step 7: recovery: lost commit 3\nconfig: page=4096"}

	// A new error with no open issue makes one issue with the telemetry of the run.
	if err := tracker.report(ctx, first, platform, text, failed); err != nil {
		t.Fatal(err)
	}
	if len(fake.issues) != 1 {
		t.Fatalf("issues: got %d, want 1", len(fake.issues))
	}
	issue := fake.issues[0]
	if !strings.HasPrefix(issue.Title, "Blackout: "+text) || len(issue.Labels) != 1 || issue.Labels[0] != "blackout" {
		t.Errorf("title %q, labels %q", issue.Title, issue.Labels)
	}
	for _, want := range []string{
		"The Blackout supervisor",
		"created this issue automatically",
		"`42`",
		"| Steps | 300 |",
		"| Failed at step | 7 |",
		"abc1234aaaa",
		platform,
		"-blackout.seed=42 -blackout.steps=300",
		"config: page=4096",
		"blackout-signature: " + signatureKey(text),
	} {
		if !strings.Contains(issue.Body, want) {
			t.Errorf("the issue body has no %q:\n%s", want, issue.Body)
		}
	}

	// The same error after a restart, on a new commit: the search finds the open issue, so the tracker adds a comment.
	fake.open[signatureKey(text)] = 101
	restarted := newIssueTracker("owner/name", "test-token")
	restarted.api = server.URL
	second := &build{Commit: "def5678", FullCommit: "def5678bbbb", GOOS: "linux", GOARCH: "arm64"}
	again := failed
	again.Seed = 43
	if err := restarted.report(ctx, second, platform, text, again); err != nil {
		t.Fatal(err)
	}
	if len(fake.issues) != 1 || len(fake.comments[101]) != 1 || !strings.Contains(fake.comments[101][0], "def5678bbbb") || !strings.Contains(fake.comments[101][0], "`43`") {
		t.Fatalf("after a restart: %d issues, comments %q", len(fake.issues), fake.comments[101])
	}

	// A finished shrink adds the shorter run to the issue.
	if err := tracker.shrunk(ctx, first, text, 42, 9); err != nil {
		t.Fatal(err)
	}
	if comments := fake.comments[101]; len(comments) != 2 || !strings.Contains(comments[1], "-blackout.seed=42 -blackout.steps=9") {
		t.Fatalf("after the shrink: comments %q", comments)
	}

	// One commit makes at most maxIssuesPerCommit issues, so a broken commit cannot make hundreds.
	for n := range maxIssuesPerCommit + 3 {
		err := tracker.report(ctx, first, platform, fmt.Sprintf("fail: other error %c", 'a'+n), failed)
		if err != nil && !errors.Is(err, errIssueLimit) {
			t.Fatal(err)
		}
	}
	if len(fake.issues) != maxIssuesPerCommit {
		t.Fatalf("issues of one commit: got %d, want %d", len(fake.issues), maxIssuesPerCommit)
	}
}

func TestIssuesFlagNeedsAToken(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	if _, _, err := parseOptions([]string{"-issues", "owner/name"}, io.Discard); err == nil {
		t.Error("-issues without GITHUB_TOKEN gave no error")
	}
	t.Setenv("GITHUB_TOKEN", "test-token")
	if _, _, err := parseOptions([]string{"-issues", "no-slash"}, io.Discard); err == nil {
		t.Error("-issues accepted a repository without owner")
	}
	opts, _, err := parseOptions([]string{"-issues", "owner/name"}, io.Discard)
	if err != nil || opts.issues != "owner/name" {
		t.Fatalf("-issues owner/name: got %q, %v", opts.issues, err)
	}
}
