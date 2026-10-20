package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// maxIssuesPerCommit is the number of new issues that one commit can make.
	// A broken commit can fail with many errors, and the errors after the limit go into the log only.
	maxIssuesPerCommit = 5
	// maxIssueReport is the number of bytes of the report in an issue, because GitHub allows 65,536 characters in the body.
	maxIssueReport = 50000
	// issueTimeout is the time limit of the calls for one error.
	issueTimeout = 30 * time.Second
)

// errIssueLimit is the error of a new error that gets no issue, because its commit already made maxIssuesPerCommit issues.
var errIssueLimit = errors.New("the commit already made the maximum number of new issues")

// issueTracker makes a GitHub issue for each new error, and adds a comment when an error with an open issue comes back.
// It is safe for concurrent use.
type issueTracker struct {
	api    string // root of the GitHub REST API; tests change it
	repo   string // owner/name
	token  string
	client *http.Client

	mu      sync.Mutex
	numbers map[string]int // the open issue of an error on one commit, by signature key and full commit hash
	created map[string]int // the issues that this process made for each full commit hash
}

func newIssueTracker(repo, token string) *issueTracker {
	return &issueTracker{
		api:     "https://api.github.com",
		repo:    repo,
		token:   token,
		client:  &http.Client{Timeout: issueTimeout},
		numbers: make(map[string]int),
		created: make(map[string]int),
	}
}

// signatureKey returns a short key of an error text.
// The body of the issue holds the key, so a search finds the issue of the error again, also after a restart.
func signatureKey(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:6])
}

// report makes the issue of a new error, or adds a comment to the open issue of the same error.
// It returns errIssueLimit when the commit already made maxIssuesPerCommit issues.
func (tracker *issueTracker) report(ctx context.Context, current *build, platform, text string, outcome runOutcome) error {
	key := signatureKey(text)
	number, err := tracker.find(ctx, current, key)
	if err != nil {
		return err
	}
	if number > 0 {
		return tracker.comment(ctx, number, "The Blackout supervisor found this error again.\n\n"+runTable(current, platform, outcome))
	}
	tracker.mu.Lock()
	if tracker.created[current.FullCommit] >= maxIssuesPerCommit {
		tracker.mu.Unlock()
		return errIssueLimit
	}
	tracker.created[current.FullCommit]++
	tracker.mu.Unlock()
	issue := map[string]any{
		"title":  truncate("Blackout: "+text, 250),
		"body":   issueBody(current, platform, text, outcome),
		"labels": []string{"blackout"},
	}
	var created struct {
		Number int `json:"number"`
	}
	if err := tracker.call(ctx, http.MethodPost, "/repos/"+tracker.repo+"/issues", issue, &created); err != nil {
		return err
	}
	tracker.mu.Lock()
	tracker.numbers[key+" "+current.FullCommit] = created.Number
	tracker.mu.Unlock()
	return nil
}

// shrunk adds the shorter run of a finished shrink to the open issue of the error.
func (tracker *issueTracker) shrunk(ctx context.Context, current *build, text string, seed uint64, steps int) error {
	number, err := tracker.find(ctx, current, signatureKey(text))
	if err != nil || number == 0 {
		return err
	}
	body := fmt.Sprintf("The shrink found a shorter run of this error, with %d steps.\nReplay it at commit %s:\n\n```sh\n%s\n```\n", steps, current.FullCommit, replayCommand(seed, steps))
	return tracker.comment(ctx, number, body)
}

// find returns the number of the open issue of the error with key, or zero.
// It keeps the number for the commit, because the search of GitHub can find a new issue only after some time.
// A new commit searches again, so an error that comes back after its issue was closed gets a new issue.
func (tracker *issueTracker) find(ctx context.Context, current *build, key string) (int, error) {
	tracker.mu.Lock()
	number := tracker.numbers[key+" "+current.FullCommit]
	tracker.mu.Unlock()
	if number > 0 {
		return number, nil
	}
	query := fmt.Sprintf(`repo:%s is:issue is:open in:body "blackout-signature: %s"`, tracker.repo, key)
	var found struct {
		Items []struct {
			Number int `json:"number"`
		} `json:"items"`
	}
	if err := tracker.call(ctx, http.MethodGet, "/search/issues?q="+url.QueryEscape(query), nil, &found); err != nil {
		return 0, err
	}
	if len(found.Items) == 0 {
		return 0, nil
	}
	number = found.Items[0].Number
	tracker.mu.Lock()
	tracker.numbers[key+" "+current.FullCommit] = number
	tracker.mu.Unlock()
	return number, nil
}

// comment adds a comment to an issue.
func (tracker *issueTracker) comment(ctx context.Context, number int, body string) error {
	return tracker.call(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/comments", tracker.repo, number), map[string]string{"body": body}, nil)
}

// call sends one request to the GitHub REST API, and it decodes the JSON answer into result when result is not nil.
func (tracker *issueTracker) call(ctx context.Context, method, path string, body, result any) error {
	var content io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		content = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, tracker.api+path, content)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+tracker.token)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	response, err := tracker.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		text, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return fmt.Errorf("GitHub %s %s: %s: %s", method, path, response.Status, strings.TrimSpace(string(text)))
	}
	if result == nil {
		return nil
	}
	return json.NewDecoder(response.Body).Decode(result)
}

// issueBody returns the body of the issue of a new error.
// It says that a program made the issue, and it holds the telemetry of the run, the replay command, the report, and the signature key.
func issueBody(current *build, platform, text string, outcome runOutcome) string {
	report := outcome.Report
	if len(report) > maxIssueReport {
		report = truncate(report, maxIssueReport) + "\n[The supervisor cut the report here. The full report is in reports/SEED.txt on the runner when it runs with -reports, and a replay prints it too.]"
	}
	var body strings.Builder
	body.WriteString("> [!NOTE]\n")
	body.WriteString("> The Blackout supervisor, the runner of the deterministic simulation test of KVLite, created this issue automatically when it found a new error.\n\n")
	fmt.Fprintf(&body, "**Error:** `%s`\n\n", text)
	body.WriteString(runTable(current, platform, outcome))
	fmt.Fprintf(&body, "\n<details><summary>Report of the run</summary>\n\n````text\n%s\n````\n\n</details>\n\n", report)
	fmt.Fprintf(&body, "<!-- blackout-signature: %s -->\n", signatureKey(text))
	return body.String()
}

// runTable returns the telemetry of one failed run as a Markdown table, followed by its replay command.
func runTable(current *build, platform string, outcome runOutcome) string {
	failedAt := "none (a panic, a timeout, or an error)"
	if record := seedFrom(outcome); record.FailedAt != nil {
		failedAt = strconv.Itoa(*record.FailedAt)
	}
	commit := current.FullCommit
	if current.Dirty {
		commit += " (the checkout had changes, so a replay is not exact)"
	}
	return fmt.Sprintf("| | |\n|---|---|\n| Kind | %s |\n| Seed | `%d` |\n| Steps | %d |\n| Failed at step | %s |\n| Commit | %s |\n| Platform | %s |\n\nReplay at this commit, on a platform with the same operating-system page size:\n\n```sh\n%s\n```\n",
		outcome.Kind, outcome.Seed, outcome.Steps, failedAt, commit, platform, replayCommand(outcome.Seed, outcome.Steps))
}

// replayCommand returns the command that replays a seed with a trace.
func replayCommand(seed uint64, steps int) string {
	return fmt.Sprintf("go test -run 'TestBlackout$' -blackout.seed=%d -blackout.steps=%d -blackout.trace=200 -v .", seed, steps)
}
