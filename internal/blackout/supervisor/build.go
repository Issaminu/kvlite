package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// build is one compiled test binary and the source that it comes from.
type build struct {
	Commit     string // short hash of HEAD
	FullCommit string // full hash of HEAD
	Dirty      bool   // the checkout had changes
	Binary     string // path of the test binary
	GoVersion  string // version of the Go toolchain that compiled the binary
	GOOS       string
	GOARCH     string
}

// label returns the commit, with "-dirty" when the checkout had changes.
func (b *build) label() string {
	if b.Dirty {
		return b.Commit + "-dirty"
	}
	return b.Commit
}

// gitRunner runs git with args in the checkout and returns its output without surrounding space.
type gitRunner func(ctx context.Context, args ...string) (string, error)

// commandGit returns a gitRunner that runs the git command in repo.
func commandGit(repo string) gitRunner {
	return func(ctx context.Context, args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repo}, args...)...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		output, err := cmd.Output()
		if err != nil {
			return "", fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
		}
		return strings.TrimSpace(string(output)), nil
	}
}

// sourceState returns the short and full hash of HEAD and whether the checkout has changes.
// exclude is a path in the checkout that does not count as a change, such as the output directory, or "".
func sourceState(ctx context.Context, git gitRunner, exclude string) (short, full string, dirty bool, err error) {
	if full, err = git(ctx, "rev-parse", "HEAD"); err != nil {
		return "", "", false, err
	}
	if short, err = git(ctx, "rev-parse", "--short", "HEAD"); err != nil {
		return "", "", false, err
	}
	args := []string{"status", "--porcelain", "--", "."}
	if exclude != "" {
		args = append(args, ":(exclude)"+exclude)
	}
	status, err := git(ctx, args...)
	if err != nil {
		return "", "", false, err
	}
	return short, full, status != "", nil
}

// compileWorker compiles the test binary of the root package in repo into binDir.
// exclude is passed to sourceState.
// The compile runs at the lowest CPU priority, with at most parallel programs at the same time.
func compileWorker(ctx context.Context, goTool, repo, binDir, exclude string, parallel int) (*build, error) {
	git := commandGit(repo)
	short, full, dirty, err := sourceState(ctx, git, exclude)
	if err != nil {
		return nil, err
	}
	result := &build{Commit: short, FullCommit: full, Dirty: dirty}
	result.Binary = filepath.Join(binDir, "blackout-"+result.label()+".test")
	temporary := result.Binary + ".tmp"
	name, args := lowPriority(goTool, "test", "-c", "-p", strconv.Itoa(parallel), "-o", temporary, ".")
	compile := exec.CommandContext(ctx, name, args...)
	compile.Dir = repo
	if output, err := compile.CombinedOutput(); err != nil {
		os.Remove(temporary)
		return nil, fmt.Errorf("compile the test binary of %s: %v\n%s", result.label(), err, output)
	}
	// A rename keeps the file of a worker that still runs an old binary with the same name.
	if err := os.Rename(temporary, result.Binary); err != nil {
		return nil, err
	}
	environment := exec.CommandContext(ctx, goTool, "env", "GOVERSION", "GOOS", "GOARCH")
	environment.Dir = repo
	output, err := environment.Output()
	if err != nil {
		return nil, fmt.Errorf("go env: %w", err)
	}
	if fields := strings.Fields(string(output)); len(fields) == 3 {
		result.GoVersion, result.GOOS, result.GOARCH = fields[0], fields[1], fields[2]
	}
	return result, nil
}

// excludedPath returns out relative to repo when out is inside repo, or "" when it is not.
// The output directory inside the checkout must not make the checkout dirty.
func excludedPath(repo, out string) string {
	relative, err := filepath.Rel(repo, out)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return ""
	}
	return filepath.ToSlash(relative)
}
