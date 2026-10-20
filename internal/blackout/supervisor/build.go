package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// build is one compiled test binary and the source that it comes from.
type build struct {
	Commit     string // short hash of HEAD
	FullCommit string // full hash of HEAD
	Dirty      bool   // the checkout had changes
	Binary     string // path of the test binary
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
// A file that .gitignore lists is no change, for example in the default output directory.
func sourceState(ctx context.Context, git gitRunner) (short, full string, dirty bool, err error) {
	if full, err = git(ctx, "rev-parse", "HEAD"); err != nil {
		return "", "", false, err
	}
	if short, err = git(ctx, "rev-parse", "--short", "HEAD"); err != nil {
		return "", "", false, err
	}
	status, err := git(ctx, "status", "--porcelain", "--", ".")
	if err != nil {
		return "", "", false, err
	}
	return short, full, status != "", nil
}

// compileWorker compiles the test binary of the root package in repo into binDir.
// The compile runs at the lowest CPU priority.
// The workers run on this computer, so the binary is for the operating system and architecture of the supervisor.
func compileWorker(ctx context.Context, goTool, repo, binDir string) (*build, error) {
	git := commandGit(repo)
	short, full, dirty, err := sourceState(ctx, git)
	if err != nil {
		return nil, err
	}
	result := &build{Commit: short, FullCommit: full, Dirty: dirty, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH}
	result.Binary = filepath.Join(binDir, "blackout-"+result.label()+".test")
	temporary := result.Binary + ".tmp"
	name, args := lowPriority(goTool, "test", "-c", "-o", temporary, ".")
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
	return result, nil
}
