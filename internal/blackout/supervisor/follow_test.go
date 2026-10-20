package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// fakeGit plays a git checkout whose remote branch points to remote.
type fakeGit struct {
	remote      string
	fetchErr    error
	checkoutErr error
	calls       []string
}

func (fake *fakeGit) run(ctx context.Context, args ...string) (string, error) {
	fake.calls = append(fake.calls, strings.Join(args, " "))
	switch args[0] {
	case "fetch":
		return "", fake.fetchErr
	case "rev-parse":
		return fake.remote, nil
	case "checkout":
		return "", fake.checkoutErr
	}
	return "", errors.New("unexpected git command")
}

func (fake *fakeGit) checkouts() []string {
	var checkouts []string
	for _, call := range fake.calls {
		if strings.HasPrefix(call, "checkout ") {
			checkouts = append(checkouts, call)
		}
	}
	return checkouts
}

func TestShouldFollow(t *testing.T) {
	for _, test := range []struct {
		remote, built, tried string
		want                 bool
	}{
		{"b", "a", "", true},
		{"a", "a", "", false},  // the binary has the remote commit
		{"b", "a", "b", false}, // the follower tried the remote commit already
		{"c", "a", "b", true},  // a newer commit after a failed one
		{"", "a", "", false},
	} {
		if got := shouldFollow(test.remote, test.built, test.tried); got != test.want {
			t.Errorf("shouldFollow(%q, %q, %q) = %v, want %v", test.remote, test.built, test.tried, got, test.want)
		}
	}
}

func TestFollowerChecksOutNewCommitOnce(t *testing.T) {
	ctx := context.Background()
	fake := &fakeGit{remote: "aaa"}
	follow := &follower{git: fake.run, remote: "origin", branch: "main"}

	if commit, err := follow.check(ctx, "aaa"); err != nil || commit != "" {
		t.Fatalf("same commit: got %q, %v; want no change", commit, err)
	}
	if !slices.Contains(fake.calls, "fetch --quiet origin") || !slices.Contains(fake.calls, "rev-parse --verify origin/main^{commit}") {
		t.Fatalf("git calls: %q", fake.calls)
	}
	if len(fake.checkouts()) != 0 {
		t.Fatalf("checkout without a new commit: %q", fake.checkouts())
	}

	fake.remote = "bbb"
	if commit, err := follow.check(ctx, "aaa"); err != nil || commit != "bbb" {
		t.Fatalf("new commit: got %q, %v; want bbb", commit, err)
	}
	if got := fake.checkouts(); !slices.Equal(got, []string{"checkout --quiet --detach bbb"}) {
		t.Fatalf("checkouts: got %q", got)
	}

	// The compile of bbb failed, so the binary still has aaa: the follower must not try bbb again.
	if commit, err := follow.check(ctx, "aaa"); err != nil || commit != "" {
		t.Fatalf("tried commit: got %q, %v; want no change", commit, err)
	}
	fake.remote = "ccc"
	if commit, err := follow.check(ctx, "aaa"); err != nil || commit != "ccc" {
		t.Fatalf("newer commit: got %q, %v; want ccc", commit, err)
	}
	if len(fake.checkouts()) != 2 {
		t.Fatalf("checkouts: got %q, want 2", fake.checkouts())
	}
}

func TestFollowerErrors(t *testing.T) {
	ctx := context.Background()
	fake := &fakeGit{remote: "bbb", fetchErr: errors.New("network down")}
	follow := &follower{git: fake.run, remote: "origin", branch: "main"}
	if _, err := follow.check(ctx, "aaa"); err == nil {
		t.Fatal("a failed fetch gave no error")
	}
	if len(fake.calls) != 1 {
		t.Fatalf("git calls after a failed fetch: %q", fake.calls)
	}

	fake.fetchErr = nil
	fake.checkoutErr = errors.New("local changes")
	if _, err := follow.check(ctx, "aaa"); err == nil {
		t.Fatal("a failed checkout gave no error")
	}
	fake.checkoutErr = nil
	if commit, err := follow.check(ctx, "aaa"); err != nil || commit != "" {
		t.Fatalf("after a failed checkout: got %q, %v; want no second try of the same commit", commit, err)
	}
}
