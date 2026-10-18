package main

import (
	"context"
	"fmt"
)

// follower finds new commits on a remote branch and checks them out.
type follower struct {
	git    gitRunner
	remote string
	branch string
	// tried is the last commit that the follower checked out.
	// The follower does not try it again, so a commit that does not compile does not cause a new compile at each interval.
	tried string
}

// shouldFollow reports whether the supervisor must change to the remote commit.
func shouldFollow(remote, built, tried string) bool {
	return remote != "" && remote != built && remote != tried
}

// check fetches the remote and checks out the head of the branch when it is a new commit.
// built is the full hash of the commit of the running test binary.
// It returns the new commit, or "" when nothing changed.
func (follower *follower) check(ctx context.Context, built string) (string, error) {
	if _, err := follower.git(ctx, "fetch", "--quiet", follower.remote); err != nil {
		return "", err
	}
	ref := follower.remote + "/" + follower.branch
	remote, err := follower.git(ctx, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return "", err
	}
	if !shouldFollow(remote, built, follower.tried) {
		return "", nil
	}
	follower.tried = remote
	if _, err := follower.git(ctx, "checkout", "--quiet", "--detach", remote); err != nil {
		return "", fmt.Errorf("check out %s (%s): %w", ref, remote, err)
	}
	return remote, nil
}
