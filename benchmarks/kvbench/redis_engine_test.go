package kvbench

import (
	"context"
	"strings"
	"testing"
)

func TestRedisOpenRequiresFlushPermission(t *testing.T) {
	_, err := openRedisEngine(context.Background(), "127.0.0.1:1", DurabilityDurable, 1, false)
	if err == nil || !strings.Contains(err.Error(), "KVBENCH_REDIS_FLUSHDB=1") {
		t.Fatalf("got %v, want flush permission error", err)
	}
}

func TestRedisOptionsDisableRetriesAndExtraCommands(t *testing.T) {
	options := redisOptions("127.0.0.1:6379", 8)
	if options.MaxRetries != -1 {
		t.Fatalf("maximum retries: got %d, want -1", options.MaxRetries)
	}
	if !options.DisableIdentity {
		t.Fatal("client identity command is enabled")
	}
	if options.PoolSize != 8 {
		t.Fatalf("pool size: got %d, want 8", options.PoolSize)
	}
	if options.Protocol != 2 {
		t.Fatalf("protocol: got %d, want RESP2", options.Protocol)
	}
}

func TestRedisDurabilityExpectation(t *testing.T) {
	for _, test := range []struct {
		mode DurabilityMode
		want string
	}{
		{mode: DurabilityDurable, want: "always"},
		{mode: DurabilityNoCommitSync, want: "no"},
	} {
		got, err := redisAppendFsync(test.mode)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("mode %q: got %q, want %q", test.mode, got, test.want)
		}
	}
}
