package kvbench

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisEngine struct {
	client *redis.Client
	mode   DurabilityMode
}

func redisOptions(address string, clients int) *redis.Options {
	return &redis.Options{
		Addr:            address,
		Protocol:        2,
		PoolSize:        clients,
		MinIdleConns:    clients,
		MaxRetries:      -1,
		DisableIdentity: true,
	}
}

func openRedisEngine(ctx context.Context, address string, mode DurabilityMode, clients int, flushAllowed bool) (Engine, error) {
	if !flushAllowed {
		return nil, errors.New("Redis benchmark requires KVBENCH_REDIS_FLUSHDB=1 because it clears the selected database")
	}
	if clients < 1 {
		return nil, fmt.Errorf("Redis client count must be positive: %d", clients)
	}
	client := redis.NewClient(redisOptions(address, clients))
	engine := &redisEngine{client: client, mode: mode}
	if err := engine.warmConnections(ctx, clients); err != nil {
		return nil, errors.Join(err, client.Close())
	}
	if err := engine.validateDurability(ctx); err != nil {
		return nil, errors.Join(err, client.Close())
	}
	if err := engine.reset(ctx); err != nil {
		return nil, errors.Join(err, client.Close())
	}
	return engine, nil
}

func (engine *redisEngine) warmConnections(ctx context.Context, clients int) error {
	start := make(chan struct{})
	errorsByClient := make(chan error, clients)
	var ready sync.WaitGroup
	ready.Add(clients)
	for range clients {
		go func() {
			ready.Done()
			<-start
			errorsByClient <- engine.client.Ping(ctx).Err()
		}()
	}
	ready.Wait()
	close(start)
	for range clients {
		if err := <-errorsByClient; err != nil {
			return err
		}
	}
	return nil
}

func (engine *redisEngine) reset(ctx context.Context) error {
	if err := engine.client.FlushDB(ctx).Err(); err != nil {
		return err
	}
	if err := engine.client.BgRewriteAOF(ctx).Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Minute)
	for {
		info, err := engine.persistenceInfo(ctx)
		if err != nil {
			return err
		}
		if info["aof_rewrite_in_progress"] == "0" {
			if info["aof_last_bgrewrite_status"] != "ok" {
				return fmt.Errorf("Redis AOF rewrite status is %q", info["aof_last_bgrewrite_status"])
			}
			return engine.validatePersistenceIdle(ctx)
		}
		if time.Now().After(deadline) {
			return errors.New("Redis AOF rewrite did not finish within 60 seconds")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (engine *redisEngine) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, err := engine.client.Get(ctx, string(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrKeyNotFound
	}
	return value, err
}

func (engine *redisEngine) Put(ctx context.Context, key, value []byte) error {
	return engine.client.Set(ctx, string(key), value, 0).Err()
}

func (engine *redisEngine) PutBatch(ctx context.Context, pairs []Pair) error {
	_, err := engine.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, pair := range pairs {
			pipe.Set(ctx, string(pair.Key), pair.Value, 0)
		}
		return nil
	})
	return err
}

func (engine *redisEngine) Count(ctx context.Context) (int, error) {
	count, err := engine.client.DBSize(ctx).Result()
	if err != nil {
		return 0, err
	}
	if count > math.MaxInt {
		return 0, fmt.Errorf("Redis key count exceeds int: %d", count)
	}
	return int(count), nil
}

func (engine *redisEngine) Close() error { return engine.client.Close() }

func redisAppendFsync(mode DurabilityMode) (string, error) {
	switch mode {
	case DurabilityDurable:
		return "always", nil
	case DurabilityNoCommitSync:
		return "no", nil
	default:
		return "", fmt.Errorf("unsupported Redis durability mode %q", mode)
	}
}

func (engine *redisEngine) validateDurability(ctx context.Context) error {
	wantFsync, err := redisAppendFsync(engine.mode)
	if err != nil {
		return err
	}
	wants := map[string]string{
		"appendonly":                  "yes",
		"appendfsync":                 wantFsync,
		"save":                        "",
		"auto-aof-rewrite-percentage": "0",
		"no-appendfsync-on-rewrite":   "no",
	}
	for name, want := range wants {
		got, err := engine.configValue(ctx, name)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("Redis setting %s is %q, want %q", name, got, want)
		}
	}
	return engine.validatePersistenceIdle(ctx)
}

func (engine *redisEngine) configValue(ctx context.Context, name string) (string, error) {
	values, err := engine.client.ConfigGet(ctx, name).Result()
	if err != nil {
		return "", err
	}
	value, found := values[name]
	if !found {
		return "", fmt.Errorf("Redis CONFIG GET %s returned no value", name)
	}
	return value, nil
}

func (engine *redisEngine) validatePersistenceIdle(ctx context.Context) error {
	info, err := engine.persistenceInfo(ctx)
	if err != nil {
		return err
	}
	for _, field := range []string{"loading", "rdb_bgsave_in_progress", "aof_rewrite_in_progress"} {
		if info[field] != "0" {
			return fmt.Errorf("Redis persistence field %s is %q, want 0", field, info[field])
		}
	}
	if value := info["aof_last_write_status"]; value != "" && value != "ok" {
		return fmt.Errorf("Redis AOF write status is %q", value)
	}
	return nil
}

func (engine *redisEngine) persistenceInfo(ctx context.Context) (map[string]string, error) {
	raw, err := engine.client.Info(ctx, "persistence").Result()
	if err != nil {
		return nil, err
	}
	values := make(map[string]string)
	for line := range strings.SplitSeq(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(line, ":")
		if found {
			values[name] = value
		}
	}
	return values, nil
}
