package kvbench

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisEngine struct {
	client  *redis.Client
	mode    DurabilityMode
	clients int
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
	engine := &redisEngine{client: client, mode: mode, clients: clients}
	if err := engine.configureDurability(ctx); err != nil {
		return nil, errors.Join(err, client.Close())
	}
	if err := engine.warmConnections(ctx, clients); err != nil {
		return nil, errors.Join(err, client.Close())
	}
	if err := engine.validateDurability(ctx); err != nil {
		return nil, errors.Join(err, client.Close())
	}
	if err := engine.reset(ctx); err != nil {
		return nil, errors.Join(err, client.Close())
	}
	if err := engine.validateDurability(ctx); err != nil {
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
	deadline := time.Now().Add(time.Second)
	for {
		if err := engine.validatePool(); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			stats := engine.client.PoolStats()
			return fmt.Errorf("Redis pool has %d total and %d idle connections, want at least %d", stats.TotalConns, stats.IdleConns, clients)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
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

func (engine *redisEngine) GetBatch(ctx context.Context, keys [][]byte) ([][]byte, error) {
	arguments := make([]string, len(keys))
	for index, key := range keys {
		arguments[index] = string(key)
	}
	results, err := engine.client.MGet(ctx, arguments...).Result()
	if err != nil {
		return nil, err
	}
	values := make([][]byte, len(results))
	for index, result := range results {
		if result == nil {
			return nil, ErrKeyNotFound
		}
		switch value := result.(type) {
		case string:
			values[index] = []byte(value)
		case []byte:
			values[index] = value
		default:
			return nil, fmt.Errorf("Redis MGET returned %T", result)
		}
	}
	return values, nil
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

func (engine *redisEngine) MixedBatch(ctx context.Context, keys [][]byte, pairs []Pair) ([][]byte, error) {
	commands, err := engine.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			pipe.Get(ctx, string(key))
		}
		for _, pair := range pairs {
			pipe.Set(ctx, string(pair.Key), pair.Value, 0)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	values := make([][]byte, len(keys))
	for index, command := range commands[:len(keys)] {
		if errors.Is(command.Err(), redis.Nil) {
			return nil, ErrKeyNotFound
		}
		if command.Err() != nil {
			return nil, command.Err()
		}
		stringCommand, ok := command.(*redis.StringCmd)
		if !ok {
			return nil, fmt.Errorf("Redis transaction read returned %T", command)
		}
		value, err := stringCommand.Bytes()
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

func (engine *redisEngine) ScanPrefix(ctx context.Context, prefix []byte, visit func([]byte, []byte) error) error {
	pattern := redisPrefixPattern(prefix)
	seen := make(map[string]struct{})
	var cursor uint64
	for {
		keys, next, err := engine.client.Scan(ctx, cursor, pattern, 1_000).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			values, err := engine.client.MGet(ctx, keys...).Result()
			if err != nil {
				return err
			}
			for index, value := range values {
				if _, duplicate := seen[keys[index]]; duplicate {
					continue
				}
				seen[keys[index]] = struct{}{}
				if value == nil {
					continue
				}
				bytes, ok := value.(string)
				if !ok {
					return fmt.Errorf("Redis MGET returned %T", value)
				}
				if err := visit([]byte(keys[index]), []byte(bytes)); err != nil {
					return err
				}
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func redisPrefixPattern(prefix []byte) string {
	var pattern strings.Builder
	for _, character := range prefix {
		if strings.ContainsRune("\\*?[]", rune(character)) {
			pattern.WriteByte('\\')
		}
		pattern.WriteByte(character)
	}
	pattern.WriteByte('*')
	return pattern.String()
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

func (engine *redisEngine) Validate(ctx context.Context) error {
	if err := engine.validateDurability(ctx); err != nil {
		return err
	}
	return engine.validatePool()
}

func (engine *redisEngine) StorageStats(ctx context.Context) (storageStats, error) {
	persistence, err := engine.persistenceInfo(ctx)
	if err != nil {
		return storageStats{}, err
	}
	logBytes, err := parseRedisMetric(persistence, "aof_current_size")
	if err != nil {
		return storageStats{}, err
	}
	memory, err := engine.infoValues(ctx, "memory")
	if err != nil {
		return storageStats{}, err
	}
	memoryBytes, err := parseRedisMetric(memory, "used_memory_dataset")
	if err != nil {
		return storageStats{}, err
	}
	return storageStats{logBytes: logBytes, memoryBytes: memoryBytes}, nil
}

func (engine *redisEngine) PrepareCollections(_ context.Context, _ [][][]byte) error {
	return nil
}

func (engine *redisEngine) PutCollectionBatch(ctx context.Context, path [][]byte, pairs []Pair) error {
	prefix := redisCollectionPrefix(path)
	_, err := engine.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, pair := range pairs {
			key := make([]byte, 0, len(prefix)+len(pair.Key))
			key = append(key, prefix...)
			key = append(key, pair.Key...)
			pipe.Set(ctx, string(key), pair.Value, 0)
		}
		return nil
	})
	return err
}

func (engine *redisEngine) GetCollection(ctx context.Context, path [][]byte, key []byte) ([]byte, error) {
	prefix := redisCollectionPrefix(path)
	fullKey := make([]byte, 0, len(prefix)+len(key))
	fullKey = append(fullKey, prefix...)
	fullKey = append(fullKey, key...)
	value, err := engine.client.Get(ctx, string(fullKey)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrKeyNotFound
	}
	return value, err
}

func redisCollectionPrefix(path [][]byte) []byte {
	prefix := []byte("collection/")
	for _, name := range path {
		prefix = append(prefix, name...)
		prefix = append(prefix, '/')
	}
	return prefix
}

func (engine *redisEngine) Close() error { return engine.client.Close() }

func (engine *redisEngine) validatePool() error {
	stats := engine.client.PoolStats()
	if stats.TotalConns < uint32(engine.clients) || stats.IdleConns < uint32(engine.clients) {
		return fmt.Errorf("Redis pool has %d total and %d idle connections, want at least %d", stats.TotalConns, stats.IdleConns, engine.clients)
	}
	return nil
}

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

func (engine *redisEngine) configureDurability(ctx context.Context) error {
	wantFsync, err := redisAppendFsync(engine.mode)
	if err != nil {
		return err
	}
	return engine.client.ConfigSet(ctx, "appendfsync", wantFsync).Err()
}

func (engine *redisEngine) setDurability(ctx context.Context, mode DurabilityMode) error {
	engine.mode = mode
	if err := engine.configureDurability(ctx); err != nil {
		return err
	}
	return engine.validateDurability(ctx)
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
	if value, found := info["aof_pending_bio_fsync"]; found && value != "0" {
		return fmt.Errorf("Redis persistence field aof_pending_bio_fsync is %q, want 0", value)
	}
	if value := info["aof_last_write_status"]; value != "" && value != "ok" {
		return fmt.Errorf("Redis AOF write status is %q", value)
	}
	return nil
}

func (engine *redisEngine) persistenceInfo(ctx context.Context) (map[string]string, error) {
	return engine.infoValues(ctx, "persistence")
}

func (engine *redisEngine) infoValues(ctx context.Context, section string) (map[string]string, error) {
	raw, err := engine.client.Info(ctx, section).Result()
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

func parseRedisMetric(values map[string]string, name string) (int64, error) {
	value, found := values[name]
	if !found {
		return 0, fmt.Errorf("Redis INFO returned no %s", name)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse Redis INFO %s: %w", name, err)
	}
	return parsed, nil
}
