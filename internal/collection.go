package internal

type CollectionOptions struct {
	FsyncPolicy FsyncPolicy // OnEveryWrite, Periodic, GroupCommit

	// For background fsync
	FsyncIntervalMs int // default: 10ms

	// For group commit
	BatchSize      int // default: 100 writes
	BatchTimeoutMs int // default: 10ms

	// Checkpointing
	CheckpointEveryWrites  int // default: 10000
	CheckpointEverySeconds int // default: 300 (5 minutes)
}

type Collection struct {
	name  string
	store map[string]string
}

func (c *Collection) Get(key string) (string, error) {
	if len(key) == 0 {
		return "", ErrInvalidKey
	}

	val, found := c.store[key]
	if !found {
		return "", ErrKeyNotFound
	}

	return val, nil
}

func (c *Collection) Set(key, val string) (string, error) {
	if len(key) == 0 {
		return "", ErrInvalidKey
	}

	c.store[key] = val

	return val, nil
}

func (c *Collection) Delete(key string) (string, error) {
	if len(key) == 0 {
		return "", ErrInvalidKey
	}

	_, found := c.store[key]
	if !found {
		return "", ErrKeyNotFound
	}

	delete(c.store, key)

	return "", nil
}
