package internal

type Collection struct {
	name   string
	store  map[string]string
	config Config
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
