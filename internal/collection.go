package internal

type Collection struct {
	Metadata CollectionMetadata
	store    map[string]string
}

// CollectionMetadata is at the start of each collection's data
type CollectionMetadata struct {
	NameLength uint32
	Name       []byte
	EntryCount uint32 // Number of key-value pairs
	Config     Config // Configuration data for this collection
}

func NewCollection(name string, config Config) *Collection {
	return &Collection{
		Metadata: CollectionMetadata{
			NameLength: uint32(len(name)),
			Name:       []byte(name),
			EntryCount: 0,
			Config:     config,
		},
		store: make(map[string]string),
	}
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
	c.Metadata.EntryCount++

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

func (c *Collection) GetStore() map[string]string {
	return c.store
}
