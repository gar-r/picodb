package picodb

import "sync"

// cache is a thread safe in-memory key-value store
type cache struct {
	m *sync.Map
}

func (c *cache) store(key string, val []byte) error {
	c.m.Store(key, val)
	return nil
}

func (c *cache) load(key string) ([]byte, error) {
	val, ok := c.m.Load(key)
	if !ok {
		return nil, NewKeyNotFound(key)
	}
	return val.([]byte), nil
}

func (c *cache) delete(key string) error {
	c.m.Delete(key)
	return nil
}
