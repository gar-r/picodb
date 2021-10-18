package picodb

import (
	"os"
	"sync"

	"github.com/google/uuid"
)

// PicoDb is a simplistic directory based key-value storage.
// Keys are always of type string, and PicoDb will store the data
// associated with the given key under a file with the same name.
// PicoDb is always initialized with a root path, which will
// contain the data.
type PicoDb struct {
	opt   *PicoDbOptions
	cache *sync.Map
	id    uuid.UUID
}

// New returns a new PicoDb instance.
func New(options *PicoDbOptions) *PicoDb {
	return &PicoDb{
		opt:   options,
		cache: &sync.Map{},
		id:    uuid.New(),
	}
}

// Store a key.
func (p *PicoDb) Store(key string, val []byte) error {
	if err := p.ensureStorable(key); err != nil {
		return err
	}
	path := p.path(key)
	if err := os.WriteFile(path, val, p.opt.FileMode); err != nil {
		return err
	}
	if p.opt.Caching {
		p.cache.Store(key, val)
	}
	return nil
}

// Store a key with a string value.
func (p *PicoDb) StoreString(key, val string) error {
	return p.Store(key, []byte(val))
}

// Load a key.
// If the key is missing, an error is returned.
func (p *PicoDb) Load(key string) ([]byte, error) {
	if err := p.ensureLoadable(key); err != nil {
		return nil, err
	}
	if p.opt.Caching {
		val, ok := p.cache.Load(key)
		if ok {
			return val.([]byte), nil
		}
	}
	path := p.path(key)
	return os.ReadFile(path)
}

// Load a key with a string value.
// If the key is missing, and error is returned.
func (p *PicoDb) LoadString(key string) (string, error) {
	b, err := p.Load(key)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Delete a key.
// If the key is missing, an error is returned.
func (p *PicoDb) Delete(key string) error {
	if err := p.ensureLoadable(key); err != nil {
		return err
	}
	path := p.path(key)
	return os.Remove(path)
}
