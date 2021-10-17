package picodb

import (
	"github.com/gofrs/flock"
)

// StoreWithLock stores a key with the supplied bytes as value.
// This function is safe to use in concurrent scenarios, including
// multiple processes accessing the same store.
func (p *PicoDb) StoreWithLock(key string, val []byte) error {
	lock := flock.New(p.path(key))
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()
	return p.Store(key, val)
}

// LoadWithLock loads data for the given key.
// If the key is missing, an error is returned.
// This function is safe to use in concurrent scenarios, including
// multiple processes accessing the same store.
func (p *PicoDb) LoadWithLock(key string) ([]byte, error) {
	lock := flock.New(p.path(key))
	if err := lock.RLock(); err != nil {
		return nil, err
	}
	defer lock.Unlock()
	return p.Load(key)
}
