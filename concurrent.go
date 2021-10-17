package picodb

import (
	"os"

	"github.com/gofrs/flock"
)

// StoreWithLock stores a key with the supplied bytes as value.
// This function is safe to use in concurrent scenarios, including
// multiple processes accessing the same store.
func (p *PicoDb) StoreWithLock(key string, val []byte) error {
	if err := p.checkWritable(key); err != nil {
		return err
	}
	name := p.path(key)
	lock := flock.New(p.path(key))
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()
	return os.WriteFile(name, val, p.opt.FileMode)
}

// LoadWithLock loads data for the given key.
// If the key is missing, an error is returned.
// This function is safe to use in concurrent scenarios, including
// multiple processes accessing the same store.
func (p *PicoDb) LoadWithLock(key string) ([]byte, error) {
	if err := p.checkReadable(key); err != nil {
		return nil, err
	}
	name := p.path(key)
	lock := flock.New(p.path(key))
	if err := lock.RLock(); err != nil {
		return nil, err
	}
	defer lock.Unlock()
	return os.ReadFile(name)
}
