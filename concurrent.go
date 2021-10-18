package picodb

import (
	"os"

	"github.com/gofrs/flock"
)

// StoreWithLock stores a key with the supplied bytes as value.
// This function is safe to use in concurrent scenarios, including
// multiple processes accessing the same store.
func (p *PicoDb) StoreWithLock(key string, val []byte) error {
	if err := p.ensureStorable(key); err != nil {
		return err
	}
	name := p.path(key)
	lock := flock.New(name)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()
	return os.WriteFile(name, val, p.opt.FileMode)
}

// StoreStringWithLock stores a string value with the given key.
// This function is safe to use in concurrent scenarios, including
// multiple processes accessing the same store.
func (p *PicoDb) StoreStringWithLock(key, val string) error {
	return p.StoreWithLock(key, []byte(val))
}

// LoadWithLock loads data for the given key.
// If the key is missing, an error is returned.
// This function is safe to use in concurrent scenarios, including
// multiple processes accessing the same store.
func (p *PicoDb) LoadWithLock(key string) ([]byte, error) {
	if err := p.ensureLoadable(key); err != nil {
		return nil, err
	}
	name := p.path(key)
	lock := flock.New(name)
	if err := lock.RLock(); err != nil {
		return nil, err
	}
	defer lock.Unlock()
	return os.ReadFile(name)
}

// LoadStringWithLock loads a string value for the given key.
// If the key is missing, an error is returned.
// This function is safe to use in concurrent scenarios, including
// multiple processes accessing the same store.
func (p *PicoDb) LoadStringWithLock(key string) (string, error) {
	b, err := p.LoadWithLock(key)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Mutate loads, alters and stores the value of a key in one step.
// It uses file locks to ensure atomicity.
// If the given key does exist it returns an error.
// This function is safe to use in a concurrent environment.
func (p *PicoDb) Mutate(key string, fn func([]byte) []byte) error {
	if fn == nil {
		return nil
	}
	if err := p.ensureLoadable(key); err != nil {
		return err
	}
	name := p.path(key)
	lock := flock.New(name)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()
	b, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	return os.WriteFile(name, fn(b), p.opt.FileMode)
}

// MutateString loads, alters and stores the string value of a key in one step.
// It uses file locks to ensure atomicity.
// If the given key does exist it returns an error.
// This function is safe to use in a concurrent environment.
func (p *PicoDb) MutateString(key string, fn func(string) string) error {
	return p.Mutate(key, func(b []byte) []byte {
		return []byte(fn(string(b)))
	})
}
