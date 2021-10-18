package picodb

import (
	"os"

	"github.com/gofrs/flock"
)

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
