package picodb

import "errors"

// fallback is special kvs which can handle fallback between
// multiple kvs' in case of missing keys
type fallback struct {
	list []kvs
}

// store adds the key-value pair to every underlying kvs
// If any store operation fails, the operation fails
// and the error is immediately returned.
func (f *fallback) store(key string, val []byte) error {
	for _, s := range f.list {
		if err := s.store(key, val); err != nil {
			return err
		}
	}
	return nil
}

// load iterates the underlying kvs and returns the given
// key from the first one that contains it
// If any of the kvs returns an error during the iteration,
// the operation fails and an error is immediately returned.
// If the key is not present in any of them, a
// KeyNotFound error is returned.
func (f *fallback) load(key string) ([]byte, error) {
	notfound := NewKeyNotFound(key)
	for _, s := range f.list {
		val, err := s.load(key)
		if err != nil {
			if errors.Is(err, notfound) {
				continue
			}
			return nil, err
		}
		return val, nil
	}
	return nil, notfound
}

// delete removes the given key
func (f *fallback) delete(key string) error {

}
