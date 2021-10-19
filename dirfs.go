package picodb

import (
	"os"
	"path"
	"strings"
)

// dirfs uses a single directory with a flat structure to map names to files.
type dirfs struct {
	root    string  // the root directory which hosts the files
	locking bool    // use locking for file access
	s       storage // underlying storage
}

// store a key-value pair.
// The key is used to create a file with the given data
// under the root directory.
// A KeyInvalid error is returned if the given key
// cannot be used as a file name.
func (d *dirfs) store(key string, val []byte) error {
	if err := d.check(key); err != nil {
		return err
	}
	if err := d.mkroot(); err != nil {
		return err
	}
	path := d.path(key)
	if d.locking {
		lock := d.s.getl(path)
		if err := lock.Lock(); err != nil {
			return err
		}
		defer lock.Unlock()
	}
	return d.s.write(path, val)
}

// load the value associated with the given key.
// Data is loaded from a file with the name of the given key.
// A KeyNotFound error is returned if the key does not exist.
// A KeyInvalid error is returned if the given key
// cannot be used as a file name.
func (d *dirfs) load(key string) ([]byte, error) {
	if err := d.check(key); err != nil {
		return nil, err
	}
	path := d.path(key)
	b, err := d.s.read(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, NewKeyNotFound(key)
		}
		return nil, err
	}
	return b, nil
}

// delete a key and the associated value.
// A KeyInvalid error is returned if the given key
// If the key does not exist, nothing is deleted and
// no error is returned.
func (d *dirfs) delete(key string) error {
	if err := d.check(key); err != nil {
		return err
	}
	path := d.path(key)
	err := d.s.remove(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

// check if the name is valid.
// returns an error if invalid, or nil
func (d *dirfs) check(name string) error {
	if strings.ContainsRune(name, os.PathSeparator) {
		return NewKeyInvalid(name)
	}
	return nil
}

// mkroot creates the dirfs root directory.
func (d *dirfs) mkroot() error {
	return d.s.mkdir(d.root)
}

// path returns the file path associated with the given name.
func (d *dirfs) path(name string) string {
	return path.Join(d.root, name)
}
