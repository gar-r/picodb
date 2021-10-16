package picodb

import (
	"os"
	"path"
)

// existsWithCache checks if a given key exists.
// The key is first checked in the cache, and if it
// exists there, the file system is not checked.
func (p *PicoDb) existsWithCache(key string) (bool, error) {
	_, ok := p.cache.Load(key)
	if ok {
		return true, nil
	}
	return p.existsInternal(key)
}

// existsInternal checks if a key exists
// An error is returned if the check itself fails.
func (p *PicoDb) existsInternal(key string) (bool, error) {
	path := p.dataFile(key)
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// dataFile returns the full path of the data file
// that should be associated with the given key.
// (the file itself may or may not exist)
func (p *PicoDb) dataFile(key string) string {
	return path.Join(p.opt.RootPath, key)
}
