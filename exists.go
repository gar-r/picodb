package picodb

import "os"

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
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return !fi.IsDir(), nil
}
