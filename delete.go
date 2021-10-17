package picodb

import "os"

// deleteWithCache removes a key.
// The key will be removed from both the cache
// and the file system.
func (p *PicoDb) deleteWithCache(key string) error {
	p.cache.Delete(key)
	if p.watcherEnabled {
		p.broadcast(key)
	}
	return p.deleteInternal(key)
}

// deleteInternal removes a key from the file system
func (p *PicoDb) deleteInternal(key string) error {
	path := p.dataFile(key)
	return os.Remove(path)
}
