package picodb

import (
	"path"
)

// dataFile returns the full path of the data file
// that should be associated with the given key.
// (the file itself may or may not exist)
func (p *PicoDb) dataFile(key string) string {
	return path.Join(p.opt.RootPath, key)
}

// isKeyReserved checks if the given key is one that
// is reserved by picodb for internal use
func (p *PicoDb) isKeyReserved(key string) bool {
	return key == WatcherKey
}
