package picodb

import (
	"os"
	"path"
)

// PicoDb is a simplistic directory based key-value storage.
// Keys are always of type string, and PicoDb will store the data
// associated with the given key under a file with the same name.
// PicoDb is always initialized with a root path, which will
// contain the data.
type PicoDb struct {
	opt *PicoDbOptions
}

// PicoDbOptions contains options which are passed on to the
// New function to create a PicoDb instace.
type PicoDbOptions struct {
	RootPath    string      // root directory
	FileMode    os.FileMode // create chmod for the root path, defaults to 0700
	Compression bool        // enable compression at rest
	Caching     bool        // enable in-memory cache
	FileWatcher bool        // enable file watcher
}

func New(options *PicoDbOptions) (*PicoDb, error) {
	if options.FileMode == os.FileMode(0) {
		options.FileMode = 0700
	}
	err := os.MkdirAll(options.RootPath, options.FileMode)
	if err != nil {
		return nil, err
	}
	return &PicoDb{
		opt: options,
	}, nil
}

func (p *PicoDb) Write(key string, data interface{}) error {
	path := p.dataFile(key)
	return p.writeWithLock(path, data)
}

func (p *PicoDb) Read(key string, data interface{}) error {
	path := p.dataFile(key)
	return p.readWithLock(path, data)
}

func (p *PicoDb) dataFile(key string) string {
	return path.Join(p.opt.RootPath, key)
}
