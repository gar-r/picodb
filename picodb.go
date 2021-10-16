package picodb

import (
	"errors"
	"os"
	"sync"
)

// PicoDb is a simplistic directory based key-value storage.
// Keys are always of type string, and PicoDb will store the data
// associated with the given key under a file with the same name.
// PicoDb is always initialized with a root path, which will
// contain the data.
type PicoDb struct {
	opt   *PicoDbOptions
	cache *sync.Map
}

// PicoDbOptions contains options which are passed on to the
// New function to create a PicoDb instace.
type PicoDbOptions struct {
	RootPath    string      // root directory
	FileMode    os.FileMode // create chmod for the root path, defaults to 0700
	Compression bool        // enable compression at rest
	Caching     bool        // enable in-memory cache
	FileWatcher bool        // enable file watcher (ignored unless cache is enabled)
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
		opt:   options,
		cache: &sync.Map{},
	}, nil
}

func (p *PicoDb) Write(key string, data interface{}) error {
	if p.opt.Caching {
		return p.writeWithCache(key, data)
	}
	return p.writeInternal(key, data)
}

func (p *PicoDb) Read(key string, data interface{}) error {
	ok, err := p.Exists(key)
	if !ok {
		return errors.New(ErrNoExist)
	}
	if err != nil {
		return err
	}
	if p.opt.Caching {
		return p.readWithCache(key, data)
	}
	return p.readInternal(key, data)
}

func (p *PicoDb) Exists(key string) (bool, error) {
	if p.opt.Caching {
		return p.existsWithCache(key)
	}
	return p.existsInternal(key)
}

const ErrNoExist = "key does not exist"

func IsNoExist(err error) bool {
	return err.Error() == ErrNoExist
}
