package picodb

import (
	"errors"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// PicoDb is a simplistic directory based key-value storage.
// Keys are always of type string, and PicoDb will store the data
// associated with the given key under a file with the same name.
// PicoDb is always initialized with a root path, which will
// contain the data.
type PicoDb struct {
	opt            *PicoDbOptions
	cache          *sync.Map
	id             uuid.UUID
	ticker         *time.Ticker
	watcherEnabled bool
}

// PicoDbOptions contains options which are passed on to the
// New function to create a PicoDb instace.
type PicoDbOptions struct {
	RootPath    string      // root directory
	FileMode    os.FileMode // create chmod for the root path, defaults to 0700
	Compression bool        // enable compression at rest
	Caching     bool        // enable in-memory cache
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
		id:    uuid.New(),
	}, nil
}

func (p *PicoDb) Write(key string, data interface{}) error {
	if p.isKeyReserved(key) {
		return errors.New(ErrReservedKey)
	}
	if p.opt.Caching {
		return p.writeWithCache(key, data)
	}
	return p.writeInternal(key, data)
}

func (p *PicoDb) Read(key string, data interface{}) error {
	ok, err := p.Exists(key)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New(ErrNoExist)
	}
	if p.opt.Caching {
		return p.readWithCache(key, data)
	}
	return p.readInternal(key, data)
}

type Mutator func(interface{})

func (p *PicoDb) Mutate(key string, data interface{}, fn Mutator) error {
	if p.isKeyReserved(key) {
		return errors.New(ErrReservedKey)
	}
	ok, err := p.Exists(key)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New(ErrNoExist)
	}
	if p.opt.Caching {
		return p.mutateWithCache(key, data, fn)
	}
	return p.mutateInternal(key, data, fn)
}

func (p *PicoDb) Exists(key string) (bool, error) {
	if p.opt.Caching {
		return p.existsWithCache(key)
	}
	return p.existsInternal(key)
}

func (p *PicoDb) Delete(key string) error {
	if p.isKeyReserved(key) {
		return errors.New(ErrReservedKey)
	}
	if p.opt.Caching {
		return p.deleteWithCache(key)
	}
	return p.deleteInternal(key)
}

func (p *PicoDb) EnableWatcher(freq time.Duration) (chan error, error) {
	if !p.opt.Caching {
		return nil, errors.New(ErrCacheDisabled)
	}
	return p.enableWatcherInternal(freq)
}

func (p *PicoDb) DisableWatcher() {
	p.disableWatcherInternal()
}
