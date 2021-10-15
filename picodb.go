package picodb

import (
	"bytes"
	"encoding/gob"
	"os"
	"path"

	"github.com/gofrs/flock"
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
	lf := p.lockFile(key)
	lock := flock.New(lf)
	err := lock.Lock()
	if err != nil {
		return err
	}
	defer lock.Unlock()
	defer os.Remove(lf)
	return p.writeInternal(key, data)
}

func (p *PicoDb) writeInternal(key string, data interface{}) error {
	df := p.dataFile(key)
	f, err := os.Create(df)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)
	err = e.Encode(data)
	if err != nil {
		return err
	}
	f.Write(buf.Bytes())
	return nil
}

func (p *PicoDb) dataFile(key string) string {
	return path.Join(p.opt.RootPath, key)
}

func (p *PicoDb) lockFile(key string) string {
	return p.dataFile(key) + ".lock"
}
