package picodb

import (
	"compress/gzip"
	"encoding/gob"
	"io"
	"os"

	"github.com/gofrs/flock"
)

// writeWithCache stores the given key-data pair into the cache,
// and then stores them using the file system
func (p *PicoDb) writeWithCache(key string, data interface{}) error {
	p.cache.Store(key, data)
	if p.watcherEnabled {
		p.broadcast(key)
	}
	return p.writeInternal(key, data)
}

// writeInternal writes the given key-data pair.
// The function uses flock to obtain a write lock,
// and is safe to use in scenarios with multiple
// processes writing the same file.
func (p *PicoDb) writeInternal(key string, data interface{}) error {
	path := p.dataFile(key)
	lock := flock.New(path)
	err := lock.Lock()
	if err != nil {
		return err
	}
	defer lock.Unlock()
	return p.writeData(path, data)
}

// writeData encodes and dumps the data into the given file
// without any concern for concurrent access or inter-
// process locking
func (p *PicoDb) writeData(path string, data interface{}) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Sync()
	defer f.Close()
	return p.encode(f, data)
}

// encode encodes the data using the given writer.
// The function also applies compression if enabled.
func (p *PicoDb) encode(w io.Writer, data interface{}) error {
	var writer io.Writer
	if p.opt.Compression {
		gz := gzip.NewWriter(w)
		defer gz.Close()
		writer = gz
	} else {
		writer = w
	}
	e := gob.NewEncoder(writer)
	return e.Encode(data)
}
