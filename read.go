package picodb

import (
	"compress/gzip"
	"encoding/gob"
	"io"
	"os"
	"reflect"

	"github.com/gofrs/flock"
)

// readWithCache reads a key-data pair searching in cache first,
// and the file system second.
// If the data is found on the file system, but not in the cache,
// its value is also stored in the after reading it.
// This function is not guarded against non-existing keys,
// and will store a nil pointer if called with such.
func (p *PicoDb) readWithCache(key string, data interface{}) error {
	d, ok := p.cache.Load(key)
	if ok {
		vout := reflect.ValueOf(data)
		vdat := reflect.ValueOf(d)
		reflect.Indirect(vout).Set(vdat)
		return nil
	}
	err := p.readInternal(key, data)
	p.cache.Store(key, data)
	return err
}

// readInternal reads the data for the given key.
// The function uses flock to obtain a read lock,
// and is safe to use in scenarios with multiple
// processes writing the same file.
// If there are no errors, the result is returned
// in the data out parameter.
func (p *PicoDb) readInternal(key string, data interface{}) error {
	path := p.dataFile(key)
	lock := flock.New(path)
	err := lock.RLock()
	if err != nil {
		return err
	}
	defer lock.Unlock()
	return p.readData(path, data)
}

// readData decodes data from the given file
// without any concern for concurrent access or inter-
// process locking
// data is an out parameter containing the decoded result
// if there was no error
func (p *PicoDb) readData(path string, data interface{}) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return p.decode(f, data)
}

// decode decodes the data using the given reader.
// The function also applies decompression if enabled.
func (p *PicoDb) decode(r io.Reader, data interface{}) error {
	var reader io.Reader
	if p.opt.Compression {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		defer gz.Close()
		reader = gz
	} else {
		reader = r
	}
	e := gob.NewDecoder(reader)
	return e.Decode(data)
}
