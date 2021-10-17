package picodb

import (
	"github.com/gofrs/flock"
)

func (p *PicoDb) mutateWithCache(key string, data interface{}, fn Mutator) error {
	p.cache.Delete(key)
	return p.mutateInternal(key, data, func(data interface{}) {
		fn(data)
		p.cache.Store(key, data)
	})
}

func (p *PicoDb) mutateInternal(key string, data interface{}, fn Mutator) error {
	path := p.dataFile(key)
	lock := flock.New(path)
	err := lock.Lock()
	if err != nil {
		return err
	}
	defer lock.Unlock()

	err = p.readData(path, data)
	if err != nil {
		return err
	}
	fn(data)
	return p.writeData(path, data)
}
