package picodb

import (
	"github.com/gofrs/flock"
	"github.com/google/uuid"
)

const WatcherKey = ".watcher"

type WatcherData map[uuid.UUID][]string

func (p *PicoDb) sync() error {
	lock := flock.New(p.dataFile(WatcherKey))
	err := lock.Lock()
	if err != nil {
		return err
	}
	defer lock.Unlock()
	wd, err := p.readWatcherData()
	if err != nil {
		return err
	}
	for _, key := range wd[p.id] {
		p.cache.Delete(key)
	}
	return nil
}

func (p *PicoDb) broadcast(key string) error {
	lock := flock.New(p.dataFile(WatcherKey))
	err := lock.Lock()
	if err != nil {
		return err
	}
	defer lock.Unlock()
	wd, err := p.readWatcherData()
	if err != nil {
		return err
	}
	for id, keys := range wd {
		if id != p.id {
			wd[id] = append(keys, key)
		}
	}
	return p.writeWatcherData(wd)
}

func (p *PicoDb) subscribe() error {
	lock := flock.New(p.dataFile(WatcherKey))
	err := lock.Lock()
	if err != nil {
		return err
	}
	defer lock.Unlock()
	wd, err := p.readWatcherData()
	if err != nil {
		return err
	}
	if _, ok := wd[p.id]; !ok {
		wd[p.id] = make([]string, 0)
	}
	return p.writeWatcherData(wd)
}

func (p *PicoDb) readWatcherData() (WatcherData, error) {
	var wd WatcherData
	ok, err := p.existsInternal(WatcherKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		wd = make(WatcherData)
	}
	p.readInternal(WatcherKey, &wd)
	return wd, nil
}

func (p *PicoDb) writeWatcherData(wd WatcherData) error {
	return p.writeInternal(WatcherKey, wd)
}
