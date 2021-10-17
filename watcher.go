package picodb

import (
	"time"

	"github.com/google/uuid"
)

const WatcherKey = ".picodb-watcher"

type WatcherData map[uuid.UUID][]string

func (p *PicoDb) enableWatcherInternal(freq time.Duration) (chan error, error) {
	p.ticker = time.NewTicker(freq)
	err := p.subscribe()
	if err != nil {
		return nil, err
	}

	ch := make(chan error, 3)
	go func() {
		for range p.ticker.C {
			err := p.sync()
			if err != nil {
				ch <- err
			}
		}
	}()
	p.watcherEnabled = true
	return ch, nil
}

func (p *PicoDb) disableWatcherInternal() {
	if p.ticker != nil {
		p.ticker.Stop()
	}
	p.watcherEnabled = false
}

func (p *PicoDb) sync() error {
	var d WatcherData
	return p.mutateInternal(WatcherKey, &d, func(i interface{}) {
		wd := *i.(*WatcherData)
		for _, key := range wd[p.id] {
			p.cache.Delete(key)
		}
		wd[p.id] = wd[p.id][:0] // clear events slice
	})
}

func (p *PicoDb) broadcast(key string) error {
	var d WatcherData
	return p.mutateInternal(WatcherKey, &d, func(i interface{}) {
		wd := *i.(*WatcherData)
		for id := range wd {
			if id != p.id {
				wd[id] = append(wd[id], key)
			}
		}
	})
}

func (p *PicoDb) subscribe() error {
	exists, err := p.existsInternal(WatcherKey)
	if err != nil {
		return err
	}
	var d WatcherData
	if !exists {
		d = make(WatcherData)
		d[p.id] = make([]string, 0)
		return p.writeInternal(WatcherKey, d)
	}
	return p.mutateInternal(WatcherKey, &d, func(i interface{}) {
		wd := *i.(*WatcherData)
		if _, ok := wd[p.id]; !ok {
			wd[p.id] = make([]string, 0)
		}
	})
}
