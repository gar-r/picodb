package picodb

import (
	"os"
	"path"
	"strings"
)

// ensure the given key is valid for writing
func (p *PicoDb) ensureStorable(key string) error {
	if err := p.mkroot(); err != nil {
		return err
	}
	if !p.legal(key) {
		return NewInvalidName(key)
	}
	return nil
}

// ensure the given key is valid for reading
func (p *PicoDb) ensureLoadable(key string) error {
	if !p.legal(key) {
		return NewInvalidName(key)
	}
	return p.stat(key)
}

// stats the path associated with the key
func (p *PicoDb) stat(key string) error {
	path := p.path(key)
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewKeyNotFound(key)
		}
		return err
	}
	return nil
}

// check if the given key only contains legal characters
func (p *PicoDb) legal(key string) bool {
	return !strings.ContainsRune(key, os.PathSeparator)
}

// create the root dir if it does not exist
func (p *PicoDb) mkroot() error {
	return os.MkdirAll(p.opt.RootDir, p.opt.DirMode)
}

// return the data path associated with the given key
func (p *PicoDb) path(key string) string {
	return path.Join(p.opt.RootDir, key)
}
