package picodb

import (
	"sync"

	"github.com/google/uuid"
)

// PicoDb is a simplistic directory based key-value storage.
// Keys are always of type string, and PicoDb will store the data
// associated with the given key under a file with the same name.
// PicoDb is always initialized with a root path, which will
// contain the data.
type PicoDb struct {
	opt   *PicoDbOptions
	cache *sync.Map
	id    uuid.UUID
}

// PicoDbOptions contains options which are passed on to the
// New function to create a PicoDb instace.
type PicoDbOptions struct {
	RootDir     string // root directory
	Compression bool   // enable compression at rest
	Caching     bool   // enable in-memory cache
}

// Defaults returns a PicoDbOptions with reasonable defaults.
func Defaults() *PicoDbOptions {
	return &PicoDbOptions{
		RootDir:     "picodb",
		Compression: false,
		Caching:     false,
	}
}

// New returns a new PicoDb instance.
func New(options *PicoDbOptions) *PicoDb {
	return &PicoDb{
		opt:   options,
		cache: &sync.Map{},
		id:    uuid.New(),
	}
}
