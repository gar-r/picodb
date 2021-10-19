package picodb

import (
	"os"

	"github.com/gofrs/flock"
)

// fs is a basic storage implementation using the file system.
type fs struct {
	fmode os.FileMode // file mode used to create new files
	dmode os.FileMode // file mode used to create new directories
}

// write bytes to a file indicated by name.
func (f *fs) write(name string, val []byte) error {
	return os.WriteFile(name, val, f.fmode)
}

// read bytes from a file indicated by name.
func (f *fs) read(name string) ([]byte, error) {
	return os.ReadFile(name)
}

// remove deletes the file by the given name
func (f *fs) remove(name string) error {
	return os.Remove(name)
}

// mkdir creates the directory with the given name
func (f *fs) mkdir(name string) error {
	return os.MkdirAll(name, f.dmode)
}

// getl creates and returns a file-lock for the given name
func (f *fs) getl(name string) lock {
	return flock.New(name)
}
