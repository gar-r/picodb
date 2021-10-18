package picodb

import (
	"os"
	"path"
	"strings"
)

// storage represents a generic interface which can read and write bytes based on a name.
type storage interface {
	write(string, []byte) error  // write bytes to a given name
	read(string) ([]byte, error) // read bytes from a given name
	mkdir(string) error          // make directory with the given name
}

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

// mkdir creates the directory with the given name
func (f *fs) mkdir(name string) error {
	return os.MkdirAll(name, f.dmode)
}

// dirfs uses a single directory with a flat structure to map names to files.
type dirfs struct {
	root string  // the root directory which hosts the files
	s    storage // underlying storage
}

// write the bytes to a file indicated by name.
// A new file is created under the root directory with the given name,
// and the bytes are written to it.
// The name should be useable as a filename, and can not contain
// path separator characters.
func (d *dirfs) write(name string, val []byte) error {
	if err := d.check(name); err != nil {
		return err
	}
	if err := d.mkroot(); err != nil {
		return err
	}
	path := d.path(name)
	return d.s.write(path, val)
}

// read the bytes from the file indicated by name.
// The name is mapped to a single file under the root directory,
// and should not contain path separator characters.
func (d *dirfs) read(name string) ([]byte, error) {
	if err := d.check(name); err != nil {
		return nil, err
	}
	path := d.path(name)
	return d.s.read(path)
}

// mkdir with given name: just a proxy into underlying storage
func (d *dirfs) mkdir(name string) error {
	return d.s.mkdir(name)
}

// check if the name is valid.
// returns an error if invalid, or nil
func (d *dirfs) check(name string) error {
	if strings.ContainsRune(name, os.PathSeparator) {
		return NewInvalidName(name)
	}
	return nil
}

// mkroot creates the dirfs root directory.
func (d *dirfs) mkroot() error {
	return d.mkdir(d.root)
}

// path returns the file path associated with the given name.
func (d *dirfs) path(name string) string {
	return path.Join(d.root, name)
}
