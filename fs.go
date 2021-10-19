package picodb

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
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

// fsc is a storage implementation storing compressed bytes using the file system
type fsc struct {
	s storage
}

// write compressed bytes to a file indicated by name.
func (f *fsc) write(name string, val []byte) error {
	b, err := f.compress(val)
	if err != nil {
		return err
	}
	return f.s.write(name, b)
}

func (f *fsc) compress(val []byte) ([]byte, error) {
	var buf bytes.Buffer
	z := gzip.NewWriter(&buf)
	_, err := z.Write(val)
	if err != nil {
		return nil, err
	}
	err = z.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// read and uncompress bytes from a file indicated by name.
func (f *fsc) read(name string) ([]byte, error) {
	b, err := f.s.read(name)
	if err != nil {
		return nil, err
	}
	return f.uncompress(b)
}

func (f *fsc) uncompress(val []byte) ([]byte, error) {
	z, err := gzip.NewReader(bytes.NewReader(val))
	if err != nil {
		return nil, err
	}
	defer z.Close()
	return ioutil.ReadAll(z)
}

// remove is a proxy to the same method on fs
func (f *fsc) remove(name string) error {
	return f.s.remove(name)
}

// mkdir is a proxy to the same method on fs
func (f *fsc) mkdir(name string) error {
	return f.s.mkdir(name)
}

// getl is a proxy to the same method on fs
func (f *fsc) getl(name string) lock {
	return f.s.getl(name)
}
