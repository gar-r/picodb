package picodb

import (
	"errors"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFs(t *testing.T) {

	fs := &fs{
		fmode: 0644,
		dmode: 0744,
	}

	t.Run("read", func(t *testing.T) {

		t.Run("read non-existing file", func(t *testing.T) {
			_, err := fs.read("dummy")
			assert.True(t, os.IsNotExist(err))
		})

		t.Run("read existing file", func(t *testing.T) {
			f, err := os.CreateTemp("", "pico")
			require.NoError(t, err)
			name := f.Name()
			defer os.Remove(name)

			val, err := fs.read(name)
			require.NoError(t, err)
			assert.NotNil(t, val)
		})

	})

	t.Run("write", func(t *testing.T) {
		dir := os.TempDir()

		t.Run("write data", func(t *testing.T) {
			name := path.Join(dir, "foo")
			defer os.Remove(name)
			require.NoError(t, fs.write(name, []byte("test")))
			val, err := fs.read(name)
			require.NoError(t, err)
			assert.Equal(t, "test", string(val))
		})

		t.Run("file mode", func(t *testing.T) {
			name := path.Join(dir, "bar")
			defer os.Remove(name)
			require.NoError(t, fs.write(name, []byte("test")))
			fi, err := os.Stat(name)
			require.NoError(t, err)
			assert.Equal(t, fs.fmode, fi.Mode().Perm())
		})

	})

	t.Run("mkdir", func(t *testing.T) {

		dir, err := os.MkdirTemp("", "pico")
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		name := path.Join(dir, "foo", "bar")
		require.NoError(t, fs.mkdir(name))

		assert.DirExists(t, name)

		fi, err := os.Stat(name)
		require.NoError(t, err)
		assert.Equal(t, fs.dmode, fi.Mode().Perm())
	})

}

func Test_DirFs(t *testing.T) {

	dfs := &dirfs{root: "root"}

	t.Run("read", func(t *testing.T) {

		t.Run("read from invalid name", func(t *testing.T) {
			name := path.Join("foo", "bar")
			_, err := dfs.read(name)
			assert.ErrorIs(t, err, NewInvalidName(name))
		})

		t.Run("read from underlying storage", func(t *testing.T) {
			dfs.s = &testFs{
				readResult: func(s string) ([]byte, error) {
					if s == "root/foo" {
						return []byte{1}, nil
					}
					return nil, nil
				},
			}
			b, err := dfs.read("foo")
			require.NoError(t, err)
			assert.Equal(t, []byte{1}, b)
		})

		t.Run("read error", func(t *testing.T) {
			dfs.s = &testFs{
				readResult: func(s string) ([]byte, error) {
					return nil, errors.New("test")
				},
			}
			_, err := dfs.read("foo")
			assert.Error(t, err)
		})

	})

	t.Run("write", func(t *testing.T) {

		t.Run("write to invalid name", func(t *testing.T) {
			name := path.Join("foo", "bar")
			err := dfs.write(name, []byte{})
			assert.ErrorIs(t, err, NewInvalidName(name))
		})

		t.Run("root directory is created", func(t *testing.T) {
			dfs.s = &testFs{
				mkdirVerify: func(s string) {
					assert.Equal(t, "root", s)
				},
			}
			require.NoError(t, dfs.write("foo", []byte{}))
		})

		t.Run("root dir creation error", func(t *testing.T) {
			dfs.s = &testFs{
				mkdirResult: func(s string) error {
					return errors.New("test")
				},
			}
			assert.Error(t, dfs.write("foo", []byte{}))
		})

		t.Run("write to underlying storage", func(t *testing.T) {
			dfs.s = &testFs{
				writeVerify: func(s string, b []byte) {
					assert.Equal(t, "root/foo", s)
					assert.Equal(t, []byte{1}, b)
				},
			}
			require.NoError(t, dfs.write("foo", []byte{1}))
		})

		t.Run("write error", func(t *testing.T) {
			dfs.s = &testFs{
				writeResult: func(s string, b []byte) error {
					return errors.New("test")
				},
			}
			assert.Error(t, dfs.write("foo", []byte{}))
		})

	})

}

// mock fs used for testing
type testFs struct {
	writeResult func(string, []byte) error
	writeVerify func(string, []byte)
	readResult  func(string) ([]byte, error)
	readVerify  func(string)
	mkdirResult func(string) error
	mkdirVerify func(string)
}

func (f *testFs) write(name string, val []byte) error {
	if f.writeVerify != nil {
		f.writeVerify(name, val)
	}
	if f.writeResult != nil {
		return f.writeResult(name, val)
	}
	return nil
}

func (f *testFs) read(name string) ([]byte, error) {
	if f.readVerify != nil {
		f.readVerify(name)
	}
	if f.readResult != nil {
		return f.readResult(name)
	}
	return nil, nil
}

func (f *testFs) mkdir(name string) error {
	if f.mkdirVerify != nil {
		f.mkdirVerify(name)
	}
	if f.mkdirResult != nil {
		return f.mkdirResult(name)
	}
	return nil
}
