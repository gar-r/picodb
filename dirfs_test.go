package picodb

import (
	"errors"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_DirFs(t *testing.T) {

	dfs := &dirfs{root: "root"}

	t.Run("read", func(t *testing.T) {

		t.Run("read from invalid key", func(t *testing.T) {
			key := path.Join("foo", "bar")
			_, err := dfs.load(key)
			assert.ErrorIs(t, err, NewKeyInvalid(key))
		})

		t.Run("read non-existing key", func(t *testing.T) {
			dfs.s = &testFs{
				readResult: func(s string) ([]byte, error) {
					return nil, os.ErrNotExist
				},
			}
			key := "missing"
			_, err := dfs.load(key)
			assert.ErrorIs(t, err, NewKeyNotFound(key))
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
			b, err := dfs.load("foo")
			require.NoError(t, err)
			assert.Equal(t, []byte{1}, b)
		})

		t.Run("read error", func(t *testing.T) {
			dfs.s = &testFs{
				readResult: func(s string) ([]byte, error) {
					return nil, errors.New("test")
				},
			}
			_, err := dfs.load("foo")
			assert.Error(t, err)
		})

	})

	t.Run("write", func(t *testing.T) {

		t.Run("write to invalid key", func(t *testing.T) {
			key := path.Join("foo", "bar")
			err := dfs.store(key, []byte{})
			assert.ErrorIs(t, err, NewKeyInvalid(key))
		})

		t.Run("root directory is created", func(t *testing.T) {
			dfs.s = &testFs{
				mkdirVerify: func(s string) {
					assert.Equal(t, "root", s)
				},
			}
			require.NoError(t, dfs.store("foo", []byte{}))
		})

		t.Run("root dir creation error", func(t *testing.T) {
			dfs.s = &testFs{
				mkdirResult: func(s string) error {
					return errors.New("test")
				},
			}
			assert.Error(t, dfs.store("foo", []byte{}))
		})

		t.Run("write to underlying storage", func(t *testing.T) {
			dfs.s = &testFs{
				writeVerify: func(s string, b []byte) {
					assert.Equal(t, "root/foo", s)
					assert.Equal(t, []byte{1}, b)
				},
			}
			require.NoError(t, dfs.store("foo", []byte{1}))
		})

		t.Run("write error", func(t *testing.T) {
			dfs.s = &testFs{
				writeResult: func(s string, b []byte) error {
					return errors.New("test")
				},
			}
			assert.Error(t, dfs.store("foo", []byte{}))
		})

	})

	t.Run("delete", func(t *testing.T) {

		t.Run("delete invalid key", func(t *testing.T) {
			key := path.Join("foo", "bar")
			err := dfs.delete(key)
			assert.ErrorIs(t, err, NewKeyInvalid(key))
		})

		t.Run("delete non-existing key", func(t *testing.T) {
			dfs.s = &testFs{
				removeResult: func(s string) error {
					return os.ErrNotExist
				},
			}
			assert.NoError(t, dfs.delete("foo"))
		})

		t.Run("delete existing key", func(t *testing.T) {
			dfs.s = &testFs{
				removeVerify: func(s string) {
					assert.Equal(t, "root/foo", s)
				},
			}
			assert.NoError(t, dfs.delete("foo"))
		})

		t.Run("delete error", func(t *testing.T) {
			dfs.s = &testFs{
				removeResult: func(s string) error {
					return errors.New("test")
				},
			}
			assert.Error(t, dfs.delete("foo"))
		})

	})
}

func Test_Locking(t *testing.T) {
	tl := &testLock{}
	dfs := &dirfs{
		root:    "root",
		locking: true,
		s: &testFs{
			getlResult: func(s string) lock {
				return tl
			},
		},
	}

	t.Run("write with lock", func(t *testing.T) {
		locked := false
		tl.lockResult = func() error {
			locked = true
			return nil
		}
		dfs.store("foo", []byte{})
		assert.True(t, locked)
	})

	t.Run("unlock is called", func(t *testing.T) {
		called := false
		tl.unlockResult = func() error {
			called = true
			return nil
		}
		dfs.store("foo", []byte{})
		assert.True(t, called)
	})

	t.Run("obtain lock error", func(t *testing.T) {
		tl.lockResult = func() error {
			return errors.New("test")
		}
		err := dfs.store("foo", []byte{})
		assert.Error(t, err)
	})

}

// mock fs used for testing
type testFs struct {
	writeResult  func(string, []byte) error
	writeVerify  func(string, []byte)
	readResult   func(string) ([]byte, error)
	readVerify   func(string)
	removeResult func(string) error
	removeVerify func(string)
	mkdirResult  func(string) error
	mkdirVerify  func(string)
	getlResult   func(string) lock
}

func (f *testFs) reset() {
	f.writeResult = nil
	f.writeVerify = nil
	f.readResult = nil
	f.readVerify = nil
	f.removeResult = nil
	f.removeVerify = nil
	f.mkdirResult = nil
	f.mkdirVerify = nil
	f.getlResult = nil
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

func (f *testFs) remove(name string) error {
	if f.removeVerify != nil {
		f.removeVerify(name)
	}
	if f.removeResult != nil {
		return f.removeResult(name)
	}
	return nil
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

func (f *testFs) getl(name string) lock {
	if f.getlResult != nil {
		return f.getlResult(name)
	}
	return nil
}

type testLock struct {
	lockResult   func() error
	unlockResult func() error
}

func (l *testLock) Lock() error {
	if l.lockResult != nil {
		return l.lockResult()
	}
	return nil
}

func (l *testLock) Unlock() error {
	if l.unlockResult != nil {
		return l.unlockResult()
	}
	return nil
}
