package picodb

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_New(t *testing.T) {

	t.Run("create with default options", func(t *testing.T) {
		pico := New(Defaults())
		assert.NotNil(t, pico)
	})

	t.Run("create with custom options", func(t *testing.T) {
		pico := New(&PicoDbOptions{
			RootDir:     "test",
			Compression: true,
			Caching:     true,
		})
		assert.NotNil(t, pico)
	})

}

func Test_Store(t *testing.T) {
	pico := New(Defaults())
	defer os.RemoveAll(pico.opt.RootDir)

	bytes := []byte{1, 3, 5, 7}

	t.Run("read back stored value", func(t *testing.T) {
		require.NoError(t, pico.Store("readwrite", bytes))
		actual, err := pico.Load("readwrite")
		require.NoError(t, err)
		assert.Equal(t, bytes, actual)
	})

	t.Run("store an invalid key", func(t *testing.T) {
		key := path.Join("foo", "bar")
		err := pico.Store(key, bytes)
		assert.ErrorIs(t, err, NewInvalidKey(key))
	})

}

func Test_Load(t *testing.T) {
	pico := New(Defaults())
	defer os.RemoveAll(pico.opt.RootDir)

	t.Run("read missing key", func(t *testing.T) {
		key := "foo"
		_, err := pico.Load(key)
		assert.ErrorIs(t, err, NewKeyNotFound(key))
	})

	t.Run("read invalid key", func(t *testing.T) {
		key := path.Join("foo", "bar")
		_, err := pico.Load(key)
		assert.ErrorIs(t, err, NewInvalidKey(key))
	})

	t.Run("read key which points to a directory", func(t *testing.T) {
		// TODO
	})

}

func Test_Delete(t *testing.T) {
	pico := New(Defaults())
	defer os.RemoveAll(pico.opt.RootDir)

	t.Run("delete key", func(t *testing.T) {
		key := "foo"
		require.NoError(t, pico.Store(key, []byte("bar")))
		err := pico.Delete(key)
		assert.NoError(t, err)
		_, err = pico.Load(key)
		assert.ErrorIs(t, err, NewKeyNotFound(key))
	})

	t.Run("delete missing key", func(t *testing.T) {
		err := pico.Delete("missing")
		assert.NoError(t, err)
	})

}

func Test_Lock(t *testing.T) {
	pico := New(Defaults())
	defer os.RemoveAll(pico.opt.RootDir)
	bytes := []byte{1, 3, 5, 7}

	t.Run("read back stored value", func(t *testing.T) {
		require.NoError(t, pico.StoreWithLock("lock", bytes))
		actual, err := pico.Load("lock")
		require.NoError(t, err)
		assert.Equal(t, bytes, actual)
	})

	t.Run("read with lock", func(t *testing.T) {
		require.NoError(t, pico.Store("rlock", bytes))
		actual, err := pico.LoadWithLock("rlock")
		require.NoError(t, err)
		assert.Equal(t, bytes, actual)
	})
}
