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
		key := "missing"
		err := pico.Delete(key)
		assert.ErrorIs(t, err, NewKeyNotFound(key))
	})

}

func Test_Strings(t *testing.T) {
	pico := New(Defaults())
	defer os.RemoveAll(pico.opt.RootDir)

	t.Run("store and load string value", func(t *testing.T) {
		s := "test"
		require.NoError(t, pico.StoreString("foo", s))
		r, err := pico.LoadString("foo")
		assert.NoError(t, err)
		assert.Equal(t, s, r)
	})

	t.Run("load string error", func(t *testing.T) {
		key := path.Join("foo", "bar")
		_, err := pico.LoadString(key)
		assert.Error(t, err)
	})

}

func Test_Caching(t *testing.T) {
	pico := New(Defaults().WithCaching())
	defer os.RemoveAll(pico.opt.RootDir)

	t.Run("store value with cache", func(t *testing.T) {
		key := "foo"
		require.NoError(t, pico.StoreString(key, "test"))
		val, ok := pico.cache.Load(key)
		assert.True(t, ok)
		assert.Equal(t, "test", string(val.([]byte)))
	})

	t.Run("load value from cache", func(t *testing.T) {
		key := "bar"
		pico.cache.Store(key, []byte("test"))
		val, err := pico.LoadString(key)
		assert.NoError(t, err)
		assert.Equal(t, "test", val)
	})
}
