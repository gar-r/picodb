package picodb

import (
	"os"
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
	bytes := []byte{1, 3, 5, 7}

	defer os.RemoveAll(pico.opt.RootDir)

	t.Run("store to sub-key", func(t *testing.T) {
		require.NoError(t, pico.Store("sub/dir", bytes))
		actual, err := pico.Load("sub/dir")
		require.NoError(t, err)
		assert.Equal(t, bytes, actual)
	})

	t.Run("read back stored value", func(t *testing.T) {
		require.NoError(t, pico.Store("readwrite", bytes))
		actual, err := pico.Load("readwrite")
		require.NoError(t, err)
		assert.Equal(t, bytes, actual)
	})

}

func Test_Read(t *testing.T) {
	pico := New(Defaults())

	t.Run("read missing key", func(t *testing.T) {
		_, err := pico.Load("missing")
		assert.Error(t, err)
		assert.True(t, IsErrKeyNotFound(err))
	})

	t.Run("read invalid key", func(t *testing.T) {
		require.NoError(t, pico.Store("foo/bar", []byte{}))
		_, err := pico.Load("foo")
		assert.Error(t, err)
		assert.True(t, IsErrKeyNotFound(err))
	})

}
