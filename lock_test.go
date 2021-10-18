package picodb

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	t.Run("store invalid key", func(t *testing.T) {
		key := path.Join("foo", "bar")
		err := pico.StoreWithLock(key, []byte("test"))
		assert.ErrorIs(t, err, NewInvalidName(key))
	})

	t.Run("load with invalid key", func(t *testing.T) {
		key := path.Join("foo", "bar")
		_, err := pico.LoadWithLock(key)
		assert.ErrorIs(t, err, NewInvalidName(key))
	})

	t.Run("store and load string value with locks", func(t *testing.T) {
		s := "test"
		require.NoError(t, pico.StoreStringWithLock("foo", s))
		r, err := pico.LoadStringWithLock("foo")
		assert.NoError(t, err)
		assert.Equal(t, s, r)
	})

}
