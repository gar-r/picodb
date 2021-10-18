package picodb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Mutate(t *testing.T) {
	pico := New(Defaults())
	defer os.RemoveAll(pico.opt.RootDir)

	t.Run("mutate value", func(t *testing.T) {
		key := "foo"
		require.NoError(t, pico.Store(key, []byte{1, 2, 3}))
		err := pico.Mutate(key, func(b []byte) []byte {
			return append(b, 4)
		})
		assert.NoError(t, err)
		b, err := pico.Load(key)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3, 4}, b)
	})

	t.Run("mutate missing key", func(t *testing.T) {
		key := "missing"
		err := pico.Mutate(key, func(b []byte) []byte {
			return []byte{}
		})
		assert.ErrorIs(t, err, NewKeyNotFound(key))
	})

	t.Run("mutate invalid key", func(t *testing.T) {
		key := "foo/bar"
		err := pico.Mutate(key, func(b []byte) []byte {
			return []byte{}
		})
		assert.ErrorIs(t, err, NewInvalidKey(key))
	})

	t.Run("mutate invalid fn", func(t *testing.T) {
		assert.NoError(t, pico.Mutate("foo", nil))
	})

	t.Run("mutate string", func(t *testing.T) {
		key := "bar"
		require.NoError(t, pico.StoreString(key, "test"))
		err := pico.MutateString(key, func(s string) string {
			return s + "123"
		})
		assert.NoError(t, err)
		s, err := pico.LoadString(key)
		require.NoError(t, err)
		assert.Equal(t, "test123", s)
	})
}
