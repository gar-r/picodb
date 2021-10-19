package picodb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Cache(t *testing.T) {

	c := &cache{
		m: &sync.Map{},
	}

	t.Run("load missing key", func(t *testing.T) {
		key := "missing"
		_, err := c.load(key)
		assert.ErrorIs(t, err, NewKeyNotFound(key))
	})

	t.Run("store and read back", func(t *testing.T) {
		key := "foo"
		val := []byte{1, 2, 3}
		assert.NoError(t, c.store(key, val))
		res, err := c.load(key)
		assert.NoError(t, err)
		assert.Equal(t, val, res)
	})

	t.Run("delete key", func(t *testing.T) {
		key := "bar"
		require.NoError(t, c.store(key, []byte{}))
		assert.NoError(t, c.delete(key))
		_, err := c.load(key)
		assert.ErrorIs(t, err, NewKeyNotFound(key))
	})

	t.Run("delete missing key", func(t *testing.T) {
		assert.NoError(t, c.delete("asd"))
	})

}
