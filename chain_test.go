package picodb

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Chain(t *testing.T) {

	c1 := &cache{m: &sync.Map{}}
	c2 := &cache{m: &sync.Map{}}

	chain := chain{list: []kvs{c1, c2}}

	t.Run("store key in the chain", func(t *testing.T) {
		key := "foo"
		val := []byte{1, 2, 3, 4}
		require.NoError(t, chain.store(key, val))

		v1, err := c1.load(key)
		require.NoError(t, err)
		assert.Equal(t, val, v1)

		v2, err := c2.load(key)
		require.NoError(t, err)
		assert.Equal(t, val, v2)
	})

	t.Run("load key from chain", func(t *testing.T) {
		key := "bar"
		val := []byte{1, 2, 3, 4}
		require.NoError(t, c2.store(key, val))

		v, err := chain.load(key)
		require.NoError(t, err)
		assert.Equal(t, val, v)
	})

	t.Run("delete key from chain", func(t *testing.T) {
		key := "baz"
		val := []byte{1, 2, 3, 4}
		require.NoError(t, c1.store(key, val))
		require.NoError(t, c2.store(key, val))

		require.NoError(t, chain.delete(key))

		_, err := c1.load(key)
		assert.ErrorIs(t, err, NewKeyNotFound(key))

		_, err = c2.load(key)
		assert.ErrorIs(t, err, NewKeyNotFound(key))
	})

}

func Test_ChainErrors(t *testing.T) {
	c1 := &testKvp{}
	c2 := &testKvp{}

	notfound := NewKeyNotFound("foo")

	chain := chain{list: []kvs{c1, c2}}

	t.Run("error during store", func(t *testing.T) {
		c1.result = nil
		c2.result = errors.New("test")
		err := chain.store("foo", nil)
		assert.ErrorIs(t, err, c2.result)
	})

	t.Run("error during load", func(t *testing.T) {
		c1.result = errors.New("test")
		c2.result = nil
		_, err := chain.load("foo")
		assert.ErrorIs(t, err, c1.result)
	})

	t.Run("error during delete", func(t *testing.T) {
		c1.result = nil
		c2.result = errors.New("test")
		err := chain.delete("foo")
		assert.ErrorIs(t, err, c2.result)
	})

	t.Run("missing key skipped on load", func(t *testing.T) {
		c1.result = notfound
		c2.result = nil
		_, err := chain.load("foo")
		assert.NoError(t, err)
	})

	t.Run("key missing from chain", func(t *testing.T) {
		c1.result = notfound
		c2.result = notfound
		_, err := chain.load("foo")
		assert.ErrorIs(t, err, notfound)
	})

	t.Run("key missing during delete", func(t *testing.T) {
		c1.result = notfound
		c2.result = nil
		err := chain.delete("foo")
		assert.NoError(t, err)
	})

}

type testKvp struct {
	result error
}

func (t *testKvp) store(string, []byte) error {
	return t.result
}

func (t *testKvp) load(string) ([]byte, error) {
	return nil, t.result
}

func (t *testKvp) delete(string) error {
	return t.result
}
