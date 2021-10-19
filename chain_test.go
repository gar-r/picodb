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
	c1 := &testKvs{}
	c2 := &testKvs{}

	notfound := NewKeyNotFound("foo")
	testErr := errors.New("test")

	chain := chain{list: []kvs{c1, c2}}

	t.Run("error during store", func(t *testing.T) {
		defer c1.reset()
		defer c2.reset()
		c2.storeMock = func(s string, b []byte) error { return testErr }
		err := chain.store("foo", nil)
		assert.ErrorIs(t, err, testErr)
	})

	t.Run("error during load", func(t *testing.T) {
		defer c1.reset()
		defer c2.reset()
		c1.loadMock = func(s string) ([]byte, error) { return nil, testErr }
		_, err := chain.load("foo")
		assert.ErrorIs(t, err, testErr)
	})

	t.Run("error during delete", func(t *testing.T) {
		defer c1.reset()
		defer c2.reset()
		c2.deleteMock = func(s string) error { return testErr }
		err := chain.delete("foo")
		assert.ErrorIs(t, err, testErr)
	})

	t.Run("missing key skipped on load", func(t *testing.T) {
		defer c1.reset()
		defer c2.reset()
		c1.loadMock = func(s string) ([]byte, error) { return nil, notfound }
		_, err := chain.load("foo")
		assert.NoError(t, err)
	})

	t.Run("key missing from chain", func(t *testing.T) {
		c1.loadMock = func(s string) ([]byte, error) { return nil, notfound }
		c2.loadMock = func(s string) ([]byte, error) { return nil, notfound }
		_, err := chain.load("foo")
		assert.ErrorIs(t, err, notfound)
	})

	t.Run("key missing partially", func(t *testing.T) {
		defer c1.reset()
		defer c2.reset()
		c1.deleteMock = func(s string) error { return notfound }
		err := chain.delete("foo")
		assert.NoError(t, err)
	})

}
