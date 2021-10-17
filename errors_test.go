package picodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_KeyNotFound(t *testing.T) {

	t.Run("equality", func(t *testing.T) {
		e1 := NewKeyNotFound("test")
		e2 := NewKeyNotFound("test")
		assert.ErrorIs(t, e1, e2)
	})

	t.Run("error message", func(t *testing.T) {
		e := NewKeyNotFound("key")
		assert.Contains(t, e.Error(), "key")
	})

}

func Test_InvalidKey(t *testing.T) {

	t.Run("equality", func(t *testing.T) {
		e1 := NewInvalidKey("test")
		e2 := NewInvalidKey("test")
		assert.ErrorIs(t, e1, e2)
	})

	t.Run("error message", func(t *testing.T) {
		e := NewInvalidKey("key")
		assert.Contains(t, e.Error(), "key")
	})

}

func Test_KeyConflict(t *testing.T) {

	t.Run("equality", func(t *testing.T) {
		e1 := NewKeyConflict("key", "dir")
		e2 := NewKeyConflict("key", "dir")
		assert.ErrorIs(t, e1, e2)
	})

	t.Run("error message", func(t *testing.T) {
		e := NewKeyConflict("key", "dir")
		assert.Contains(t, e.Error(), "key")
		assert.Contains(t, e.Error(), "dir")
	})

}
