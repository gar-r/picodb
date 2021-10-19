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

func Test_InvalidName(t *testing.T) {

	t.Run("equality", func(t *testing.T) {
		e1 := NewKeyInvalid("test")
		e2 := NewKeyInvalid("test")
		assert.ErrorIs(t, e1, e2)
	})

	t.Run("error message", func(t *testing.T) {
		e := NewKeyInvalid("test")
		assert.Contains(t, e.Error(), "test")
	})

}
