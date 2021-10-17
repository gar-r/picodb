package picodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_KeyNotFound(t *testing.T) {
	e1 := NewKeyNotFound("test")
	e2 := NewKeyNotFound("test")
	assert.ErrorIs(t, e1, e2)
}

func Test_InvalidKey(t *testing.T) {
	e1 := NewInvalidKey("test")
	e2 := NewInvalidKey("test")
	assert.ErrorIs(t, e1, e2)
}
