package picodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
