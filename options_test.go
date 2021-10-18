package picodb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_OptionsBuilder(t *testing.T) {
	opt := Defaults().
		WithRootDir("test").
		WithCaching().
		WithCompression().
		WithFileMode(0600).
		WithDirMode(0700)

	assert.Equal(t, "test", opt.RootDir)
	assert.True(t, opt.Caching)
	assert.True(t, opt.Compression)
	assert.Equal(t, os.FileMode(0600), opt.FileMode)
	assert.Equal(t, os.FileMode(0700), opt.DirMode)
}
