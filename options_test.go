package picodb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Defaults(t *testing.T) {
	opt := Defaults()
	assert.NotNil(t, opt)
	assert.NotEmpty(t, opt.RootDir)
}

func Test_Builders(t *testing.T) {
	opt := Defaults().
		WithRootDir("test").
		WithCaching().
		WithCompression().
		WithLocking().
		WithFileMode(0666).
		WithDirMode(0777)

	assert.Equal(t, "test", opt.RootDir)
	assert.True(t, opt.Caching)
	assert.True(t, opt.Compression)
	assert.True(t, opt.Locking)
	assert.Equal(t, os.FileMode(0666), opt.FileMode)
	assert.Equal(t, os.FileMode(0777), opt.DirMode)
}
