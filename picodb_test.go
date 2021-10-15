package picodb

import (
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_New(t *testing.T) {

	path, err := os.MkdirTemp(os.TempDir(), "pdb")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	t.Run("root path must a valid directory", func(t *testing.T) {
		dir := ""
		_, err := New(&PicoDbOptions{
			RootPath: dir,
		})
		assert.Error(t, err)
	})

	t.Run("root path is created if not exists", func(t *testing.T) {
		dir := path + "/create"
		_, err = New(&PicoDbOptions{
			RootPath: dir,
		})

		assert.NoError(t, err)
		assert.DirExists(t, dir)
	})

	t.Run("root path default file mode", func(t *testing.T) {
		dir := path + "/default-file-mode"
		_, err = New(&PicoDbOptions{
			RootPath: dir,
		})

		assert.NoError(t, err)
		info, err := os.Stat(dir)
		assert.NoError(t, err)
		assert.Equal(t, fs.FileMode(0700), info.Mode().Perm())
	})

	t.Run("root path explicit file mode", func(t *testing.T) {
		dir := path + "/file-mode"
		_, err = New(&PicoDbOptions{
			RootPath: dir,
			FileMode: 0744,
		})

		assert.NoError(t, err)
		info, err := os.Stat(dir)
		assert.NoError(t, err)
		assert.Equal(t, fs.FileMode(0744), info.Mode().Perm())
	})

}

func Test_Write(t *testing.T) {

	path, err := os.MkdirTemp(os.TempDir(), "pdb")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	dir := path + "/write"
	p, err := New(&PicoDbOptions{
		RootPath: dir,
	})
	require.NoError(t, err)

	data := struct {
		Str string
		Num int
	}{"test", 5}

	err = p.Write("key", data)
	assert.NoError(t, err)

}
