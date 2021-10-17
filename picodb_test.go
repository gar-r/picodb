package picodb

import (
	"io/fs"
	"os"
	"testing"
	"time"

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

func Test_Readback(t *testing.T) {

	path, err := os.MkdirTemp(os.TempDir(), "pdb")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	t.Run("standard storage", func(t *testing.T) {
		p, err := New(&PicoDbOptions{
			RootPath: path,
		})
		require.NoError(t, err)

		data := testData{
			Num:  5,
			Str:  "test",
			Flag: true,
		}

		assertReadback(t, p, data)
	})

	t.Run("compressed storage", func(t *testing.T) {
		p, err := New(&PicoDbOptions{
			RootPath:    path,
			Compression: true,
		})
		require.NoError(t, err)

		data := testData{
			Num:  5,
			Str:  "test",
			Flag: true,
		}

		assertReadback(t, p, data)
	})

}

func Test_MissingKey(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "pdb")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	p, err := New(&PicoDbOptions{
		RootPath: path,
	})
	require.NoError(t, err)

	exists, err := p.Exists("missing")
	require.NoError(t, err)
	assert.False(t, exists)

	var i interface{}
	err = p.Read("missing", &i)
	assert.Error(t, err)
	assert.True(t, IsNoExist(err))
}

func Test_Cache(t *testing.T) {

	path, err := os.MkdirTemp(os.TempDir(), "pdb")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	p, err := New(&PicoDbOptions{
		RootPath: path,
		Caching:  true,
	})
	require.NoError(t, err)

	t.Run("cache hit", func(t *testing.T) {

		data := testData{
			Num:  5,
			Str:  "test",
			Flag: true,
		}

		p.Write("test", data)

		var actual testData
		p.Read("test", &actual)

		assert.Equal(t, data, actual)
	})

	t.Run("cache miss", func(t *testing.T) {

		data := testData{
			Num:  5,
			Str:  "test",
			Flag: true,
		}

		p.Write("test", data)
		p.cache.Delete("test") // remove key from cache

		var actual testData
		p.Read("test", &actual) // this read should hit the fs

		assert.Equal(t, data, actual)
	})

}

func Test_Mutate(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "pdb")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	p, err := New(&PicoDbOptions{
		RootPath: path,
	})
	require.NoError(t, err)

	data := testData{
		Num:  5,
		Str:  "test",
		Flag: true,
	}

	p.Write("test", data)
	p.Mutate("test", &data, func(i interface{}) {
		d := i.(*testData)
		d.Num = 10
		d.Str = "mutated"
		d.Flag = true
	})

	// verify attributes are mutated in memory
	assert.Equal(t, data.Num, 10)
	assert.Equal(t, data.Str, "mutated")
	assert.Equal(t, data.Flag, true)

	// verify persisted data is mutated
	var stored testData
	err = p.Read("test", &stored)
	require.NoError(t, err)
	assert.Equal(t, stored, data)
}

func Test_Watcher(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "pdb")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	// create two picodb instances pointing to the same path

	p1, err := New(&PicoDbOptions{
		RootPath: path,
		Caching:  true,
	})
	require.NoError(t, err)
	_, err = p1.EnableWatcher(50 * time.Millisecond)
	defer p1.DisableWatcher()
	require.NoError(t, err)

	p2, err := New(&PicoDbOptions{
		RootPath: path,
		Caching:  true,
	})
	require.NoError(t, err)
	_, err = p2.EnableWatcher(time.Hour) // frequency doesn't matter for this one
	defer p2.DisableWatcher()
	require.NoError(t, err)

	p1.Write("test", "foo")
	p2.Write("test", "bar")

	time.Sleep(100 * time.Millisecond) // wait for p1 watcher cycle

	_, ok := p1.cache.Load("test")
	assert.False(t, ok) // the key in p1.cache should be invalidated
}

func assertReadback(t *testing.T, p *PicoDb, data interface{}) {
	t.Helper()
	err := p.Write("key", data)
	require.NoError(t, err)

	var actual testData
	err = p.Read("key", &actual)
	require.NoError(t, err)

	assert.Equal(t, data, actual)
}

type testData struct {
	Num  int
	Str  string
	Flag bool
}
