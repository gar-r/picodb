package picodb

import (
	"errors"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFs(t *testing.T) {

	fs := &fs{
		fmode: 0644,
		dmode: 0744,
	}

	t.Run("read", func(t *testing.T) {
		t.Run("read non-existing file", func(t *testing.T) {
			_, err := fs.read("dummy")
			assert.True(t, os.IsNotExist(err))
		})

		t.Run("read existing file", func(t *testing.T) {
			f, err := os.CreateTemp("", "pico")
			require.NoError(t, err)
			name := f.Name()
			defer os.Remove(name)

			val, err := fs.read(name)
			require.NoError(t, err)
			assert.NotNil(t, val)
		})
	})

	t.Run("write", func(t *testing.T) {
		dir := os.TempDir()

		t.Run("write data", func(t *testing.T) {
			name := path.Join(dir, "foo")
			defer os.Remove(name)
			require.NoError(t, fs.write(name, []byte("test")))
			val, err := fs.read(name)
			require.NoError(t, err)
			assert.Equal(t, "test", string(val))
		})

		t.Run("file mode", func(t *testing.T) {
			name := path.Join(dir, "bar")
			defer os.Remove(name)
			require.NoError(t, fs.write(name, []byte("test")))
			fi, err := os.Stat(name)
			require.NoError(t, err)
			assert.Equal(t, fs.fmode, fi.Mode().Perm())
		})

	})

	t.Run("remove", func(t *testing.T) {
		t.Run("remove non-existing name", func(t *testing.T) {
			assert.Error(t, fs.remove("missing"))
		})

		t.Run("remove existing name", func(t *testing.T) {
			f, err := os.CreateTemp("", "pico")
			name := f.Name()
			require.NoError(t, err)
			defer os.Remove(name)
			assert.NoError(t, fs.remove(name))
			_, err = os.Stat(name)
			assert.True(t, os.IsNotExist(err))
		})
	})

	t.Run("mkdir", func(t *testing.T) {
		dir, err := os.MkdirTemp("", "pico")
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		name := path.Join(dir, "foo", "bar")
		require.NoError(t, fs.mkdir(name))

		assert.DirExists(t, name)

		fi, err := os.Stat(name)
		require.NoError(t, err)
		assert.Equal(t, fs.dmode, fi.Mode().Perm())
	})

	t.Run("getl", func(t *testing.T) {
		l := fs.getl("foo")
		assert.NotNil(t, l)
	})

}

func Test_Compression(t *testing.T) {

	testFs := &testFs{}
	fs := &fsc{s: testFs}

	testErr := errors.New("test")

	t.Run("write compressed bytes", func(t *testing.T) {

		name := "foo"
		val := []byte{1, 2, 3, 4, 5}

		testFs.reset()
		testFs.writeResult = func(s string, b []byte) error {
			assert.Equal(t, name, s)
			assert.Greater(t, len(b), 0)
			return nil
		}

		err := fs.write(name, val)
		assert.NoError(t, err)
	})

	t.Run("write error", func(t *testing.T) {

		testFs.reset()
		testFs.writeResult = func(s string, b []byte) error {
			return testErr
		}

		err := fs.write("foo", nil)
		assert.ErrorIs(t, err, testErr)
	})

	t.Run("read compressed bytes", func(t *testing.T) {

		name := "foo"
		val := []byte("this is a test")

		var cap []byte

		testFs.reset()
		testFs.writeVerify = func(s string, b []byte) {
			cap = b // capture compressed bytes
		}
		testFs.readResult = func(s string) ([]byte, error) {
			return cap, nil
		}

		require.NoError(t, fs.write(name, val))
		v, err := fs.read(name)

		assert.NoError(t, err)
		assert.Equal(t, val, v)
	})

	t.Run("test proxied calls", func(t *testing.T) {

		name := "foo"

		testFs.reset()
		testFs.removeVerify = func(s string) {
			assert.Equal(t, name, s)
		}
		testFs.mkdirVerify = func(s string) {
			assert.Equal(t, name, s)
		}
		testFs.getlResult = func(s string) lock {
			assert.Equal(t, name, s)
			return nil
		}

		assert.NoError(t, fs.remove(name))
		assert.NoError(t, fs.mkdir(name))
		assert.Nil(t, fs.getl(name))

	})

}
