package picodb

import (
	"errors"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_New(t *testing.T) {

	t.Run("id is not nil", func(t *testing.T) {
		pico := New(Defaults())
		assert.NotEmpty(t, pico.id)
	})

	t.Run("opts assigned", func(t *testing.T) {
		opt := &PicoDbOptions{}
		pico := New(opt)
		assert.Equal(t, opt, pico.opt)
	})

	t.Run("kvs with cache disabled", func(t *testing.T) {
		pico := New(Defaults())
		assert.IsType(t, &dirfs{}, pico.kvs)
	})

	t.Run("kvs with cache enabled", func(t *testing.T) {
		pico := New(Defaults().WithCaching())
		assert.IsType(t, &chain{}, pico.kvs)
		s := pico.kvs.(*chain)
		assert.IsType(t, &cache{}, s.list[0])
		assert.IsType(t, &dirfs{}, s.list[1])
	})

}

func Test_Store(t *testing.T) {

	s := &testKvs{}

	pico := &PicoDb{
		kvs: s,
	}

	testErr := errors.New("test")

	t.Run("store bytes", func(t *testing.T) {
		key := "foo"
		val := []byte{1, 2, 3, 4}
		defer s.reset()
		s.storeMock = func(s string, b []byte) error {
			assert.Equal(t, key, s)
			assert.Equal(t, val, b)
			return testErr
		}
		err := pico.Store(key, val)
		assert.ErrorIs(t, err, testErr)
	})

	t.Run("store string", func(t *testing.T) {
		key := "foo"
		val := "bar"
		defer s.reset()
		s.storeMock = func(s string, b []byte) error {
			assert.Equal(t, key, s)
			assert.Equal(t, []byte(val), b)
			return testErr
		}
		err := pico.StoreString(key, val)
		assert.ErrorIs(t, err, testErr)
	})

}

func Test_Load(t *testing.T) {

	s := &testKvs{}

	pico := &PicoDb{
		kvs: s,
	}

	testErr := errors.New("test")

	t.Run("load bytes", func(t *testing.T) {
		key := "foo"
		val := []byte{1, 2, 3}
		defer s.reset()
		s.loadMock = func(s string) ([]byte, error) {
			assert.Equal(t, key, s)
			return val, nil
		}
		v, err := pico.Load(key)
		assert.NoError(t, err)
		assert.Equal(t, val, v)
	})

	t.Run("load string", func(t *testing.T) {
		key := "foo"
		val := "bar"
		defer s.reset()
		s.loadMock = func(s string) ([]byte, error) {
			assert.Equal(t, key, s)
			return []byte(val), nil
		}
		v, err := pico.LoadString(key)
		assert.NoError(t, err)
		assert.Equal(t, val, v)
	})

	t.Run("load string error", func(t *testing.T) {
		key := "foo"
		defer s.reset()
		s.loadMock = func(s string) ([]byte, error) {
			assert.Equal(t, key, s)
			return nil, testErr
		}
		_, err := pico.LoadString(key)
		assert.ErrorIs(t, err, testErr)
	})

}

func Test_Delete(t *testing.T) {
	s := &testKvs{}
	pico := &PicoDb{
		kvs: s,
	}
	testErr := errors.New("test")
	key := "foo"
	defer s.reset()
	s.deleteMock = func(s string) error {
		assert.Equal(t, key, s)
		return testErr
	}
	err := pico.Delete(key)
	assert.ErrorIs(t, err, testErr)
}

type testKvs struct {
	storeMock  func(string, []byte) error
	loadMock   func(string) ([]byte, error)
	deleteMock func(string) error
}

func (t *testKvs) reset() {
	t.storeMock = nil
	t.loadMock = nil
	t.deleteMock = nil
}

func (t *testKvs) store(key string, val []byte) error {
	if t.storeMock != nil {
		return t.storeMock(key, val)
	}
	return nil
}

func (t *testKvs) load(key string) ([]byte, error) {
	if t.loadMock != nil {
		return t.loadMock(key)
	}
	return nil, nil
}

func (t *testKvs) delete(key string) error {
	if t.deleteMock != nil {
		return t.deleteMock(key)
	}
	return nil
}

var rnd *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func Benchmark_Store(b *testing.B) {

	str := randString(1000)

	b.Run("default", func(b *testing.B) {
		pico := New(Defaults())
		defer cleanup(b, pico)
		for i := 0; i < b.N; i++ {
			benchStore(b, pico, i, str)
		}
	})

	b.Run("compression", func(b *testing.B) {
		pico := New(Defaults().WithCompression())
		defer cleanup(b, pico)
		for i := 0; i < b.N; i++ {
			benchStore(b, pico, i, str)
		}
	})

	b.Run("caching", func(b *testing.B) {
		pico := New(Defaults().WithCaching())
		defer cleanup(b, pico)
		for i := 0; i < b.N; i++ {
			benchStore(b, pico, i, str)
		}
	})

	b.Run("locking", func(b *testing.B) {
		pico := New(Defaults().WithLocking())
		defer cleanup(b, pico)
		for i := 0; i < b.N; i++ {
			benchStore(b, pico, i, str)
		}
	})

}

func Benchmark_Load(b *testing.B) {

	key := "key"
	const Mb = 1000000
	str := randString(1 * Mb)

	b.Run("default", func(b *testing.B) {
		pico := New(Defaults())
		defer cleanup(b, pico)
		pico.StoreString(key, str)
		for i := 0; i < b.N; i++ {
			benchLoad(b, pico, key)
		}
	})

	b.Run("compression", func(b *testing.B) {
		pico := New(Defaults().WithCompression())
		defer cleanup(b, pico)
		pico.StoreString(key, str)
		for i := 0; i < b.N; i++ {
			benchLoad(b, pico, key)
		}
	})

	b.Run("caching", func(b *testing.B) {
		pico := New(Defaults().WithCaching())
		defer cleanup(b, pico)
		pico.StoreString(key, str)
		for i := 0; i < b.N; i++ {
			benchLoad(b, pico, key)
		}
	})

	b.Run("locking", func(b *testing.B) {
		pico := New(Defaults().WithLocking())
		defer cleanup(b, pico)
		pico.StoreString(key, str)
		for i := 0; i < b.N; i++ {
			benchLoad(b, pico, key)
		}
	})

}

func benchStore(b *testing.B, pico *PicoDb, i int, str string) {
	b.Helper()
	key := strconv.Itoa(i)
	err := pico.StoreString(key, str)
	if err != nil {
		b.Error(err)
	}

}

func benchLoad(b *testing.B, pico *PicoDb, key string) {
	b.Helper()
	_, err := pico.LoadString(key)
	if err != nil {
		b.Error(err)
	}

}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rnd.Intn(len(charset))]
	}
	return string(b)
}

func cleanup(b *testing.B, pico *PicoDb) {
	os.RemoveAll(pico.opt.RootDir)
}
