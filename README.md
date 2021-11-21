# picodb

`picodb` is a simplistic file system based key-value store written in go

# usage

## basic example

The basic example demonstrates how to use the library with default settings.

```go
import (
	"okki.hu/picodb"
)

func example() {
    // make a new picodb instance with default settins
    pico := picodb.New(picodb.Defaults()

    // store a string
    err := pico.StoreString("key", "pico")

    // retrieve the stored string
    v, err := pico.LoadString(key)    // v == "pico"

    // remove the key
    pico.Delete("key")
}
```

## non-string values

Non-string values can be stored/loaded as `[]byte`. (Serialize your objects using the `gob` package.)

```go
pico.Store("bytes", []byte{})
```

## setting custom options

One way is to pass in a `PicoDbOptions` to `New`. The following example sets a couple of custom options:

   * root directory: where the values will be persisted
   * file mode: file creation mode
   * dir mode: directory creation mode (used for root dir if not exists)


```go
import (
	"os"
	"okki.hu/picodb"
)

func demo() {
	opts := &picodb.PicoDbOptions{
		RootDir:  "dir",
		FileMode: os.FileMode(0666),
		DirMode:  os.FileMode(0777),
	}
	pico := picodb.New(opts)

}
```

The above example can also be written using the provided builders:

```go
import (
	"os"
	"okki.hu/picodb"
)

func example() {
	pico := picodb.New(
		picodb.Defaults().
			WithRootDir("dir").
			WithFileMode(os.FileMode(0666)).
			WithDirMode(os.FileMode(0777)))	
}
```

# additional features

## caching

Turn on the built-in caching to get superior performance on repeated loads for the same key. Keys are cached on both writes and reads. Deleting a key removes it from the cache.

Note, that the built-in basic cache does not support expiry of values, nor does it have a maximum size.

```go
import (
	"okki.hu/picodb"
)

func example() {
	pico := picodb.New(picodb.Defaults().WithCaching())
	// assuming the key "foo" exists
	pico.Load("foo")
	pico.Load("foo") // will be loaded from cache
}
```

## locking

Locking uses file locks (`flock`) to ensure that only one thread can write the file belonging to a key. Other threads will block and wait until writing is done and the lock is released. Enabling locking slightly reduces performance.

```go
func example() {
    pico := picodb.New(picodb.Defaults().WithLocking())
    pico.StoreString("foo", "bar")   // will lock "foo" while writing
}
```

## compression

Compression can potentially decrease data size at rest. It uses standard gzip compression on the values when persisting them to the disk.

Note that compression has a significant performance impact. In addition, the size of the stored values must be sufficiently large to make compression worthwhile.

```go
func example() {
    pico := picodb.New(picodb.Defaults().WithCompression())
    var data []byte	// large data	
    pico.Store("foo", data)
}
```