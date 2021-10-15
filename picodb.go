package picodb

import "os"

// PicoDb is a simplistic directory based key-value storage.
// Keys are always of type string, and PicoDb will store the data
// associated with the given key under a file with the same name.
// PicoDb is always initialized with a root path, which will
// contain the data.
type PicoDb struct {
	opt *PicoDbOptions
}

// PicoDbOptions contains options which are passed on to the
// New function to create a PicoDb instace.
// RootPath: the root directory where PicoDb will store the data
// FileMode: (optional) when the RootPath does not exist, it will be created
//           using this file mode the default value is 0700
type PicoDbOptions struct {
	RootPath string      // the root directory
	FileMode os.FileMode // the file mode of the picodb root path
}

func New(options *PicoDbOptions) (*PicoDb, error) {
	if options.FileMode == os.FileMode(0) {
		options.FileMode = 0700
	}
	err := os.MkdirAll(options.RootPath, options.FileMode)
	if err != nil {
		return nil, err
	}
	return &PicoDb{
		opt: options,
	}, nil
}
