package picodb

import "os"

// PicoDbOptions contains options which are passed on to the
// New function to create a PicoDb instace.
type PicoDbOptions struct {
	RootDir     string      // root directory
	Compression bool        // enable compression at rest
	Caching     bool        // enable in-memory cache
	FileMode    os.FileMode // file mode used to create files
	DirMode     os.FileMode // file mode used to create directories
}

// Defaults returns a PicoDbOptions with sensible defaults.
func Defaults() *PicoDbOptions {
	return &PicoDbOptions{
		RootDir:     "./picodb",
		Compression: false,
		Caching:     false,
		FileMode:    0644,
		DirMode:     0744,
	}
}

func (p *PicoDbOptions) WithRootDir(rootDir string) *PicoDbOptions {
	p.RootDir = rootDir
	return p
}

func (p *PicoDbOptions) WithCompression() *PicoDbOptions {
	p.Compression = true
	return p
}

func (p *PicoDbOptions) WithCaching() *PicoDbOptions {
	p.Caching = true
	return p
}

func (p *PicoDbOptions) WithFileMode(fileMode os.FileMode) *PicoDbOptions {
	p.FileMode = fileMode
	return p
}

func (p *PicoDbOptions) WithDirMode(dirMode os.FileMode) *PicoDbOptions {
	p.DirMode = dirMode
	return p
}
