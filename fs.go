package picodb

import (
	"os"
)

// writeData encodes and dumps the data into the given file
// without any concern for concurrent access or inter-
// process locking
func (p *PicoDb) writeData(path string, data interface{}) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Sync()
	defer f.Close()
	return p.encode(f, data)
}

// readData decodes data from the given file
// without any concern for concurrent access or inter-
// process locking
// data is an out parameter containing the decoded result
// if there was no error
func (p *PicoDb) readData(path string, data interface{}) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return p.decode(f, data)
}
