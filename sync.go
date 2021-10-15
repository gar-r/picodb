package picodb

import "github.com/gofrs/flock"

// writeWithLock uses flock to obtain an exclusive lock
// and write the data to the given path.
// Using this function is safe even when multiple
// processes are writing the same file.
func (p *PicoDb) writeWithLock(path string, data interface{}) error {
	lock := flock.New(path)
	err := lock.Lock()
	if err != nil {
		return err
	}
	defer lock.Unlock()
	return p.writeData(path, data)
}

// readWithLock uses flock to obtain a read lock
// and read the data from the given path.
// If there are no errors, the result is returned
// in the data out parameter.
// Using this function is safe even when multiple
// processes are writing the same file.
func (p *PicoDb) readWithLock(path string, data interface{}) error {
	lock := flock.New(path)
	err := lock.RLock()
	if err != nil {
		return err
	}
	defer lock.Unlock()
	return p.readData(path, data)
}
