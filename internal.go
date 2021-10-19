package picodb

// storage represents a generic interface which can read and write bytes based on a name.
type storage interface {
	write(string, []byte) error  // write bytes to a given name
	read(string) ([]byte, error) // read bytes from a given name
	remove(string) error         // delete a given name
	mkdir(string) error          // make directory with the given name
	getl(string) lock            // get a lock for the given name
}

// kvs represents a basic key-value store
type kvs interface {
	store(string, []byte) error  // store a key-value pair
	load(string) ([]byte, error) // load a key
	delete(string) error         // delete a key
}

// lock represents a lock on a given resource
type lock interface {
	Lock() error   // lock the resource
	Unlock() error // unlock the resource
}
