package picodb

import "fmt"

type KeyNotFound struct {
	key string
}

func NewKeyNotFound(key string) KeyNotFound {
	return KeyNotFound{
		key: key,
	}
}

func (e KeyNotFound) Error() string {
	return fmt.Sprintf("key not found: %s", e.key)
}

type InvalidKey struct {
	key string
}

func NewInvalidKey(key string) InvalidKey {
	return InvalidKey{
		key: key,
	}
}

func (e InvalidKey) Error() string {
	return fmt.Sprintf("invalid key: %s", e.key)
}

type KeyConflict struct {
	key string
	dir string
}

func NewKeyConflict(key, dir string) KeyConflict {
	return KeyConflict{
		key: key,
		dir: dir,
	}
}

func (e KeyConflict) Error() string {
	return fmt.Sprintf("conflicts between key: %s and directory: %s", e.key, e.dir)
}
