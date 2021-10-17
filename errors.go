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
