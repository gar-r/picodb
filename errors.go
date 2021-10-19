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

type KeyInvalid struct {
	name string
}

func NewKeyInvalid(name string) KeyInvalid {
	return KeyInvalid{
		name: name,
	}
}

func (e KeyInvalid) Error() string {
	return fmt.Sprintf("invalid key: %s", e.name)
}
