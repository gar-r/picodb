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

type InvalidName struct {
	name string
}

func NewInvalidName(name string) InvalidName {
	return InvalidName{
		name: name,
	}
}

func (e InvalidName) Error() string {
	return fmt.Sprintf("invalid name: %s", e.name)
}
