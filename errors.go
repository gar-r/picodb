package picodb

const ErrKeyNotFound = "key not found"

func IsErrKeyNotFound(err error) bool {
	return ErrKeyNotFound == err.Error()
}
