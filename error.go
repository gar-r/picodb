package picodb

const ErrNoExist = "key does not exist"
const ErrReservedKey = "key is reserved and cannot be used"
const ErrCacheDisabled = "caching is disabled"

func IsNoExist(err error) bool {
	return err.Error() == ErrNoExist
}
