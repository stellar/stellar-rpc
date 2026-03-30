package db

type transactionalCache struct {
	entries map[string]string
}

func newTransactionalCache() transactionalCache {
	return transactionalCache{entries: map[string]string{}}
}
