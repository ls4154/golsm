package db

type WriteBatch interface {
	Put(key, value []byte)
	Delete(key []byte)
}
