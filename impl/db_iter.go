package impl

type dbIter struct{}

type internalIterator interface {
	Valid() bool
	Next()
	Prev()
	SeekToFirst()
	SeekToLast()
	Seek(key []byte)
	Key() []byte
	Value() []byte
}
