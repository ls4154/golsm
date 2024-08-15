package db

type FilterPolicy interface {
	Name() string
	CreateFilter(keys [][]byte) []byte
	MayMatch(key, filter []byte) bool
}
