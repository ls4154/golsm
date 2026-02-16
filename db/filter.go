package db

type FilterPolicy interface {
	Name() string
	AppendFilter(keys [][]byte, dst []byte) []byte
	MightContain(key, filter []byte) bool
}
