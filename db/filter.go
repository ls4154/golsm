package db

type FilterPolicy interface {
	Name() string
	AppendFilter(keys [][]byte, dst []byte) []byte
	MayMatch(key, filter []byte) bool
}
