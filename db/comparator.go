package db

type Comparator interface {
	Compare(a, b []byte) int
	Name() string
	FindShortestSeparator(start *[]byte, limit []byte)
	FindShortSuccessor(key *[]byte)
}
