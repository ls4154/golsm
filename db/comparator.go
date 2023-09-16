package db

type Comparator interface {
	Compare(a, b []byte) int
	Name() string
	// TODO FindShortestSeparator(start []byte) []byte
}
