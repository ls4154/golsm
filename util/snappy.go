package util

import (
	"github.com/golang/snappy"
)

func SnappyCompress(input []byte) []byte {
	return snappy.Encode(nil, input)
}

func SnappyUncompress(input []byte) ([]byte, error) {
	return snappy.Decode(nil, input)
}
