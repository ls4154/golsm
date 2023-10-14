package impl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionEdit(t *testing.T) {
	edit := VersionEdit{}

	edit.SetComparator("test.comparator")
	edit.SetLogNumber(12345)
	edit.SetLastSequence(9999999)
	for i := 0; i < 3; i++ {
		edit.RemoveFile(uint64(1000+i), i)
	}
	for i := 0; i < 6; i++ {
		edit.RemoveFile(uint64(2000+i), i)
	}

	encoded := edit.Append(nil)

	decoded := VersionEdit{}
	decoded.DecodeFrom(encoded)

	require.Equal(t, edit, decoded)
}
