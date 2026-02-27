package impl

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

func TestManifestRotationBySize(t *testing.T) {
	testDir := t.TempDir()

	opt := db.DefaultOptions()
	opt.WriteBufferSize = 4 << 10
	opt.MaxManifestFileSize = 1 << 10

	ldb, err := Open(opt, testDir)
	require.NoError(t, err)

	for i := 0; i < 1500; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := bytes.Repeat([]byte{byte(i)}, 4096)
		require.NoError(t, ldb.Put(key, val, nil))
	}
	require.NoError(t, ldb.Close())

	current, err := os.ReadFile(filepath.Join(testDir, "CURRENT"))
	require.NoError(t, err)

	ftype, fnum, ok := ParseFileName(strings.TrimSpace(string(current)))
	require.True(t, ok)
	require.Equal(t, FileTypeDescriptor, ftype)
	require.Greater(t, fnum, FileNumber(2))

	ldb, err = Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	for _, i := range []int{0, 123, 1499} {
		key := []byte(fmt.Sprintf("key-%06d", i))
		expect := bytes.Repeat([]byte{byte(i)}, 4096)
		got, err := ldb.Get(key, nil)
		require.NoError(t, err)
		require.Equal(t, expect, got)
	}
}
