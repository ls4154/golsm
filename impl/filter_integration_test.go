package impl

import (
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func TestDBGetWithBloomFilterAfterReopen(t *testing.T) {
	testDir := t.TempDir()

	opt := db.DefaultOptions()
	opt.FilterPolicy = util.NewBloomFilterPolicy(10)

	ldb, err := Open(opt, testDir)
	require.NoError(t, err)

	require.NoError(t, ldb.Put([]byte("hello"), []byte("world"), nil))
	require.NoError(t, ldb.Close())

	ldb, err = Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	v, err := ldb.Get([]byte("hello"), nil)
	require.NoError(t, err)
	require.Equal(t, []byte("world"), v)

	_, err = ldb.Get([]byte("not-found"), nil)
	require.ErrorIs(t, err, db.ErrNotFound)
}
