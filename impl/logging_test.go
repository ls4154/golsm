package impl

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ls4154/golsm/fs"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func TestOpenInfoLoggerFlushesEachWrite(t *testing.T) {
	env := fs.Default()
	dbname := t.TempDir()

	logger, f, err := openInfoLogger(env, dbname, nil)
	require.NoError(t, err)
	require.NotNil(t, logger)
	require.NotNil(t, f)
	defer func() {
		_ = f.Close()
	}()

	logger.Printf("flush-test")

	data, err := util.ReadFile(env, filepath.Join(dbname, util.DefaultLogFileName))
	require.NoError(t, err)
	require.True(t, strings.Contains(string(data), "flush-test"))
}
