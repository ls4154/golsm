package util

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ls4154/golsm/fs"
	"github.com/stretchr/testify/require"
)

func TestFileLoggerOpenFlushesEachWrite(t *testing.T) {
	env := fs.Default()
	dirname := t.TempDir()

	logger, err := OpenFileLoggerDir(env, dirname, 0)
	require.NoError(t, err)
	require.NotNil(t, logger)
	defer func() {
		_ = logger.Close()
	}()

	logger.Printf("flush-test")

	data, err := ReadFile(env, filepath.Join(dirname, DefaultLogFileName))
	require.NoError(t, err)
	require.Contains(t, string(data), "flush-test")
}

func TestFileLoggerRotateBySize(t *testing.T) {
	env := fs.Default()
	dirname := t.TempDir()

	logger, err := OpenFileLoggerDir(env, dirname, 64)
	require.NoError(t, err)
	defer func() {
		_ = logger.Close()
	}()

	logger.Printf(strings.Repeat("a", 48))
	logger.Printf(strings.Repeat("b", 48))

	children, err := env.GetChildren(dirname)
	require.NoError(t, err)

	var activeFound bool
	var rotated []string
	for _, name := range children {
		switch {
		case name == "info_log":
			activeFound = true
		case strings.HasPrefix(name, "info_log."):
			rotated = append(rotated, filepath.Join(dirname, name))
		}
	}

	require.True(t, activeFound)
	require.NotEmpty(t, rotated)

	activeData, err := ReadFile(env, filepath.Join(dirname, DefaultLogFileName))
	require.NoError(t, err)
	require.Contains(t, string(activeData), strings.Repeat("b", 48))

	rotatedData, err := ReadFile(env, rotated[0])
	require.NoError(t, err)
	require.Contains(t, string(rotatedData), strings.Repeat("a", 48))
}
