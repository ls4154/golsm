package fs

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOSEnvWritableAndAppendableFile(t *testing.T) {
	env := Default()
	name := filepath.Join(t.TempDir(), "data")

	w, err := env.NewWritableFile(name)
	require.NoError(t, err)
	_, err = w.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	data, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	w, err = env.NewAppendableFile(name)
	require.NoError(t, err)
	_, err = w.Write([]byte(" world"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	data, err = os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(data))
}

func TestOSEnvDirectoryAndFileHelpers(t *testing.T) {
	env := Default()
	root := t.TempDir()
	subdir := filepath.Join(root, "child")
	name := filepath.Join(root, "data")

	require.NoError(t, env.CreateDir(subdir))

	w, err := env.NewWritableFile(name)
	require.NoError(t, err)
	_, err = w.Write([]byte("abc"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.True(t, env.FileExists(name))

	size, err := env.GetFileSize(name)
	require.NoError(t, err)
	require.Equal(t, uint64(3), size)

	children, err := env.GetChildren(root)
	require.NoError(t, err)
	sort.Strings(children)
	require.Equal(t, []string{"child", "data"}, children)

	require.NoError(t, env.RemoveFile(name))
	require.NoError(t, env.RemoveDir(subdir))

	require.False(t, env.FileExists(name))

	_, err = env.GetFileSize(name)
	require.True(t, IsNotExist(err))
}

func TestOSEnvLockFile(t *testing.T) {
	env := Default()
	lock, err := env.LockFile(filepath.Join(t.TempDir(), "LOCK"))
	require.NoError(t, err)
	require.NoError(t, env.UnlockFile(lock))
}
