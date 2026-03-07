package util

import (
	"errors"
	"fmt"
	"io"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/fs"
)

func ReadFile(env fs.Env, fname string) ([]byte, error) {
	file, err := env.NewSequentialFile(fname)
	if err != nil {
		return nil, WrapIOError(err, "open file %s", fname)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, WrapIOError(err, "read file %s", fname)
	}

	return data, nil
}

func WrapIOError(err error, format string, args ...any) error {
	if err == nil || errors.Is(err, db.ErrIO) {
		return err
	}
	return fmt.Errorf("%w: %s: %v", db.ErrIO, fmt.Sprintf(format, args...), err)
}
