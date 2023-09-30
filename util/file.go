package util

import (
	"io"

	"github.com/ls4154/golsm/db"
)

func ReadFile(env db.Env, fname string) ([]byte, error) {
	file, err := env.NewSequentialFile(fname)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return data, nil
}
