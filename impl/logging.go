package impl

import (
	"io"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/fs"
	"github.com/ls4154/golsm/util"
)

func openInfoLogger(env fs.Env, dbname string, userLogger db.Logger) (db.Logger, io.Closer, error) {
	if userLogger != nil {
		return userLogger, nil, nil
	}

	const defaultInfoLogSize = 10 << 20
	logger, err := util.OpenFileLoggerDir(env, dbname, defaultInfoLogSize)
	if err != nil {
		return nil, nil, err
	}

	return logger, logger, nil
}

func statusForLog(err error) string {
	if err == nil {
		return "OK"
	}
	return err.Error()
}
