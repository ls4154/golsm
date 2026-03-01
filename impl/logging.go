package impl

import (
	stdlog "log"

	"github.com/ls4154/golsm/db"
)

func openInfoLogger(env db.Env, dbname string, logger db.Logger) (db.Logger, db.WritableFile, error) {
	if logger != nil {
		return logger, nil, nil
	}

	f, err := env.NewAppendableFile(InfoLogFileName(dbname))
	if err != nil {
		return nil, nil, err
	}

	return stdlog.New(f, "", stdlog.LstdFlags|stdlog.Lmicroseconds), f, nil
}

func statusForLog(err error) string {
	if err == nil {
		return "OK"
	}
	return err.Error()
}
