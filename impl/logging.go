package impl

import (
	"io"
	stdlog "log"

	"github.com/ls4154/golsm/db"
)

type flushOnWrite struct {
	f db.WritableFile
}

func (w *flushOnWrite) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	if err := w.f.Flush(); err != nil {
		return n, err
	}
	return n, nil
}

func openInfoLogger(env db.Env, dbname string, logger db.Logger) (db.Logger, db.WritableFile, error) {
	if logger != nil {
		return logger, nil, nil
	}

	f, err := env.NewAppendableFile(InfoLogFileName(dbname))
	if err != nil {
		return nil, nil, err
	}

	return stdlog.New(&flushOnWrite{f: f}, "", stdlog.LstdFlags|stdlog.Lmicroseconds), f, nil
}

func statusForLog(err error) string {
	if err == nil {
		return "OK"
	}
	return err.Error()
}
