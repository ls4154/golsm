package db

type Logger interface {
	Printf(format string, v ...any)
}
