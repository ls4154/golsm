package impl

import (
	"errors"
	"fmt"

	"github.com/ls4154/golsm/db"
)

func wrapIOError(err error, format string, args ...any) error {
	if err == nil || isAlreadyDBError(err) {
		return err
	}
	return fmt.Errorf("%w: %s: %v", db.ErrIO, fmt.Sprintf(format, args...), err)
}

func isAlreadyDBError(err error) bool {
	return errors.Is(err, db.ErrIO) ||
		errors.Is(err, db.ErrCorruption) ||
		errors.Is(err, db.ErrInvalidArgument) ||
		errors.Is(err, db.ErrNotFound) ||
		errors.Is(err, db.ErrNotSupported) ||
		errors.Is(err, errDBClosed)
}
