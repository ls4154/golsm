package golsm

import (
	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/impl"
)

func Open(options *db.Options, path string) (db.DB, error) {
	return impl.Open(options, path)
}
