package goldb

import (
	"fmt"
	"path/filepath"
)

func CurrentFileName(dbname string) string {
	return filepath.Join(dbname, "CURRENT")
}

func LogFileName(dbname string, num uint64) string {
	return filepath.Join(dbname, fmt.Sprintf("%06d.log", num))
}

func TableFileName(dbname string, num uint64) string {
	return filepath.Join(dbname, fmt.Sprintf("%06d.ldb", num))
}

func SSTTableFileName(dbname string, num uint64) string {
	return filepath.Join(dbname, fmt.Sprintf("%06d.sst", num))
}

func TempFileName(dbname string, num uint64) string {
	return filepath.Join(dbname, fmt.Sprintf("%06d.dbtmp", num))
}

func DescriptorFileName(dbname string, num uint64) string {
	return filepath.Join(dbname, fmt.Sprintf("MANIFEST-%06d", num))
}

func LockFileName(dbname string) string {
	return filepath.Join(dbname, "LOCK")
}
