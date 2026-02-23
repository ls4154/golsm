package impl

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

type FileType uint8

const (
	FileTypeLog FileType = iota
	FileTypeLock
	FileTypeTable
	FileTypeDescriptor
	FileTypeCurrent
	FileTypeTemp
	FileTypeInfoLog
)

func CurrentFileName(dbname string) string {
	return filepath.Join(dbname, "CURRENT")
}

func LogFileName(dbname string, num FileNumber) string {
	return filepath.Join(dbname, fmt.Sprintf("%06d.log", num))
}

func TableFileName(dbname string, num FileNumber) string {
	return filepath.Join(dbname, fmt.Sprintf("%06d.ldb", num))
}

func SSTTableFileName(dbname string, num FileNumber) string {
	return filepath.Join(dbname, fmt.Sprintf("%06d.sst", num))
}

func TempFileName(dbname string, num FileNumber) string {
	return filepath.Join(dbname, fmt.Sprintf("%06d.dbtmp", num))
}

func DescriptorFileName(dbname string, num FileNumber) string {
	return filepath.Join(dbname, fmt.Sprintf("MANIFEST-%06d", num))
}

func LockFileName(dbname string) string {
	return filepath.Join(dbname, "LOCK")
}

func InfoLogFileName(dbname string) string {
	return filepath.Join(dbname, "LOG")
}

func ParseFileName(filename string) (FileType, FileNumber, bool) {
	if filename == "CURRENT" {
		return FileTypeCurrent, 0, true
	}

	if filename == "LOCK" {
		return FileTypeLock, 0, true
	}

	if filename == "LOG" || filename == "LOG.old" {
		return FileTypeInfoLog, 0, true
	}

	if strings.HasPrefix(filename, "MANIFEST-") {
		numStr := strings.TrimPrefix(filename, "MANIFEST-")
		num, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		return FileTypeDescriptor, FileNumber(num), true
	}

	var fileType FileType
	if strings.HasSuffix(filename, ".log") {
		filename = strings.TrimSuffix(filename, ".log")
		fileType = FileTypeLog
	} else if strings.HasSuffix(filename, ".ldb") {
		filename = strings.TrimSuffix(filename, ".ldb")
		fileType = FileTypeTable
	} else if strings.HasSuffix(filename, ".sst") {
		filename = strings.TrimSuffix(filename, ".sst")
		fileType = FileTypeTable
	} else if strings.HasSuffix(filename, ".dbtmp") {
		filename = strings.TrimSuffix(filename, ".dbtmp")
		fileType = FileTypeTemp
	} else {
		return 0, 0, false
	}

	num, err := strconv.ParseUint(filename, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return fileType, FileNumber(num), true
}
