package impl

import (
	"cmp"
	"fmt"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

func validateOption(userOpt *db.Options) (*db.Options, error) {
	if userOpt == nil {
		return nil, fmt.Errorf("%w: option is nil", db.ErrInvalidArgument)
	}

	opt := &db.Options{}

	opt.CreateIfMissing = userOpt.CreateIfMissing
	opt.ErrorIfExists = userOpt.ErrorIfExists
	opt.BlockSize = clipToRange(userOpt.BlockSize, 1<<10, 4<<20)
	opt.BlockRestartInterval = clipToRange(userOpt.BlockRestartInterval, 1, 128)
	opt.MaxFileSize = clipToRange(userOpt.MaxFileSize, 1<<20, 1<<30)
	opt.WriteBufferSize = clipToRange(userOpt.WriteBufferSize, 64<<10, 1<<30)
	opt.MaxOpenFiles = clipToRange(userOpt.MaxOpenFiles, 100, 50000)
	opt.BlockCacheSize = userOpt.BlockCacheSize
	opt.FilterPolicy = userOpt.FilterPolicy

	if userOpt.Compression != db.NoCompression && userOpt.Compression != db.SnappyCompression {
		return nil, fmt.Errorf("%w: invalid compression type", db.ErrInvalidArgument)
	}
	opt.Compression = userOpt.Compression

	if userOpt.Comparator != nil {
		opt.Comparator = userOpt.Comparator
	} else {
		opt.Comparator = util.BytewiseComparator
	}
	opt.Logger = userOpt.Logger

	return opt, nil
}

func clipToRange[T cmp.Ordered](val, minVal, maxVal T) T {
	if val < minVal {
		return minVal
	}
	if val > maxVal {
		return maxVal
	}
	return val
}
