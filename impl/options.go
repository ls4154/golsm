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
	opt.ParanoidChecks = userOpt.ParanoidChecks
	opt.BlockSize = clipToRange(userOpt.BlockSize, 1<<10, 4<<20)
	opt.BlockRestartInterval = clipToRange(userOpt.BlockRestartInterval, 1, 128)
	opt.MaxFileSize = clipToRange(userOpt.MaxFileSize, 1<<20, 1<<30)
	opt.MaxManifestFileSize = clipToRange(userOpt.MaxManifestFileSize, uint64(1<<10), uint64(1<<30))
	opt.WriteBufferSize = clipToRange(userOpt.WriteBufferSize, 64<<10, 1<<30)
	opt.MaxOpenFiles = clipToRange(userOpt.MaxOpenFiles, 100, 50000)
	opt.BlockCacheSize = userOpt.BlockCacheSize
	opt.FilterPolicy = userOpt.FilterPolicy
	compactionOpt, err := validateCompactionOptions(userOpt.Compaction, opt.WriteBufferSize)
	if err != nil {
		return nil, err
	}
	opt.Compaction = compactionOpt

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

func validateCompactionOptions(userOpt *db.CompactionOptions, writeBufferSize int) (*db.CompactionOptions, error) {
	opt := db.DefaultCompactionOptions(writeBufferSize)
	if userOpt == nil {
		return opt, nil
	}

	if userOpt.L0CompactionTrigger != 0 {
		opt.L0CompactionTrigger = clipToRange(userOpt.L0CompactionTrigger, 1, 1000)
	}
	if userOpt.L0SlowdownTrigger != 0 {
		opt.L0SlowdownTrigger = clipToRange(userOpt.L0SlowdownTrigger, 1, 1000)
	}
	if userOpt.L0StopWritesTrigger != 0 {
		opt.L0StopWritesTrigger = clipToRange(userOpt.L0StopWritesTrigger, 1, 1000)
	}
	if userOpt.LevelBytesBase != 0 {
		opt.LevelBytesBase = clipToRange(userOpt.LevelBytesBase, uint64(1<<20), uint64(1<<50))
	}
	if userOpt.LevelBytesMultiplier != 0 {
		opt.LevelBytesMultiplier = clipToRange(userOpt.LevelBytesMultiplier, 1, 100)
	}

	if opt.L0SlowdownTrigger < opt.L0CompactionTrigger {
		return nil, fmt.Errorf("%w: L0SlowdownTrigger (%d) must be >= L0CompactionTrigger (%d)",
			db.ErrInvalidArgument, opt.L0SlowdownTrigger, opt.L0CompactionTrigger)
	}
	if opt.L0StopWritesTrigger < opt.L0SlowdownTrigger {
		return nil, fmt.Errorf("%w: L0StopWritesTrigger (%d) must be >= L0SlowdownTrigger (%d)",
			db.ErrInvalidArgument, opt.L0StopWritesTrigger, opt.L0SlowdownTrigger)
	}

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
