package impl

import (
	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

const maxLevelBytes = ^uint64(0)

type compactionPolicy struct {
	l0CompactionTrigger int
	l0SlowdownTrigger   int
	l0StopWritesTrigger int
	levelBytesBase      uint64
	levelBytesMul       int
	targetFileSize      uint64
}

func newCompactionPolicy(opt *db.CompactionOptions, targetFileSize uint64) *compactionPolicy {
	util.Assert(opt != nil)
	util.Assert(targetFileSize > 0)

	return &compactionPolicy{
		l0CompactionTrigger: opt.L0CompactionTrigger,
		l0SlowdownTrigger:   opt.L0SlowdownTrigger,
		l0StopWritesTrigger: opt.L0StopWritesTrigger,
		levelBytesBase:      opt.LevelBytesBase,
		levelBytesMul:       opt.LevelBytesMultiplier,
		targetFileSize:      targetFileSize,
	}
}

func (p *compactionPolicy) maxBytesForLevel(level Level) uint64 {
	if level <= 1 {
		return p.levelBytesBase
	}

	result := p.levelBytesBase
	mul := uint64(p.levelBytesMul)
	for lv := Level(2); lv <= level; lv++ {
		if result > maxLevelBytes/mul {
			return maxLevelBytes
		}
		result *= mul
	}
	return result
}

func (p *compactionPolicy) maxGrandparentOverlapBytes() uint64 {
	if p.targetFileSize > maxLevelBytes/10 {
		return maxLevelBytes
	}
	return 10 * p.targetFileSize
}

func (p *compactionPolicy) expandedCompactionByteSizeLimit() uint64 {
	if p.targetFileSize > maxLevelBytes/25 {
		return maxLevelBytes
	}
	return 25 * p.targetFileSize
}
