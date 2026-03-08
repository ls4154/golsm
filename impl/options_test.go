package impl

import (
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

func TestValidateOptionCompactionDefaultsWhenNil(t *testing.T) {
	userOpt := db.DefaultOptions()
	userOpt.Compaction = nil

	opt, err := validateOption(userOpt)
	require.NoError(t, err)

	require.NotNil(t, opt.Compaction)
	require.Equal(t, *db.DefaultCompactionOptions(opt.WriteBufferSize), *opt.Compaction)
}

func TestValidateOptionCompactionNilUsesSanitizedWriteBufferSize(t *testing.T) {
	userOpt := db.DefaultOptions()
	userOpt.WriteBufferSize = 64 << 20
	userOpt.Compaction = nil

	opt, err := validateOption(userOpt)
	require.NoError(t, err)
	require.Equal(t, uint64(512<<20), opt.Compaction.LevelBytesBase)
}

func TestValidateOptionCompactionUsesDefaultsForOmittedFields(t *testing.T) {
	userOpt := db.DefaultOptions()
	userOpt.Compaction = &db.CompactionOptions{
		LevelBytesBase: 64 << 20,
	}

	opt, err := validateOption(userOpt)
	require.NoError(t, err)

	def := db.DefaultCompactionOptions(userOpt.WriteBufferSize)
	require.Equal(t, def.L0CompactionTrigger, opt.Compaction.L0CompactionTrigger)
	require.Equal(t, def.L0SlowdownTrigger, opt.Compaction.L0SlowdownTrigger)
	require.Equal(t, def.L0StopWritesTrigger, opt.Compaction.L0StopWritesTrigger)
	require.Equal(t, uint64(64<<20), opt.Compaction.LevelBytesBase)
	require.Equal(t, def.LevelBytesMultiplier, opt.Compaction.LevelBytesMultiplier)
}

func TestValidateOptionCompactionRejectsInheritedDefaultOrderViolation(t *testing.T) {
	userOpt := db.DefaultOptions()
	userOpt.Compaction = &db.CompactionOptions{L0CompactionTrigger: 13}

	_, err := validateOption(userOpt)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
}

func TestValidateOptionCompactionRejectsSlowdownBelowCompaction(t *testing.T) {
	userOpt := db.DefaultOptions()
	userOpt.Compaction = &db.CompactionOptions{
		L0CompactionTrigger: 9,
		L0SlowdownTrigger:   3,
		L0StopWritesTrigger: 12,
	}

	_, err := validateOption(userOpt)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
}

func TestValidateOptionCompactionRejectsStopBelowSlowdown(t *testing.T) {
	userOpt := db.DefaultOptions()
	userOpt.Compaction = &db.CompactionOptions{
		L0CompactionTrigger: 4,
		L0SlowdownTrigger:   9,
		L0StopWritesTrigger: 5,
	}

	_, err := validateOption(userOpt)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
}

func TestValidateOptionCompactionAcceptsIncreasingOrder(t *testing.T) {
	userOpt := db.DefaultOptions()
	userOpt.Compaction = &db.CompactionOptions{
		L0CompactionTrigger: 4,
		L0SlowdownTrigger:   6,
		L0StopWritesTrigger: 9,
	}

	opt, err := validateOption(userOpt)
	require.NoError(t, err)
	require.Equal(t, 4, opt.Compaction.L0CompactionTrigger)
	require.Equal(t, 6, opt.Compaction.L0SlowdownTrigger)
	require.Equal(t, 9, opt.Compaction.L0StopWritesTrigger)
}
