package table

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/env"
	"github.com/ls4154/golsm/util"
)

func TestTable(t *testing.T) {
	f, err := env.DefaultEnv().NewWritableFile("555555.ldb")
	if err != nil {
		t.Fatalf("failed to open file: %s", err)
	}
	defer f.Close()

	const numEntries = 100000

	builder := NewTableBuilder(f, util.BytewiseComparator, 4096, db.NoCompression, 16)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("test-key-%05d", i)
		ikey := []byte(key)
		ikey = binary.LittleEndian.AppendUint64(ikey, (uint64(i+1)<<8 | 1))
		value := fmt.Sprintf("test value %05d", i)

		builder.Add(ikey, []byte(value))
	}

	err = builder.Finish()
	if err != nil {
		t.Fatalf("failed to finish table: %s", err)
	}

	// verify using leveldbutil
	// ./leveldbutil dump 555555.ldb
}
