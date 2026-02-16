package impl

import (
	"encoding/binary"
	"testing"

	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func makeInternalKeyForFilterTest(userKey []byte, seq uint64, vt ValueType) []byte {
	k := append([]byte(nil), userKey...)
	k = binary.LittleEndian.AppendUint64(k, PackSequenceAndType(seq, vt))
	return k
}

func TestInternalFilterPolicy(t *testing.T) {
	userPolicy := util.NewBloomFilterPolicy(10)
	policy := newInternalFilterPolicy(userPolicy)

	keys := [][]byte{
		makeInternalKeyForFilterTest([]byte("alpha"), 7, TypeValue),
		makeInternalKeyForFilterTest([]byte("beta"), 11, TypeValue),
	}

	filter := policy.AppendFilter(keys, nil)

	lookupAlpha := makeInternalKeyForFilterTest([]byte("alpha"), MaxSequenceNumber, TypeValue)
	lookupGamma := makeInternalKeyForFilterTest([]byte("gamma"), MaxSequenceNumber, TypeValue)

	require.Equal(t,
		userPolicy.MightContain([]byte("alpha"), filter),
		policy.MightContain(lookupAlpha, filter),
	)
	require.Equal(t,
		userPolicy.MightContain([]byte("gamma"), filter),
		policy.MightContain(lookupGamma, filter),
	)

	// malformed key fallback
	require.Equal(t,
		userPolicy.MightContain([]byte("x"), filter),
		policy.MightContain([]byte("x"), filter),
	)
}
