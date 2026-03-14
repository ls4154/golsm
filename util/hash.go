package util

import "encoding/binary"

func LDBHash(data []byte, seed uint32) uint32 {
	n := uint32(len(data))
	const m uint32 = 0xc6a4a793
	const r uint32 = 24

	h := seed ^ (n * m)

	i := uint32(0)
	for i+4 <= n {
		w := binary.LittleEndian.Uint32(data[i:])
		i += 4
		h += w
		h *= m
		h ^= (h >> 16)
	}

	switch n - i {
	case 3:
		h += uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h += uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h += uint32(data[i])
		h *= m
		h ^= (h >> r)
	}

	return h
}

func LDBHashString(s string, seed uint32) uint32 {
	n := uint32(len(s))
	const m uint32 = 0xc6a4a793
	const r uint32 = 24

	h := seed ^ (n * m)

	i := uint32(0)
	for i+4 <= n {
		w := uint32(s[i]) | uint32(s[i+1])<<8 | uint32(s[i+2])<<16 | uint32(s[i+3])<<24
		i += 4
		h += w
		h *= m
		h ^= (h >> 16)
	}

	switch n - i {
	case 3:
		h += uint32(s[i+2]) << 16
		fallthrough
	case 2:
		h += uint32(s[i+1]) << 8
		fallthrough
	case 1:
		h += uint32(s[i])
		h *= m
		h ^= (h >> r)
	}

	return h
}
