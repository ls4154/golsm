package main

import "math/rand"

const randomDataSize = 1 << 20 // 1MB

type randomValueGenerator struct {
	data []byte
	pos  int
}

func newRandomValueGenerator(compressionRatio float64, seed int64) *randomValueGenerator {
	if compressionRatio <= 0 {
		compressionRatio = 0.01
	}
	r := rand.New(rand.NewSource(seed))
	data := make([]byte, 0, randomDataSize+128)
	for len(data) < randomDataSize {
		piece := makeCompressibleBytes(r, compressionRatio, 100)
		data = append(data, piece...)
	}
	return &randomValueGenerator{
		data: data,
		pos:  0,
	}
}

func (g *randomValueGenerator) Generate(n int) []byte {
	if n <= 0 {
		return nil
	}
	if g.pos+n > len(g.data) {
		g.pos = 0
	}
	if n > len(g.data) {
		n = len(g.data)
	}
	v := g.data[g.pos : g.pos+n]
	g.pos += n
	return v
}

func makeCompressibleBytes(r *rand.Rand, compressionRatio float64, n int) []byte {
	if n <= 0 {
		return nil
	}
	raw := int(float64(n) * compressionRatio)
	if raw < 1 {
		raw = 1
	}
	if raw > n {
		raw = n
	}
	fragment := make([]byte, raw)
	for i := range fragment {
		fragment[i] = byte(' ' + r.Intn(95))
	}
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = fragment[i%raw]
	}
	return out
}
