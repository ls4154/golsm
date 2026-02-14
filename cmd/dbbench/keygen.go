package main

func makeKey(i int) []byte {
	var b [16]byte
	n := i
	for pos := 15; pos >= 0; pos-- {
		b[pos] = byte('0' + (n % 10))
		n /= 10
	}
	return b[:]
}
