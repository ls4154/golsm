package util

import "testing"

func TestByteWiseComparator(t *testing.T) {
	cmp := bytewiseComparator{}

	if cmp.Compare([]byte("abcd"), []byte("abce")) >= 0 {
		t.Fatal("abcd >=  abce")
	}

	x := []byte("aaabbb")
	y := []byte("aaaddd")

	cmp.FindShortestSeparator(&x, y)

	if string(x) != "aaac" {
		t.Fatal("FindShortestSeparator failed")
	}

	z := []byte("dddddddd")
	cmp.FindShortSuccessor(&z)
	t.Log(string(z))
}
