package main

import "fmt"

type benchSpec struct {
	name             string
	freshDBByDefault bool
	useReads         bool
}

func benchSpecFor(name string) (benchSpec, error) {
	switch name {
	case "fillseq":
		return benchSpec{name: name, freshDBByDefault: true, useReads: false}, nil
	case "fillrandom":
		return benchSpec{name: name, freshDBByDefault: true, useReads: false}, nil
	case "overwrite":
		return benchSpec{name: name, freshDBByDefault: false, useReads: false}, nil
	case "readseq":
		return benchSpec{name: name, freshDBByDefault: false, useReads: true}, nil
	case "readreverse":
		return benchSpec{name: name, freshDBByDefault: false, useReads: true}, nil
	case "readrandom":
		return benchSpec{name: name, freshDBByDefault: false, useReads: true}, nil
	default:
		return benchSpec{}, fmt.Errorf("unknown benchmark %q", name)
	}
}
