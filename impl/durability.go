package impl

import "github.com/ls4154/golsm/fs"

type dirSyncer interface {
	SyncDir(name string) error
}

func syncDirIfSupported(env fs.Env, name string) error {
	// On Unix-like systems, parent directory sync is needed to make file
	// creation/rename durable. Non-Unix environments such as Windows may not
	// need or expose an equivalent primitive, so this remains optional.
	syncer, ok := env.(dirSyncer)
	if !ok {
		return nil
	}
	return syncer.SyncDir(name)
}
