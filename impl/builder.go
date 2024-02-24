package impl

import (
	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/table"
)

func BuildTable(dbname string, env db.Env, iter db.Iterator,
	icmp *InternalKeyComparator, options *db.Options, meta *FileMetaData,
) error {
	iter.SeekToFirst()
	if !iter.Valid() {
		return nil
	}

	fname := TableFileName(dbname, meta.number)
	f, err := env.NewWritableFile(fname)
	if err != nil {
		return err
	}

	defer func() {
		if f != nil {
			f.Close()
		}
		if err != nil || meta.size <= 0 {
			env.RemoveFile(fname)
		}
	}()

	builder := table.NewTableBuilder(f, icmp,
		options.BlockSize, options.Compression, options.BlockRestartInterval)

	meta.smallest = append([]byte(nil), iter.Key()...)
	var key []byte
	for ; iter.Valid(); iter.Next() {
		key = iter.Key()
		builder.Add(key, iter.Value())
	}
	meta.largest = append([]byte(nil), key...)

	err = builder.Finish()
	if err != nil {
		return err
	}

	meta.size = builder.FileSize()

	err = f.Sync()
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	f = nil

	// TODO check table cache

	if iter.Error() != nil {
		err = iter.Error()
		return err
	}

	return nil
}
