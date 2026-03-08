package util

import (
	"io"
	stdlog "log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ls4154/golsm/fs"
)

const DefaultLogFileName = "info_log"

type FileLogger struct {
	logger *stdlog.Logger
	file   *rotatingFile
}

type rotatingFile struct {
	mu         sync.Mutex
	env        fs.Env
	dir        string
	activePath string
	maxSize    uint64
	maxFiles   int
	file       fs.WritableFile
	size       uint64
}

// OpenFileLogger opens a file-based logger that writes to filename.
// If maxSize > 0, the log file is rotated when it exceeds maxSize bytes;
// the old file is renamed with a timestamp suffix and a new file is created.
// If maxFiles > 0, old rotated files beyond that count are deleted after each rotation.
// Each write is flushed immediately to ensure durability.
func OpenFileLogger(env fs.Env, filename string, maxSize uint64, maxFiles int) (*FileLogger, error) {
	f, err := openRotatingFile(env, filename, maxSize, maxFiles)
	if err != nil {
		return nil, err
	}
	return &FileLogger{
		logger: stdlog.New(f, "", stdlog.LstdFlags|stdlog.Lmicroseconds),
		file:   f,
	}, nil
}

// OpenFileLoggerDir opens a file-based logger in dirname using the default
// log file name (DefaultLogFileName).
func OpenFileLoggerDir(env fs.Env, dirname string, maxSize uint64, maxFiles int) (*FileLogger, error) {
	return OpenFileLogger(env, filepath.Join(dirname, DefaultLogFileName), maxSize, maxFiles)
}

func (l *FileLogger) Printf(format string, v ...any) {
	l.logger.Printf(format, v...)
}

func (l *FileLogger) Close() error {
	return l.file.Close()
}

func openRotatingFile(env fs.Env, filename string, maxSize uint64, maxFiles int) (*rotatingFile, error) {
	size, err := env.GetFileSize(filename)
	if err != nil {
		if !fs.IsNotExist(err) {
			return nil, WrapIOError(err, "stat info log %s", filename)
		}
		size = 0
	}

	file, err := env.NewAppendableFile(filename)
	if err != nil {
		return nil, WrapIOError(err, "open info log %s", filename)
	}

	f := &rotatingFile{
		env:        env,
		dir:        filepath.Dir(filename),
		activePath: filename,
		maxSize:    maxSize,
		maxFiles:   maxFiles,
		file:       file,
		size:       size,
	}
	f.pruneArchives()
	return f, nil
}

func (f *rotatingFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return 0, WrapIOError(io.ErrClosedPipe, "write info log %s", f.activePath)
	}

	if f.maxSize > 0 && f.size > 0 && f.size+uint64(len(p)) > f.maxSize {
		if err := f.rotate(); err != nil {
			return 0, err
		}
	}

	n, err := f.file.Write(p)
	if err != nil {
		return n, WrapIOError(err, "write info log %s", f.activePath)
	}
	if n < len(p) {
		return n, WrapIOError(io.ErrShortWrite, "write info log %s", f.activePath)
	}
	f.size += uint64(n)

	if err := f.file.Flush(); err != nil {
		return n, WrapIOError(err, "flush info log %s", f.activePath)
	}
	return n, nil
}

func (f *rotatingFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil
	}
	err := f.file.Close()
	f.file = nil
	if err != nil {
		return WrapIOError(err, "close info log %s", f.activePath)
	}
	return nil
}

func (f *rotatingFile) rotate() error {
	if err := f.file.Close(); err != nil {
		return WrapIOError(err, "close info log %s before rotate", f.activePath)
	}

	archivePath := f.nextArchivePath()
	if err := f.env.RenameFile(f.activePath, archivePath); err != nil {
		return WrapIOError(err, "rotate info log %s", f.activePath)
	}

	file, err := f.env.NewAppendableFile(f.activePath)
	if err != nil {
		return WrapIOError(err, "reopen info log %s", f.activePath)
	}
	f.file = file
	f.size = 0

	f.pruneArchives()
	return nil
}

func (f *rotatingFile) pruneArchives() {
	if f.maxFiles <= 0 {
		return
	}

	children, err := f.env.GetChildren(f.dir)
	if err != nil {
		return
	}

	base := filepath.Base(f.activePath) + "."
	var archives []string
	for _, name := range children {
		if strings.HasPrefix(name, base) {
			archives = append(archives, name)
		}
	}

	sort.Strings(archives)

	if len(archives) <= f.maxFiles {
		return
	}
	for _, name := range archives[:len(archives)-f.maxFiles] {
		_ = f.env.RemoveFile(filepath.Join(f.dir, name))
	}
}

func (f *rotatingFile) nextArchivePath() string {
	base := f.activePath + "." + time.Now().UTC().Format("20060102T150405Z")
	if !f.env.FileExists(base) {
		return base
	}
	for i := 1; ; i++ {
		candidate := base + "." + strconv.Itoa(i)
		if !f.env.FileExists(candidate) {
			return candidate
		}
	}
}
