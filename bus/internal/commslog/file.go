// SPDX-License-Identifier: GPL-3.0-only

package commslog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type rotatingFile struct {
	path     string
	maxBytes int64
	maxFiles int
	file     *os.File
	lock     *os.File
	size     int64
}

func openRotatingFile(path string, maxBytes int64, maxFiles int) (*rotatingFile, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("communication log path must be absolute")
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return nil, errors.New("communication log directory must be a mode-0700 directory")
	}
	lock, err := openLogFile(path+".lock", unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("communication log is already owned: %w", err)
	}
	failed := true
	defer func() {
		if failed {
			_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
			_ = lock.Close()
		}
	}()
	for index := 0; index < maxFiles; index++ {
		name := rotatedName(path, index)
		if err = validateLogPath(name); err != nil {
			return nil, err
		}
	}
	file, err := openLogFile(path, unix.O_WRONLY|unix.O_CREAT|unix.O_APPEND|unix.O_CLOEXEC)
	if err != nil {
		return nil, err
	}
	info, err = file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	result := &rotatingFile{path: path, maxBytes: maxBytes, maxFiles: maxFiles, file: file, lock: lock, size: info.Size()}
	if result.size != 0 {
		if err = result.rotate(); err != nil {
			return nil, err
		}
	}
	failed = false
	return result, nil
}

func validateLogPath(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("communication log path %q must be a mode-0600 regular file", path)
	}
	return nil
}

func openLogFile(path string, flags int) (*os.File, error) {
	fd, err := unix.Open(path, flags|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, statErr := file.Stat()
	if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = file.Close()
		if statErr != nil {
			return nil, statErr
		}
		return nil, errors.New("communication log is not a mode-0600 regular file")
	}
	return file, nil
}

func rotatedName(path string, index int) string {
	if index == 0 {
		return path
	}
	return fmt.Sprintf("%s.%d", path, index)
}

func (f *rotatingFile) WriteRecord(raw []byte) error {
	if int64(len(raw)) > f.maxBytes {
		return errors.New("communication log record exceeds file limit")
	}
	if f.size != 0 && f.size+int64(len(raw)) > f.maxBytes {
		if err := f.rotate(); err != nil {
			return err
		}
	}
	written, err := f.file.Write(raw)
	if err == nil && written != len(raw) {
		err = io.ErrShortWrite
	}
	f.size += int64(written)
	return err
}

func (f *rotatingFile) rotate() error {
	if err := errors.Join(f.file.Sync(), f.file.Close()); err != nil {
		return err
	}
	f.file = nil
	if f.maxFiles == 1 {
		file, err := openLogFile(f.path, unix.O_WRONLY|unix.O_CREAT|unix.O_TRUNC|unix.O_CLOEXEC)
		if err != nil {
			return err
		}
		f.file, f.size = file, 0
		return nil
	}
	if err := os.Remove(rotatedName(f.path, f.maxFiles-1)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for index := f.maxFiles - 2; index >= 0; index-- {
		source := rotatedName(f.path, index)
		if err := validateLogPath(source); err != nil {
			return err
		}
		if _, err := os.Lstat(source); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		if err := os.Rename(source, rotatedName(f.path, index+1)); err != nil {
			return err
		}
	}
	file, err := openLogFile(f.path, unix.O_WRONLY|unix.O_CREAT|unix.O_TRUNC|unix.O_CLOEXEC)
	if err != nil {
		return err
	}
	f.file, f.size = file, 0
	return nil
}

func (f *rotatingFile) Close() error {
	var err error
	if f.file != nil {
		err = errors.Join(f.file.Sync(), f.file.Close())
		f.file = nil
	}
	if f.lock != nil {
		err = errors.Join(err, unix.Flock(int(f.lock.Fd()), unix.LOCK_UN), f.lock.Close())
		f.lock = nil
	}
	return err
}
