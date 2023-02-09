//go:build !windows && !darwin && !openbsd && !plan9
// +build !windows,!darwin,!openbsd,!plan9

package wal

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/brk0v/directio"
)

type DirectFile struct {
	file *os.File
	*directio.DirectIO
}

func (d *DirectFile) Close() error {
	if err := d.Flush(); err != nil {
		return err
	}
	return d.file.Close()
}

func (d *DirectFile) Read(p []byte) (n int, err error) {
	return 0, errors.New("unimplemented")
}

func (d *DirectFile) Write(p []byte) (n int, err error) {
	return d.DirectIO.Write(p)
}

func (d *DirectFile) Seek(offset int64, whence int) (int64, error) {
	return d.file.Seek(offset, whence)
}

func (d *DirectFile) Sync() error {
	return d.Flush()
}

func NewWriter(name string, flag int, perm os.FileMode) (File, error) {
	file, err := os.OpenFile(name, flag|syscall.O_DIRECT, perm)
	if err != nil {
		return nil, err
	}
	reader, err := directio.New(file)
	if err != nil {
		return nil, err
	}
	return &DirectFile{file, reader}, err
}

func NewReader(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag|syscall.O_DIRECT, perm)
}
