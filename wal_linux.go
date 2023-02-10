//go:build !windows && !darwin && !openbsd && !plan9
// +build !windows,!darwin,!openbsd,!plan9

package wal

import (
	"errors"
	"os"
	"syscall"

	"github.com/brk0v/directio"
)

// DirectFile wraps direct access to the underlying file
type DirectFile struct {
	file *os.File
	buf  *directio.DirectIO
}

// Close flushes the direct io buffer and closes the underlying file
func (d *DirectFile) Close() error {
	if err := d.buf.Flush(); err != nil {
		return err
	}
	return d.file.Close()
}

// Read is unimplemented
func (d *DirectFile) Read(p []byte) (n int, err error) {
	return 0, errors.New("unimplemented")
}

// Write writes to the direct io buffer
func (d *DirectFile) Write(p []byte) (n int, err error) {
	return d.buf.Write(p)
}

// Seek seeks the underlying file to the specified location
func (d *DirectFile) Seek(offset int64, whence int) (int64, error) {
	return d.file.Seek(offset, whence)
}

// Sync flushes the direct io buffer
func (d *DirectFile) Sync() error {
	return d.buf.Flush()
}

// NewWriter creates a new direct io file with the specified name, flags, and
// permissions
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

// NewReader creates a new direct io file with the specified name, flags, and
// permissions
func NewReader(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag|syscall.O_DIRECT, perm)
}
