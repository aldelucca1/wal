//go:build !windows && !darwin && !openbsd && !plan9
// +build !windows,!darwin,!openbsd,!plan9

package wal

import (
	"errors"
	"io"
	"os"
	"syscall"
)

const (
	BLOCK_SIZE = 4096 // Minimal block size
	ALIGN_SIZE = 4096 // Align size
	O_DIRECT   = syscall.O_DIRECT
)

// DirectFile wraps direct access to the underlying file
type DirectFile struct {
	file *os.File
}

// Close flushes the direct io buffer and closes the underlying file
func (d *DirectFile) Close() error {
	return d.file.Close()
}

// Write writes to the direct io buffer
func (d *DirectFile) Write(p []byte) (int, error) {
	return writeData(d.file, p)
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
	reader, err := New(file)
	if err != nil {
		return nil, err
	}
	return &DirectFile{file, reader}, err
}

// ReadFile reads the file at the supplied path
func ReadFile(path string) ([]byte, error) {

	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return readData(file, info)
}

func readData(fd *os.File, info os.FileInfo) ([]byte, error) {
	var buf []byte

	block := allocateBlock()
	blockSize := len(block)
	chunks := (int(info.Size()) / blockSize) + 1

	for i := 0; i < chunks; i++ {
		n, err := fd.ReadAt(block, int64(i*blockSize))

		if err != nil && err != io.EOF {
			return nil, err
		}

		buf = append(buf, block[:n]...)
	}

	return buf, nil
}

func writeData(fd *os.File, data []byte) error {
	block := allocateBlock()
	blockSize := len(block)
	dataSize := len(data)
	pointer := 0
	for {
		if pointer+blockSize >= dataSize {
			copy(block, data[pointer:])
		} else {
			copy(block, data[pointer:pointer+blockSize])
		}
		_, err := fd.Write(block)
		if err != nil {
			return err
		}
		pointer += blockSize
		if pointer >= dataSize {
			break
		}
	}
	return fd.Truncate(int64(dataSize))
}
