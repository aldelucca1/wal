package wal

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

const (
	BLOCK_SIZE = 4096 // Minimal block size
	ALIGN_SIZE = 0    // Align size
)

// NewWriter returns a direct io file to the specified path
func NewWriter(name string, flag int, perm os.FileMode) (File, error) {
	return openFile(name, flag, perm)
}

// openFile opens a file disabling file system caching
func openFile(name string, flag int, perm os.FileMode) (*os.File, error) {

	// Open the file as usual
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return file, err
	}

	// Set F_NOCACHE to avoid caching
	// F_NOCACHE    Turns data caching off/on. A non-zero value in arg turns
	//              data caching off.  A value of zero in arg turns data caching
	//              on.
	_, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, uintptr(file.Fd()), syscall.F_NOCACHE, 1)
	if e1 != 0 {
		file.Close()
		return nil, fmt.Errorf("failed to set F_NOCACHE: %s", e1)
	}
	return file, nil
}

// ReadFile reads the file at the supplied path
func ReadFile(path string) ([]byte, error) {

	file, err := openFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]byte, 0, 2*BLOCK_SIZE)
	block := allocateBlock()

	for {
		n, err := file.Read(block)
		data = append(data, block[:n]...)
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return nil, err
		}
	}
	return data, nil
}
