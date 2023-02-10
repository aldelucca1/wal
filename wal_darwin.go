package wal

import (
	"fmt"
	"os"
	"syscall"
)

// NewWriter returns a direct io file to the specified path
func NewWriter(name string, flag int, perm os.FileMode) (File, error) {
	return openFile(name, flag, perm)
}

// NewReader returns a direct io file to the specified path
func NewReader(name string, flag int, perm os.FileMode) (File, error) {
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
