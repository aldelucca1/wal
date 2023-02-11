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

var ErrNotSetDirectIO = errors.New("O_DIRECT flag is absent")

func fcntl(fd uintptr, cmd uintptr, arg uintptr) (uintptr, error) {
	r0, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(cmd), uintptr(arg))
	if e1 != 0 {
		return 0, e1
	}
	return r0, nil
}

func checkDirectIO(fd uintptr) error {
	flags, err := fcntl(fd, syscall.F_GETFL, 0)
	if err != nil {
		return err
	}
	if (flags & O_DIRECT) == O_DIRECT {
		return nil
	}
	return ErrNotSetDirectIO
}

func setDirectIO(fd uintptr, dio bool) error {
	flag, err := fcntl(fd, syscall.F_GETFL, 0)
	if err != nil {
		return err
	}
	if dio {
		flag |= O_DIRECT
	} else {
		flag &^= O_DIRECT
	}
	_, err = fcntl(fd, syscall.F_SETFL, flag)
	return err
}

// DirectFile wraps direct access to the underlying file
type DirectFile struct {
	file *os.File
	buf  *DirectIO
}

// Close flushes the direct io buffer and closes the underlying file
func (d *DirectFile) Close() error {
	defer d.file.Close()
	return d.buf.Flush()
}

// Write writes to the direct io buffer
func (d *DirectFile) Write(p []byte) (int, error) {
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
	reader, err := New(file)
	if err != nil {
		return nil, err
	}
	return &DirectFile{file, reader}, err
}

// DirectIO bypasses page cache.
type DirectIO struct {
	f   *os.File
	buf []byte
	n   int
	err error
}

// New returns a new DirectIO writer with default buffer size.
func New(f *os.File) (*DirectIO, error) {
	if err := checkDirectIO(f.Fd()); err != nil {
		return nil, err
	}
	return &DirectIO{
		buf: allocateBlock(),
		f:   f,
	}, nil
}

// flush writes buffered data to the underlying os.File.
func (d *DirectIO) flush() error {
	if d.err != nil {
		return d.err
	}
	if d.n == 0 {
		return nil
	}
	n, err := d.f.Write(d.buf[0:d.n])
	if n < d.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < d.n {
			copy(d.buf[0:d.n-n], d.buf[n:d.n])
		}
	}
	d.n -= n
	return err
}

// Flush writes buffered data to the underlying file.
func (d *DirectIO) Flush() error {
	fd := d.f.Fd()

	// Disable direct IO
	err := setDirectIO(fd, false)
	if err != nil {
		return err
	}

	// Making write without alignment
	err = d.flush()
	if err != nil {
		return err
	}

	// Enable direct IO back
	return setDirectIO(fd, true)
}

// Available returns how many bytes are unused in the buffer.
func (d *DirectIO) Available() int { return len(d.buf) - d.n }

// Buffered returns the number of bytes that have been written into the current buffer.
func (d *DirectIO) Buffered() int { return d.n }

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (d *DirectIO) Write(p []byte) (nn int, err error) {
	// Write more than available in buffer.
	for len(p) >= d.Available() && d.err == nil {
		var n int
		// Check if buffer is zero size for direct and zero copy write to Writer.
		// Here we also check the p memory alignment.
		// If buffer p is not aligned, than write through buffer d.buf and flush.
		if d.Buffered() == 0 && alignment(p, ALIGN_SIZE) == 0 {

			// Large write, empty buffer.
			if (len(p) % BLOCK_SIZE) == 0 {
				// Data and buffer p are already aligned to block size.
				// So write directly from p to avoid copy.
				n, d.err = d.f.Write(p)
			} else {
				// Data needs alignment. Buffer alredy aligned.

				// Align data
				l := len(p) & -BLOCK_SIZE

				// Write directly from p to avoid copy.
				var nl int
				nl, d.err = d.f.Write(p[:l])

				// Save other data to buffer.
				n = copy(d.buf[d.n:], p[l:])
				d.n += n

				// written and buffered data
				n += nl
			}
		} else {
			n = copy(d.buf[d.n:], p)
			d.n += n
			d.flush()
		}
		nn += n
		p = p[n:]
	}

	if d.err != nil {
		return nn, d.err
	}

	n := copy(d.buf[d.n:], p)
	d.n += n
	nn += n

	return nn, nil
}

// ReadFile reads the file at the supplied path
func ReadFile(path string) ([]byte, error) {

	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECT, 0)
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
