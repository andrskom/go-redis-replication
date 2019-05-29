package rdb

import (
	"bufio"
)

type Reader struct {
	r   *bufio.Reader
	len uint64
}

func NewReader(reader bufio.Reader, len uint64) *Reader {
	return &Reader{
		r:   &reader,
		len: len,
	}
}

func (r *Reader) ReadByte() (byte, error) {
	r.len--
	return r.r.ReadByte()
}

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.len -= uint64(n)
	return
}
