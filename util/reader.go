package util

import (
	"io"
)

type ByteReader interface {
	ReadByte() (byte, error)
	io.Reader
}

type LengthReader struct {
	len uint64
	ByteReader
}

func NewLengthReader(len uint64, byteReader ByteReader) *LengthReader {
	return &LengthReader{len: len, ByteReader: byteReader}
}

func (r *LengthReader) ReadByte() (byte, error) {
	b, err := r.ByteReader.ReadByte()
	if err != nil {
		return 0x0, err
	}
	r.len = r.len - 1
	return b, nil
}

func (r *LengthReader) Read(p []byte) (int,error) {
	n, err := r.ByteReader.Read(p)
	r.len = r.len - uint64(n)
	return n, err
}
