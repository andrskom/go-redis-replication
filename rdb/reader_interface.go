package rdb

import (
	"io"
)

type ByteReader interface {
	ReadByte() (byte, error)
	io.Reader
}
