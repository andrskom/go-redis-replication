package rdb

import (
	"errors"
	"io"
)

// DecodeMagic first bytes and return RDB version
// Docs https://rdb.fnordig.de/file_format.html#magic-number
func DecodeMagic(r ByteReader) (string, error) {
	// 9 is a magic number equal number of bytes in RDB header
	// looks like `REDIS####`, where # is bytes with encoded rdb version
	bytes := make([]byte, 9)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return "", err
	}
	// more useful use string, because if u can't use == for []byte
	// and comparison happened only one time in one RDB file
	if string(bytes[0:5]) != "REDIS" {
		return "", errors.New("unexpected magic header format")
	}
	return string(bytes[5:]), nil
}
