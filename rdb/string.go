package rdb

import (
	"encoding/binary"
	"errors"
	"io"

	lzf "github.com/zhuyie/golzf"
)

// DecodeLengthPrefixedString by length
// Docs https://rdb.fnordig.de/file_format.html#string-encoding
func DecodeLengthPrefixedString(r ByteReader, len uint32) (string, error) {
	res, err := DecodeLengthPrefixedBytes(r, len)
	if err != nil {
		return "", err
	}
	return string(res), nil
}
func DecodeLengthPrefixedBytes(r ByteReader, len uint32) ([]byte, error) {
	bytes := make([]byte, len)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}
	return bytes, nil
}

// DecodeEncodedStringInt return encoded int in string
func DecodeEncodedStringInt(r ByteReader, length uint32) (uint32, error) {
	bytes := make([]byte, length)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return 0, err
	}
	val := append(make([]byte, 4-length), bytes...)
	return binary.BigEndian.Uint32(val), nil
}

type LengthPrefixedStringDecoder struct {
	r             ByteReader
	lengthDecoder func(r ByteReader) (*Length, error)
	stringDecoder func(r ByteReader, len uint32) (string, error)
}

func NewLengthPrefixedStringDecoder(r ByteReader) *LengthPrefixedStringDecoder {
	return &LengthPrefixedStringDecoder{
		r:             r,
		lengthDecoder: DecodeLength,
		stringDecoder: DecodeLengthPrefixedString,
	}
}

func (d *LengthPrefixedStringDecoder) Decode() (string, error) {
	length, err := d.lengthDecoder(d.r)
	if err != nil {
		return "", err
	}
	if length.IsEncodedString() {
		return "", errors.New("unexpected length type")
	}
	return d.stringDecoder(d.r, length.GetLength())
}

func DecodeEncodedBytes(r ByteReader, len uint32, decodedLen uint32) ([]byte, error) {
	data := make([]byte, len)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	res := make([]byte, decodedLen)
	// TODO is not complete
	lzf.Decompress(data, res)
	return res, nil
}

type StringDecoder struct {
	r ByteReader
}

func NewStringDecoder(r ByteReader) *StringDecoder {
	return &StringDecoder{r: r}
}

func (d *StringDecoder) DecodeToBytes() ([]byte, error) {
	length, err := DecodeLength(d.r)
	if err != nil {
		return nil, err
	}
	if length.IsEncodedString() {
		return DecodeEncodedBytes(d.r, length.GetLength(), length.GetVerifyLength())
	}
	return DecodeLengthPrefixedBytes(d.r, length.GetLength())
}
