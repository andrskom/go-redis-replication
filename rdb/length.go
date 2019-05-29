package rdb

import (
	"encoding/binary"
	"errors"
	"io"
)

type LengthType int

const (
	LengthType6Bits LengthType = iota + 1
	LengthType14Bits
	LengthType4Bytes
	LengthTypeEncodedStringInt
	LengthTypeEncodedStringCompressedStr
)

type Length struct {
	t            LengthType
	length       uint32
	verifyLength uint32
}

func (l *Length) IsEncodedString() bool {
	return l.t == LengthTypeEncodedStringInt || l.t == LengthTypeEncodedStringCompressedStr
}

func (l *Length) GetType() LengthType {
	return l.t
}

func (l *Length) GetLength() uint32 {
	return l.length
}

func (l *Length) CheckVerifyLength(expected uint32) error {
	if expected != l.verifyLength {
		return errors.New("verify length for encoded string isn't expected")
	}
	return nil
}

func (l *Length) GetVerifyLength() uint32 {
	return l.verifyLength
}

var ErrLengthEncodedStringBadLength = errors.New("length of encoded string is bad")

// DecodeLength length from byte reader
// Docs https://rdb.fnordig.de/file_format.html#length-encoding
func DecodeLength(r ByteReader) (*Length, error) {
	return decodeLength(r, false)
}

func decodeLength(r ByteReader, isEncodedLengthRead bool) (*Length, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	prefix := b & 0xC0 >> 6
	switch prefix {
	case 0x00: // 00 prefix
		return &Length{
			t:      LengthType6Bits,
			length: uint32(b),
		}, nil
	case 0x01: // 01 prefix
		b2, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		return &Length{
			t:      LengthType14Bits,
			length: uint32(b&0x3F)<<8 | uint32(b2),
		}, nil
	case 0x2: // 10 prefix
		bytes := make([]byte, 4)
		if _, err := io.ReadFull(r, bytes); err != nil {
			return nil, err
		}
		return &Length{
			t:      LengthType4Bytes,
			length: binary.BigEndian.Uint32(bytes),
		}, nil
	case 0x3: // 11 prefix
		if isEncodedLengthRead {
			return nil, ErrLengthEncodedStringBadLength
		}
		format := b & 0x3F
		switch format {
		case 0x00, 0x01:
			return &Length{
				t:      LengthTypeEncodedStringInt,
				length: uint32(format) + 1,
			}, nil
		case 0x02:
			return &Length{
				t:      LengthTypeEncodedStringInt,
				length: 4,
			}, nil
		case 0x03:
			length, err := decodeLength(r, true)
			if err != nil {
				return nil, err
			}
			verifyLength, err := decodeLength(r, true)
			if err != nil {
				return nil, err
			}
			return &Length{
				t:            LengthTypeEncodedStringCompressedStr,
				length:       length.length,
				verifyLength: verifyLength.length,
			}, nil
		}
		return nil, errors.New("unexpected encoded string format")
	}
	return nil, errors.New("unexpected prefix of length") // it will not can happen
}
