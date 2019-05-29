package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
)

type ZipMap struct {
	r ByteReader
	rowr ByteReader
}

func NewZipMap(r ByteReader) *ZipMap {
	return &ZipMap{rowr: r}
}

func (z *ZipMap) Decode() (map[string]string, error) {
	data, err := NewStringDecoder(z.rowr).DecodeToBytes()
	if err != nil {
		return nil, err
	}
	log.Println(data)
	log.Println(string(data))

	z.r = bufio.NewReader(bytes.NewBuffer(data))

	test := make([]byte, 8)
	io.ReadFull(z.r, test)
	if _, err := z.decodeLength(); err != nil {
		return nil, err
	}


	res := make(map[string]string)
	for {
		b, isEnd, err := z.decodeHeader()
		if err != nil {
			return nil, err
		}
		if isEnd {
			return res, nil
		}
		k, err := z.decodeKey(b)
		if err != nil {
			return nil, err
		}

		v, err := z.decodeVal()
		if err != nil {
			return nil, err
		}

		res[k] = v
	}
}

type length uint8

func (l length) isUsable() bool {
	return l < 254
}

func (z *ZipMap) decodeLength() (length, error) {
	b, err := z.r.ReadByte()
	if err != nil {
		return 0, err
	}
	return length(b), nil
}

func (z *ZipMap) decodeHeader() (byte, bool, error) {
	b, err := z.r.ReadByte()
	if err != nil {
		return 0, false, nil
	}
	if b == 0xFF {
		return b, true, nil
	}
	return b, false, nil
}

func (z *ZipMap) decodeLen() (uint32, error) {
	b, err := z.r.ReadByte()
	if err != nil {
		return 0, nil
	}
	return z.parseLen(b)
}

func (z *ZipMap) parseLen(b byte) (uint32, error) {
	switch b {
	case 253:
		bytes := make([]byte, 4)
		if _, err := io.ReadFull(z.r, bytes); err != nil {
			return 0, err
		}

		return binary.BigEndian.Uint32(bytes), nil
	case 254, 255:
		return 0, errors.New("unexpected len of entry in zip map")
	default:
		return uint32(b), nil
	}
}

func (z *ZipMap) decodeEnd() error {
	b, err := z.r.ReadByte()
	if err != nil {
		return err
	}
	if b != 0xFF {
		return errors.New("unexpected end byte")
	}
	return nil
}

func (z *ZipMap) decodeKey(b byte) (string, error) {
	l, err := z.parseLen(b)
	if err != nil {
		return "", nil
	}

	res := make([]byte, l)
	if _, err := io.ReadFull(z.r, res); err != nil {
		return "", err
	}

	return string(res), nil
}

func (z *ZipMap) decodeVal() (string, error) {
	l, err := z.decodeLen()
	if err != nil {
		return "", err
	}
	// len of free bytes after value
	freeLen, err := z.r.ReadByte()
	if err != nil {
		return "", err

	}
	// read val
	res := make([]byte, l)
	if _, err := io.ReadFull(z.r, res); err != nil {
		return "", err
	}
	// read free bytes
	trash := make([]byte, freeLen)
	if _, err := io.ReadFull(z.r, trash); err != nil {
		return "", err
	}

	return string(res), nil
}
