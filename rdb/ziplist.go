package rdb

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	ErrUnexpectedEndByte = errors.New("unexpected end byte")
)

type Entries struct {
	size uint32
	tail uint32
	len  uint16
	list []*Entry
}

func NewEntries(size uint32, tail uint32, len uint16) *Entries {
	return &Entries{
		size: size,
		tail: tail,
		len:  len,
		list: make([]*Entry, 0, len),
	}
}

type Ziplist struct {
	r ByteReader
}

func NewZiplist(r ByteReader) *Ziplist {
	return &Ziplist{r: r}
}

func (z *Ziplist) Decode() (*Entries, error) {
	size, err := z.decodeBytes()
	if err != nil {
		return nil, err
	}
	tail, err := z.decodeTail()
	if err != nil {
		return nil, err
	}
	eLen, err := z.decodeLen()
	if err != nil {
		return nil, err
	}
	entries := NewEntries(size, tail, eLen)

	entryDecoder := NewZipEntry(z.r)

	for i := uint16(0); i < eLen; i++ {
		entry, err := entryDecoder.Decode()
		if err != nil {
			return nil, err
		}
		entries.list = append(entries.list, entry)
	}
	if err := z.decodeEnd(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (z *Ziplist) decodeBytes() (uint32, error) {
	bytes := make([]byte, 4)
	_, err := io.ReadFull(z.r, bytes)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(bytes), nil
}

func (z *Ziplist) decodeTail() (uint32, error) {
	bytes := make([]byte, 4)
	_, err := io.ReadFull(z.r, bytes)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(bytes), nil
}

func (z *Ziplist) decodeLen() (uint16, error) {
	bytes := make([]byte, 2)
	_, err := io.ReadFull(z.r, bytes)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(bytes), nil
}

func (z *Ziplist) decodeEnd() error {
	b, err := z.r.ReadByte()
	if err != nil {
		return err
	}
	if b != 0xFF {
		return ErrUnexpectedEndByte
	}
	return nil
}

