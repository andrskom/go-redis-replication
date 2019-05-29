package rdb

import (
	"encoding/binary"
	"errors"
	"io"
	"strconv"
)


type EntryType uint8

const (
	EntryTypeString = iota + 1
	EntryTypeInt
)

type Entry struct {
	lengthPrevEntry uint32
	t               EntryType
	strVal          string
	intVal          uint64
}

func (e *Entry) String() string {
	switch e.t {
	case EntryTypeInt:
		return strconv.FormatUint(e.intVal, 10)
	case EntryTypeString:
		return e.strVal
	}
	return ""
}

type ZipEntry struct {
	r ByteReader
}

func NewZipEntry(r ByteReader) *ZipEntry {
	return &ZipEntry{r: r}
}

func (z *ZipEntry) Decode() (*Entry, error) {
	prevLen, err := z.decodeLengthPrevEntry()
	if err != nil {
		return nil, err
	}
	eVal, err := z.decodeSpecialFlagAndBin()
	if err != nil {
		return nil, err
	}
	eVal.lengthPrevEntry = prevLen
	return eVal, nil
}

func (z *ZipEntry) decodeLengthPrevEntry() (uint32, error) {
	b, err := z.r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch b {
	case 0:
		return 0, nil
	case 254:
		bytes := make([]byte, 4)
		if _, err := io.ReadFull(z.r, bytes); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(bytes), nil
	case 255:
		return 0, errors.New("unexpected ziplist entry's length")
	default:
		return uint32(b), nil
	}
}

func (z *ZipEntry) decodeSpecialFlagAndBin() (*Entry, error) {
	b, err := z.r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b >> 6 {
	// String value with length less than or equal to 63 bytes (6 bits)
	case 0x0:
		val := make([]byte, b)
		if _, err := io.ReadFull(z.r, val); err != nil {
			return nil, err
		}
		return &Entry{t: EntryTypeString, strVal: string(val)}, nil
	// String value with length less than or equal to 16383 bytes (14 bits)
	case 0x1:
		followB, err := z.r.ReadByte()
		if err != nil {
			return nil, err
		}
		val := make([]byte, (uint64(b&0x3f)<<8)|uint64(followB))
		if _, err := io.ReadFull(z.r, val); err != nil {
			return nil, err
		}
		return &Entry{t: EntryTypeString, strVal: string(val)}, nil
	// Next 4 byte contain an unsigned int. String value with length greater than or equal to 16384 bytes
	case 0x2:
		bytes := make([]byte, 4)
		if _, err := io.ReadFull(z.r, bytes); err != nil {
			return nil, err
		}
		val := make([]byte, binary.BigEndian.Uint32(bytes))
		if _, err := io.ReadFull(z.r, val); err != nil {
			return nil, err
		}
		return &Entry{t: EntryTypeString, strVal: string(val)}, nil
	}

	switch b >> 4 {
	// Integer encoded as 16 bit signed (2 bytes)
	case 0xC:
		bytes := make([]byte, 2)
		if _, err := io.ReadFull(z.r, bytes); err != nil {
			return nil, err
		}
		return &Entry{t: EntryTypeInt, intVal: uint64(binary.LittleEndian.Uint16(bytes))}, nil
	// Integer encoded as 32 bit signed (4 bytes)
	case 0xD:
		bytes := make([]byte, 4)
		if _, err := io.ReadFull(z.r, bytes); err != nil {
			return nil, err
		}
		return &Entry{t: EntryTypeInt, intVal: uint64(binary.LittleEndian.Uint32(bytes))}, nil
	// Integer encoded as 64 bit signed (8 bytes)
	case 0xE:
		bytes := make([]byte, 8)
		if _, err := io.ReadFull(z.r, bytes); err != nil {
			return nil, err
		}
		return &Entry{t: EntryTypeInt, intVal: binary.LittleEndian.Uint64(bytes)}, nil
	// 	Integer encoded as 24 bit signed (3 bytes)
	case 0xF:
		bytes := make([]byte, 8)
		if _, err := io.ReadFull(z.r, bytes); err != nil {
			return nil, err
		}
		return &Entry{t: EntryTypeInt, intVal: binary.LittleEndian.Uint64(bytes)}, nil
	}

	return nil, errors.New("unexpected special flag")
}
