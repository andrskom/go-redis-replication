package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"time"

	abc "github.com/zhuyie/golzf"
)

type ValueType byte

const (
	OpCodeKeyExpirySecond       = 0xFD
	OpCodeKeyExpiryMilliseconds = 0xFC

	ValueTypeString           ValueType = 0
	ValueTypeList             ValueType = 1
	ValueTypeSet              ValueType = 2
	ValueTypeSortedSet        ValueType = 3
	ValueTypeHash             ValueType = 4
	ValueTypeZipmap           ValueType = 9
	ValueTypeZiplist          ValueType = 10
	ValueTypeIntset           ValueType = 11
	ValueTypeSortedSetZiplist ValueType = 12
	ValueTypeHashmapZiplist   ValueType = 13
	ValueTypeListQuicklist    ValueType = 14
)

type Row struct {
	Expiry       *time.Time
	Key          string
	Type         ValueType
	ValString    string
	ValStringSet []string
	ValIntSet    []int
	ValMap       map[string]string
}

func (r *Row) hasTTL() bool {
	return r.Expiry != nil
}

type RowDecoder struct {
	r ByteReader
}

func (d *RowDecoder) Decode() (*Row, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return nil, err
	}
	row := &Row{}
	switch b {
	case OpCodeKeyExpirySecond:
		bytes := make([]byte, 4)
		if _, err := io.ReadFull(d.r, bytes); err != nil {
			return nil, err
		}
		row.Expiry = new(time.Time)
		*row.Expiry = time.Unix(int64(binary.LittleEndian.Uint32(bytes)), 0)

	case OpCodeKeyExpiryMilliseconds:
		bytes := make([]byte, 8)
		if _, err := io.ReadFull(d.r, bytes); err != nil {
			return nil, err
		}
		row.Expiry = new(time.Time)
		*row.Expiry = time.Unix(int64(binary.LittleEndian.Uint64(bytes))/1000, int64(binary.BigEndian.Uint64(bytes))%1000)

	}

	valType := b
	if row.hasTTL() {
		valType, err = d.r.ReadByte()
		if err != nil {
			return nil, err
		}
	}
	row.Type = ValueType(valType)

	// lpd := NewLengthPrefixedStringDecoder(d.r)
	// key, err := lpd.Decode()
	// if err != nil {
	// 	return nil, err
	// }
	keyLen, err := DecodeLength(d.r)
	if err != nil {
		return nil, err
	}
	if keyLen.IsEncodedString() {
		data := make([]byte, keyLen.GetLength())
		if _, err := io.ReadFull(d.r, data); err != nil {
			return nil, err
		}

		res := make([]byte, keyLen.GetVerifyLength())
		abc.Decompress(data, res)
		row.Key = string(res)
		// if err := keyLen.CheckVerifyLength(uint32(len(k))); err != nil {
		// 	return nil, err
		// }
	} else {
		row.Key, err = DecodeLengthPrefixedString(d.r, keyLen.GetLength())
		if err != nil {
			return nil, err
		}
	}

	switch row.Type {
	case ValueTypeString:
		var v string

		valLen, err := DecodeLength(d.r)
		if err != nil {
			return nil, err
		}
		if valLen.IsEncodedString() {
			data := make([]byte, valLen.GetLength())
			if _, err := d.r.Read(data); err != nil {
				return nil, err
			}
			res := make([]byte, valLen.GetVerifyLength())
			abc.Decompress(data, res)
			v = string(res)
			// if err := valLen.CheckVerifyLength(uint32(len(v))); err != nil {
			// 	return nil, err
			// }
		} else {
			v, err = DecodeLengthPrefixedString(d.r, valLen.GetLength())
			if err != nil {
				return nil, err
			}
		}
		row.ValString = v
	case ValueTypeList:
		length, err := DecodeLength(d.r)
		if err != nil {
			return nil, err
		}
		if length.IsEncodedString() {
			return nil, errors.New("unexpected ytpe of length for list")
		}
		row.ValStringSet = make([]string, 0, length.GetLength())
		for i := uint32(0); i < length.GetLength(); i++ {
			val, err := NewLengthPrefixedStringDecoder(d.r).Decode()
			if err != nil {
				return nil, err
			}
			row.ValStringSet = append(row.ValStringSet, val)
		}
	case ValueTypeSet:
	case ValueTypeSortedSet:
	case ValueTypeHash:
		row.ValMap = make(map[string]string)
		length, err := DecodeLength(d.r)
		if err != nil {
			return nil, err
		}
		lengthVal := length.GetLength()
		for lengthVal > 0 {
			lengthVal--

			var k, v string

			keyLen, err := DecodeLength(d.r)
			if err != nil {
				return nil, err
			}
			if keyLen.IsEncodedString() {
				data := make([]byte, keyLen.GetLength())
				if _, err := d.r.Read(data); err != nil {
					return nil, err
				}

				res := make([]byte, keyLen.GetVerifyLength())
				abc.Decompress(data, res)
				k = string(res)
				// if err := keyLen.CheckVerifyLength(uint32(len(k))); err != nil {
				// 	return nil, err
				// }
			} else {
				k, err = DecodeLengthPrefixedString(d.r, keyLen.GetLength())
				if err != nil {
					return nil, err
				}
			}

			valLen, err := DecodeLength(d.r)
			if err != nil {
				return nil, err
			}
			if valLen.IsEncodedString() {
				data := make([]byte, valLen.GetLength())
				n, err := io.ReadFull(d.r, data)
				if err != nil {
					return nil, err
				}
				_ = n
				res := make([]byte, valLen.GetVerifyLength())
				abc.Decompress(data, res)
				v = string(res)
				// if err := valLen.CheckVerifyLength(uint32(len(v))); err != nil {
				// 	return nil, err
				// }
			} else {
				v, err = DecodeLengthPrefixedString(d.r, valLen.GetLength())
				if err != nil {
					return nil, err
				}
			}

			row.ValMap[k] = v
		}
	case ValueTypeZipmap:
	case ValueTypeZiplist:
	case ValueTypeIntset:
	case ValueTypeSortedSetZiplist:
	case ValueTypeHashmapZiplist:
		data, err := NewStringDecoder(d.r).DecodeToBytes()
		if err != nil {
			return nil, err
		}
		l, err := NewZiplist(bufio.NewReader(bytes.NewBuffer(data))).Decode()
		if err != nil {
			if err != ErrUnexpectedEndByte {
				return nil, err
			}
			log.Println(data)
			log.Println(string(data))
		}
		res := make(map[string]string)
		for i := 0; i < len(l.list); i += 2 {
			res[l.list[i].String()] = l.list[i+1].String()
		}

		row.ValMap = res
	case ValueTypeListQuicklist:
	default:
		log.Println(row.Type)
		return nil, errors.New("unexpected value type code")
	}
	return row, nil
}
