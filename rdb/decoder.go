package rdb

import (
	"errors"
	"log"
)

type Decoder struct {
	r                  ByteReader
	c                  Consumer
	decodeDbInProgress bool
}

func NewDecoder(r ByteReader, c Consumer) *Decoder {
	return &Decoder{r: r, c: c}
}

func (d *Decoder) Decode() error {
	rdbVersion, err := DecodeMagic(d.r)
	if err != nil {
		return err
	}
	d.c.RDBVersion(rdbVersion)
	for {
		b, err := d.r.ReadByte()
		if err != nil {
			return err
		}
		switch b {
		case OpCodeAUX:
			aux := AuxiliaryField{}
			key, err := d.readString()
			if err != nil {
				return err
			}
			aux.K = key
			val, err := d.readVal()
			if err != nil {
				return err
			}
			aux.V = val
			d.c.AuxiliaryField(aux)
		case OpCodeRESIZEDB:
			dbHTSize, err := DecodeLength(d.r)
			if err != nil {
				return err
			}
			expiryHTSize, err := DecodeLength(d.r)
			if err != nil {
				return err
			}
			d.c.ResizeDB(dbHTSize.GetLength(), expiryHTSize.GetLength())
			if err := d.readDB(dbHTSize.GetLength()); err != nil {
				return err
			}
		case OpCodeEXPIRETIMEMS:
			panic("not implemented")
		case OpCodeEXPIRETIME:
			panic("not implemented")
		case OpCodeSELECTDB:
			db, err := DecodeLength(d.r)
			if err != nil {
				return err
			}
			d.c.SelectDB(db.GetLength())
		case OpCodeEOF:
			crc := make([]byte, 8)
			_, err := d.r.Read(crc)
			if err != nil {
				return nil
			}
			d.c.End(crc)
			return nil
		default:
			log.Printf("%#x", b)
			return errors.New("unexpected op code")
		}
	}
}

func (d *Decoder) readVal() (interface{}, error) {
	length, err := DecodeLength(d.r)
	if err != nil {
		return "", err
	}
	switch length.GetType() {
	case LengthType6Bits, LengthType4Bytes:
		return DecodeLengthPrefixedString(d.r, length.GetLength())
	case LengthTypeEncodedStringInt:
		return DecodeEncodedStringInt(d.r, length.GetLength())
	}
	return 0, errors.New("unexpected type of string")
}

func (d *Decoder) readString() (string, error) {
	length, err := DecodeLength(d.r)
	if err != nil {
		return "", err
	}
	if length.IsEncodedString() {
		return "", errors.New("unexpected length type while read string")
	}
	return DecodeLengthPrefixedString(d.r, length.GetLength())
}

func (d *Decoder) readInt() (uint32, error) {
	length, err := DecodeLength(d.r)
	if err != nil {
		return 0, err
	}
	if length.GetType() != LengthTypeEncodedStringInt {
		return 0, errors.New("unexpected length type while expected encoded int")
	}
	return DecodeEncodedStringInt(d.r, length.GetLength())
}

func (d *Decoder) readDB(numKey uint32) error {
	for numKey > 0 {
		numKey--
		rd := &RowDecoder{d.r}
		row, err := rd.Decode()
		if err != nil {
			return err
		}
		d.c.Row(row)
	}
	return nil
}
