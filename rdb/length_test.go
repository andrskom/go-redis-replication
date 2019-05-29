package rdb

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeLength_GivenReadFirstByteErr_Err(t *testing.T) {
	r := require.New(t)

	l, err := DecodeLength(bytes.NewReader([]byte{}))
	r.Error(err)
	r.Nil(l)
	r.Equal(io.EOF, err)
}

func TestDecodeLength_Given6BitsLengthBytePrefix_6BitsLength(t *testing.T) {
	r := require.New(t)
	dp := []byte{
		0x00,
		0x01,
		0x3F,
	}

	for k, data := range dp {
		t.Run(fmt.Sprintf("Data provider #%d", k), func(t *testing.T) {
			l, err := DecodeLength(bytes.NewReader([]byte{data}))
			r.NoError(err)
			r.Equal(LengthType6Bits, l.t)
			r.Equal(uint32(data), l.length)
			r.Equal(uint32(0), l.verifyLength)
		})
	}
}

func TestDecodeLength_Given14BitsLengthBytePrefix_14BitsLength(t *testing.T) {
	r := require.New(t)
	type testData struct {
		data        []byte
		expectedRes uint32
	}
	dp := []testData{
		{data: []byte{0x40, 0x00}, expectedRes: 0},
		{data: []byte{0x40, 0x01}, expectedRes: 1},
		{data: []byte{0x7F, 0xFF}, expectedRes: 16383},
	}

	for k, data := range dp {
		t.Run(fmt.Sprintf("Data provider #%d", k), func(t *testing.T) {
			l, err := DecodeLength(bytes.NewReader(data.data))
			r.NoError(err)
			r.Equal(LengthType14Bits, l.t)
			r.Equal(data.expectedRes, l.length)
			r.Equal(uint32(0), l.verifyLength)
		})
	}
}

func TestDecodeLength_Given14BitsLengthBytePrefixWithReadSecondByteErr_Err(t *testing.T) {
	r := require.New(t)
	type testData struct {
		data []byte
	}
	dp := []testData{
		{data: []byte{0x40}},
		{data: []byte{0x7F}},
	}

	for k, data := range dp {
		t.Run(fmt.Sprintf("Data provider #%d", k), func(t *testing.T) {
			l, err := DecodeLength(bytes.NewReader(data.data))
			r.Error(err)
			r.Nil(l)
			r.Equal(io.EOF, err)
		})
	}
}

func TestDecodeLength_Given4BytesLengthBytePrefix_4BytesLength(t *testing.T) {
	r := require.New(t)
	type testData struct {
		data        []byte
		expectedRes uint32
	}
	dp := []testData{
		{data: []byte{0x80, 0x00, 0x00, 0x00, 0x00}, expectedRes: 0},
		{data: []byte{0x80, 0x00, 0x00, 0x00, 0x01}, expectedRes: 1},
		{data: []byte{0x80, 0xFF, 0xFF, 0xFF, 0xFF}, expectedRes: math.MaxUint32},
		{data: []byte{0xBF, 0x00, 0x00, 0x00, 0x00}, expectedRes: 0},
		{data: []byte{0xBF, 0x00, 0x00, 0x00, 0x01}, expectedRes: 1},
		{data: []byte{0xBF, 0xFF, 0xFF, 0xFF, 0xFF}, expectedRes: math.MaxUint32},
	}

	for k, data := range dp {
		t.Run(fmt.Sprintf("Data provider #%d", k), func(t *testing.T) {
			l, err := DecodeLength(bytes.NewReader(data.data))
			r.NoError(err)
			r.Equal(LengthType4Bytes, l.t)
			r.Equal(data.expectedRes, l.length)
			r.Equal(uint32(0), l.verifyLength)
		})
	}
}

func TestDecodeLength_Given4BytesLengthBytePrefixReadOneOf4BytesErr_Err(t *testing.T) {
	r := require.New(t)
	type testData struct {
		data []byte
		err  error
	}
	dp := []testData{
		{data: []byte{0x80}, err: io.EOF},
		{data: []byte{0x80, 0x00}, err: io.ErrUnexpectedEOF},
		{data: []byte{0x80, 0x00, 0x00}, err: io.ErrUnexpectedEOF},
		{data: []byte{0x80, 0x00, 0x00, 0x00}, err: io.ErrUnexpectedEOF},
	}

	for k, data := range dp {
		t.Run(fmt.Sprintf("Data provider #%d", k), func(t *testing.T) {
			l, err := DecodeLength(bytes.NewReader(data.data))
			r.Error(err)
			r.Nil(l)
			r.Equal(data.err, err)
		})
	}
}

func TestDecodeLength_GivenEncodedStringIntLengthBytePrefix_LenOfInt(t *testing.T) {
	r := require.New(t)
	type testData struct {
		data        []byte
		expectedRes uint32
	}
	dp := []testData{
		{data: []byte{0xC0}, expectedRes: 1},
		{data: []byte{0xC1}, expectedRes: 2},
		{data: []byte{0xC2}, expectedRes: 3},
	}

	for k, data := range dp {
		t.Run(fmt.Sprintf("Data provider #%d", k), func(t *testing.T) {
			l, err := DecodeLength(bytes.NewReader(data.data))
			r.NoError(err)
			r.Equal(LengthTypeEncodedStringInt, l.t)
			r.Equal(data.expectedRes, l.length)
			r.Equal(uint32(0), l.verifyLength)
		})
	}
}

func TestDecodeLength_GivenEncodedStringCompressedStringLengthBytePrefix_LenOfCompressedStringWithVerifyLen(t *testing.T) {
	r := require.New(t)
	type testData struct {
		data              []byte
		expectedLen       uint32
		expectedVerifyLen uint32
	}
	dp := []testData{
		{
			data: []byte{
				0xC3,       // prefix
				0x03,       // first length
				0x40, 0x04, // verify length
			},
			expectedLen:       3,
			expectedVerifyLen: 4,
		},
		{
			data: []byte{
				0xC3,                         // prefix
				0x40, 0x05,                   // first length
				0x80, 0xFF, 0xFF, 0xFF, 0xFF, // verify length
			},
			expectedLen:       5,
			expectedVerifyLen: math.MaxUint32,
		},
	}

	for k, data := range dp {
		t.Run(fmt.Sprintf("Data provider #%d", k), func(t *testing.T) {
			l, err := DecodeLength(bytes.NewReader(data.data))
			r.NoError(err)
			r.Equal(LengthTypeEncodedStringCompressedStr, l.t)
			r.Equal(data.expectedLen, l.length)
			r.Equal(data.expectedVerifyLen, l.verifyLength)
		})
	}
}

func TestDecodeLength_GivenPrefixEncodedStringWithBadFormat_Err(t *testing.T) {
	r := require.New(t)
	dp := []byte{0xC4, 0xFF, 0xC5, 0xFE,}

	for k, data := range dp {
		t.Run(fmt.Sprintf("Data provider #%d", k), func(t *testing.T) {
			l, err := DecodeLength(bytes.NewReader([]byte{data}))
			r.Error(err)
			r.Nil(l)
			r.Equal("unexpected encoded string format", err.Error())
		})
	}
}

func TestDecodeLength_GivenPrefixEncodedStringWithErrInNextByte_Err(t *testing.T) {
	r := require.New(t)

	l, err := DecodeLength(bytes.NewReader([]byte{0xC3}))
	r.Error(err)
	r.Nil(l)
	r.Equal(io.EOF, err)
}

func TestDecodeLength_GivenPrefixEncodedStringWithNextTimeReturnEncodedString_Err(t *testing.T) {
	r := require.New(t)
	dp := [][]byte{
		{0xC3, 0xC0},
		{0xC3, 0xC1},
		{0xC3, 0xC2},
		{0xC3, 0xC3},
		{0xC3, 0x01, 0xC0},
		{0xC3, 0x01, 0xC1},
		{0xC3, 0x01, 0xC2},
		{0xC3, 0x01, 0xC3},
	}

	for k, data := range dp {
		t.Run(fmt.Sprintf("Data provider #%d", k), func(t *testing.T) {
			l, err := DecodeLength(bytes.NewReader(data))
			r.Error(err)
			r.Nil(l)
			r.Equal(ErrLengthEncodedStringBadLength, err)
		})
	}
}
