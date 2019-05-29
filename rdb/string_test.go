package rdb

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeLengthPrefixedString_ExpectedNumberOfIncomingBytes_CorrectString(t *testing.T) {
	r := require.New(t)

	str, err := DecodeLengthPrefixedString(bytes.NewBufferString("expected string"), 15)
	r.NoError(err)
	r.Equal("expected string", str)
}

func TestDecodeLengthPrefixedString_LessThanExpectedNumberOfIncomingBytes_CorrectString(t *testing.T) {
	r := require.New(t)

	str, err := DecodeLengthPrefixedString(bytes.NewBufferString("expected string"), 16)
	r.Error(err)
	r.Equal("", str)
	r.Equal(io.ErrUnexpectedEOF, err)
}
