package rdb

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeMagic_9BytesWithCorrectHeader_Version(t *testing.T) {
	r := require.New(t)

	rdbVersion, err := DecodeMagic(bytes.NewBufferString("REDIS0003"))
	r.NoError(err)
	r.Equal("0003", rdbVersion)
}

func TestDecodeMagic_LessThan9BytesInReader_Err(t *testing.T) {
	r := require.New(t)

	rdbVersion, err := DecodeMagic(bytes.NewBufferString(""))
	r.Error(err)
	r.Equal("", rdbVersion)
	r.Equal(io.EOF, err)
}

func TestDecodeMagic_PrefixIsNotREDIS_Err(t *testing.T) {
	r := require.New(t)

	rdbVersion, err := DecodeMagic(bytes.NewBufferString("BADDB0003"))
	r.Error(err)
	r.Equal("", rdbVersion)
	r.Equal("unexpected magic header format", err.Error())
}
