package rdb

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecoder_Decode(t *testing.T) {
	r := require.New(t)
	data, err := ioutil.ReadFile("/Users/andrskom/go/src/github.com/andrskom/go-redis-replication/rdb/rdb")
	r.NoError(err)
	dec := NewDecoder(bytes.NewBuffer(data), &LogConsumer{})
	err = dec.Decode()
	r.NoError(err)
}
