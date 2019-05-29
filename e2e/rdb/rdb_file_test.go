package rdb

import (
	"bufio"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/andrskom/go-redis-replication/rdb"
)

func TestRDBFile(t *testing.T)  {
	r := require.New(t)
	file, err := os.Open("/Users/andrskom/Downloads/20190527T100800-url-service-ams1-url-service-redis-master.rdb")
	r.NoError(err)
	reader := bufio.NewReader(file)
	dec := rdb.NewDecoder(reader, &rdb.LogConsumer{})
	r.NoError(dec.Decode())
}
