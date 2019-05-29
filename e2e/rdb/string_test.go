package rdb

import (
	"log"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/andrskom/go-redis-replication/client"
	"github.com/andrskom/go-redis-replication/e2e/util"
	"github.com/andrskom/go-redis-replication/rdb"
	"github.com/andrskom/go-redis-replication/resp"
)

func TestString(t *testing.T) {
	r := require.New(t)
	conn, err := util.GetRedisConn()
	r.NoError(err)
	cl := client.New(resp.NewConn(conn))

	{
		res, err := cl.Select(0)
		r.NoError(err)
		r.True(res.IsOk(), "Bad result", res.String())
		res, err = cl.FlushDB()
		r.NoError(err)
		r.False(res.IsErr(), "Bad result", res.String())
		for i := 0; i < 100; i++ {
			res, err = cl.Set(util.GenRandString("key"), util.GenRandString("val"))
			r.NoError(err)
			r.True(res.IsOk(), "Bad result", res.String())
		}
		for i := 0; i < 100; i++ {
			res, err = cl.Setex(util.GenRandString("key"), int(rand.Uint32()%100) + 100, util.GenRandString("val"))
			r.NoError(err)
			r.True(res.IsOk(), "Bad result", res.String())
		}
	}

	{
		res, err := cl.Select(1)
		r.NoError(err)
		r.True(res.IsOk(), "Bad result", res.String())
		res, err = cl.FlushDB()
		r.NoError(err)
		r.False(res.IsErr(), "Bad result", res.String())
		for i := 0; i < 100; i++ {
			res, err = cl.Set(util.GenRandString("key"), util.GenRandString("val"))
			r.NoError(err)
			r.True(res.IsOk(), "Bad result", res.String())
		}
		for i := 0; i < 100; i++ {
			res, err = cl.Setex(util.GenRandString("key"), int(rand.Uint32()%100) + 100, util.GenRandString("val"))
			r.NoError(err)
			r.True(res.IsOk(), "Bad result", res.String())
		}
	}

	reader, res, err := cl.Sync()
	r.NoError(err)
	r.True(res.IsBulkString())
	log.Println(res.GetBulkStringLen())
	dec := rdb.NewDecoder(reader, &rdb.LogConsumer{})
	r.NoError(dec.Decode())
}
