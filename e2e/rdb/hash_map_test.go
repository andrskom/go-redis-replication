package rdb

import (
	"log"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/andrskom/go-redis-replication/client"
	"github.com/andrskom/go-redis-replication/e2e/util"
	"github.com/andrskom/go-redis-replication/rdb"
	"github.com/andrskom/go-redis-replication/resp"
)

func TestHashMapWithoutZiplist(t *testing.T) {
	r := require.New(t)
	conn, err := util.GetRedisConn()
	r.NoError(err)
	cl := client.New(resp.NewConn(conn))
	res, err := cl.ConfigSet(resp.ConfigKeyHashMaxZiplistValue, "1")
	r.NoError(err)
	r.True(res.IsOk())

	{
		res, err := cl.Select(0)
		r.NoError(err)
		r.True(res.IsOk(), "Bad result", res.String())
		res, err = cl.FlushDB()
		r.NoError(err)
		r.False(res.IsErr(), "Bad result", res.String())
		for i :=  0; i < 100; i++ {
			key := util.GenRandString("key")
			keyNum := 3
			for j := 0; j < keyNum; j++ {
				res, err = cl.Hset(key, util.GenRandString("field"), util.GenRandString("val"))
				r.NoError(err)
				r.NoError(res.CheckCreated(), "Bad result", res.String())
			}
		}
	}

	reader, res, err := cl.Sync()
	r.NoError(err)
	r.True(res.IsBulkString())
	log.Println(res.GetBulkStringLen())
	dec := rdb.NewDecoder(reader, &rdb.LogConsumer{})
	r.NoError(dec.Decode())
	r.NoError(resp.NewDecoder(reader, &resp.LogConsumer{}).Decode())
}

func TestHashMapWitZiplist(t *testing.T) {
	r := require.New(t)
	conn, err := util.GetRedisConn()
	r.NoError(err)
	cl := client.New(resp.NewConn(conn))
	{
		res, err := cl.ConfigSet(resp.ConfigKeyHashMaxZiplistValue, "64")
		r.NoError(err)
		r.True(res.IsOk())
	}
	{
		res, err := cl.ConfigSet(resp.ConfigKeyHashMaxZiplistEntries, "512")
		r.NoError(err)
		r.True(res.IsOk())
	}

	{
		res, err := cl.Select(0)
		r.NoError(err)
		r.True(res.IsOk(), "Bad result", res.String())
		res, err = cl.FlushDB()
		r.NoError(err)
		r.False(res.IsErr(), "Bad result", res.String())
		for i :=  0; i < 100; i++ {
			key := util.GenRandString("key")
			keyNum := 3
			for j := 0; j < keyNum; j++ {
				res, err = cl.Hset(key, util.GenRandString("field"), util.GenRandString("val"))
				r.NoError(err)
				r.NoError(res.CheckCreated(), "Bad result", res.String())
			}
		}
	}

	reader, res, err := cl.Sync()
	r.NoError(err)
	r.True(res.IsBulkString())
	log.Println(res.GetBulkStringLen())
	dec := rdb.NewDecoder(reader, &rdb.LogConsumer{})
	r.NoError(dec.Decode())
	r.NoError(resp.NewDecoder(reader, &resp.LogConsumer{}).Decode())
}
