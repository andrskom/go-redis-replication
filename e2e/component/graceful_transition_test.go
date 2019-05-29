package component

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/andrskom/go-redis-replication/client"
	"github.com/andrskom/go-redis-replication/component/transition"
	"github.com/andrskom/go-redis-replication/e2e/util"
	"github.com/andrskom/go-redis-replication/rdb"
	"github.com/andrskom/go-redis-replication/resp"
)

func TestGracefulTransition(t *testing.T) {
	r := require.New(t)
	connSync, err := util.GetRedisConn()
	r.NoError(err)
	defer  connSync.Close()
	clSync := client.New(resp.NewConn(connSync))
	connCmd, err := util.GetRedisConn()
	r.NoError(err)
	defer  connCmd.Close()
	clCmd := client.New(resp.NewConn(connCmd))

	transitionComponent := transition.NewGraceful(
		transition.GetDefaultConfig(),
		&resp.LogConsumer{},
		&rdb.LogConsumer{},
		clCmd,
		clSync,
	)

	r.NoError(transitionComponent.Run())
}
