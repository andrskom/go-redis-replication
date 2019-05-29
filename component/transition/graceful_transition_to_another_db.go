package transition

import (
	"context"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/andrskom/go-redis-replication/client"
	"github.com/andrskom/go-redis-replication/rdb"
	"github.com/andrskom/go-redis-replication/resp"
)

const (
	DefaultSyncKey      = "sync_key"
	DefaultSyncTimeout  = time.Second
	DefaultBlockTimeout = 3 * time.Second
	syncValueFinal      = "final"

	transitionStatusAwait = iota
	transitionStatusStarted
	transitionStatusFinished
)

type Config struct {
	SyncDB       int
	SyncKey      string
	SyncTimeout  time.Duration
	BlockTimeout time.Duration
}

func GetDefaultConfig() Config {
	return Config{
		SyncDB:       0,
		SyncKey:      DefaultSyncKey,
		SyncTimeout:  DefaultSyncTimeout,
		BlockTimeout: DefaultBlockTimeout,
	}
}

type GracefulTransitionToAnotherDb struct {
	cfg              Config
	respConsumer     resp.Consumer
	rdbConsumer      rdb.Consumer
	cmdClient        *client.Client
	syncClient       *client.Client
	errCh            chan error
	stopSync         chan bool
	transitionStatus int
	blockCh          chan bool
}

func NewGraceful(
	cfg Config,
	respConsumer resp.Consumer,
	rdbConsumer rdb.Consumer,
	cmdClient *client.Client,
	syncClient *client.Client,
) *GracefulTransitionToAnotherDb {
	return &GracefulTransitionToAnotherDb{
		cfg:              cfg,
		respConsumer:     respConsumer,
		rdbConsumer:      rdbConsumer,
		cmdClient:        cmdClient,
		syncClient:       syncClient,
		errCh:            make(chan error),
		stopSync:         make(chan bool),
		transitionStatus: transitionStatusAwait,
		blockCh:          make(chan bool),
	}
}

func (c *GracefulTransitionToAnotherDb) Run() error {
	res, err := c.cmdClient.Select(c.cfg.SyncDB)
	if err != nil {
		return err
	}
	if !res.IsOk() {
		return errors.New("can't select command db for sync")
	}

	reader, res, err := c.syncClient.Sync()
	if err != nil {
		return err
	}
	if !res.IsBulkString() {
		log.Println(res.String())
		return errors.New("unexpected result on SYNC cmd")
	}
	log.Println("Decode started")
	if err := rdb.NewDecoder(reader, c.rdbConsumer).Decode(); err != nil {
		return err
	}

	respDecoder := resp.NewDecoder(reader, c)
	go func() {
		if err := respDecoder.Decode(); err != nil {
			c.errCh <- err
		}
	}()

	go c.sync()
	select {
	case err := <-c.errCh:
		return err
	case <-c.blockCh:
		if err := respDecoder.Shutdown(context.Background()); err != nil {
			return err
		}
	}
	return nil
}

func (c *GracefulTransitionToAnotherDb) BlockF() {
	if c.transitionStatus != transitionStatusStarted {
		return
	}

	<-c.blockCh
}

func (c *GracefulTransitionToAnotherDb) Cmd(cmd resp.Cmd) {
	if len(cmd) == 3 && strings.ToLower(cmd[0]) == string(resp.CmdSet) && cmd[1] == c.cfg.SyncKey {
		if cmd[2] == syncValueFinal {
			c.transitionStatus = transitionStatusFinished
			close(c.blockCh)
			return
		}
		t, err := strconv.ParseInt(cmd[2], 10, 0)
		if err != nil {
			c.errCh <- err
		}
		if time.Duration(time.Now().UnixNano()-t) < time.Second {
			c.stopSync <- true
			c.startTransition()
		}
		return
	}

	c.respConsumer.Cmd(cmd)
}

func (c *GracefulTransitionToAnotherDb) IsFinished() bool {
	return c.transitionStatus == transitionStatusFinished
}

func (c *GracefulTransitionToAnotherDb) sync() {
	for {
		select {
		case <-c.stopSync:
			return
		case <-time.After(time.Second):
			res, err := c.cmdClient.Set(c.cfg.SyncKey, strconv.FormatInt(time.Now().UnixNano(), 10))
			if err != nil {
				c.errCh <- err
				return
			}
			if !res.IsOk() {
				c.errCh <- errors.New("unexpected response from redis for sync")
				return
			}
		}
	}
}

func (c *GracefulTransitionToAnotherDb) startTransition() {
	c.transitionStatus = transitionStatusStarted
	res, err := c.cmdClient.Set(c.cfg.SyncKey, syncValueFinal)
	if err != nil {
		c.errCh <- err
		return
	}
	if !res.IsOk() {

	}
}
