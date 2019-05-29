package client

import (
	"bufio"
	"errors"
	"strconv"
	"sync"

	"github.com/andrskom/go-redis-replication/resp"
)

var ErrSyncStarted = errors.New("sync cmd sent, replication mode enabled")

// Client work only in one thread, and safe for concurrency
type Client struct {
	conn        *resp.Conn
	mu          sync.Mutex
	syncStarted bool
}

func New(conn *resp.Conn) *Client {
	return &Client{
		conn: conn,
	}
}

func (c *Client) Del(keys ...string) (*resp.Result, error) {
	var res *resp.Result
	var err error
	sfErr := c.safeFunc(func() {
		res, err = c.conn.ExecCmd(resp.NewCmd(resp.CmdDel, keys...))
	})
	if sfErr != nil {
		return nil, sfErr
	}
	return res, err
}

func (c *Client) FlushDB() (*resp.Result, error) {
	var res *resp.Result
	var err error
	sfErr := c.safeFunc(func() {
		res, err = c.conn.ExecCmd(resp.NewCmd(resp.CmdFlushDB))
	})
	if sfErr != nil {
		return nil, sfErr
	}
	return res, err
}

func (c *Client) LPush(key string, val string, addVals ...string) (*resp.Result, error) {
	var res *resp.Result
	var err error
	args := make([]string, 0, len(addVals)+2)
	args = append(args, key, val)
	args = append(args, addVals...)
	sfErr := c.safeFunc(func() {
		res, err = c.conn.ExecCmd(resp.NewCmd(resp.CmdLPush, args...))
	})
	if sfErr != nil {
		return nil, sfErr
	}
	return res, err
}

func (c *Client) Select(db int) (*resp.Result, error) {
	var res *resp.Result
	var err error
	sfErr := c.safeFunc(func() {
		res, err = c.conn.ExecCmd(resp.NewCmd(resp.CmdSelect, strconv.Itoa(db)))
	})
	if sfErr != nil {
		return nil, sfErr
	}
	return res, err
}

func (c *Client) Set(k string, v string) (*resp.Result, error) {
	var res *resp.Result
	var err error
	sfErr := c.safeFunc(func() {
		res, err = c.conn.ExecCmd(resp.NewCmd(resp.CmdSet, k, v))
	})
	if sfErr != nil {
		return nil, sfErr
	}
	return res, err
}

func (c *Client) Setex(k string, secondsDuration int, v string) (*resp.Result, error) {
	var res *resp.Result
	var err error
	sfErr := c.safeFunc(func() {
		res, err = c.conn.ExecCmd(resp.NewCmd(resp.CmdSetex, k, strconv.Itoa(secondsDuration), v))
	})
	if sfErr != nil {
		return nil, sfErr
	}
	return res, err
}

func (c *Client) Sync() (*bufio.Reader, *resp.Result, error) {
	var res *resp.Result
	var err error
	sfErr := c.safeSyncFunc(func() {
		if err = c.conn.WriteCmd(resp.NewCmd(resp.CmdSync)); err != nil {
			return
		}
		res, err = c.conn.WaitCmdResult()
	}, true)
	if sfErr != nil {
		return nil, nil, sfErr
	}

	return c.conn.GetReader(), res, err
}

func (c *Client) Hset(k string, field string, v string) (*resp.Result, error) {
	var res *resp.Result
	var err error
	sfErr := c.safeFunc(func() {
		res, err = c.conn.ExecCmd(resp.NewCmd(resp.CmdHset, k, field, v))
	})
	if sfErr != nil {
		return nil, sfErr
	}
	return res, err
}

func (c *Client) ConfigSet(k resp.ConfigKey, v string) (*resp.Result, error) {
	var res *resp.Result
	var err error
	sfErr := c.safeFunc(func() {
		res, err = c.conn.ExecCmd(resp.NewCmd(resp.CmdConfig, resp.ConfigSubCmdSet, string(k), v))
	})
	if sfErr != nil {
		return nil, sfErr
	}
	return res, err
}

func (c *Client) lock() {
	c.mu.Lock()
}

func (c *Client) unlock() {
	c.mu.Unlock()
}

func (c *Client) safeFunc(f func()) error {
	return c.safeSyncFunc(f, false)
}

func (c *Client) safeSyncFunc(f func(), isSync bool) error {
	c.lock()
	defer c.unlock()
	if isSync {
		if c.syncStarted {
			return ErrSyncStarted
		}
		c.syncStarted = true
	}

	f()
	return nil
}
