package resp

import (
	"bufio"
	"context"
	"errors"
	"log"
	"strconv"
)

type Consumer interface {
	Cmd(cmd Cmd)
}

type LogConsumer struct {
}

func (c *LogConsumer) Cmd(cmd Cmd) {
	log.Printf("Cmd: %#v", cmd)
}

type Decoder struct {
	r      *bufio.Reader
	c      Consumer
	stopCh chan bool
}

func NewDecoder(r *bufio.Reader, consumer Consumer) *Decoder {
	return &Decoder{
		r:      r,
		c:      consumer,
		stopCh: make(chan bool),
	}
}

func (d *Decoder) Decode() error {
	for {
		select {
		case <-d.stopCh:
			return nil
		default:
			arrNumBytes, err := d.r.ReadSlice('\n')
			if err != nil {
				return errors.New("can't read number of command lines")
			}
			if arrNumBytes[0] != ArrayOpcode {
				return errors.New("unexpected symbol")
			}
			arrNum, err := strconv.Atoi(string(arrNumBytes[1 : len(arrNumBytes)-2]))
			if err != nil {
				return errors.New("can't convert arrnum string to int")
			}
			cmd := make(Cmd, 0, arrNum)
			for arrNum > 0 {
				arrNum--
				_, err := d.r.ReadSlice('\n')
				if err != nil {
					return errors.New("can't read arg len")
				}
				val, err := d.r.ReadSlice('\n')
				if err != nil {
					return errors.New("can't read arg val")
				}
				cmd = append(cmd, string(val[:len(val)-2]))
			}
			d.c.Cmd(cmd)
		}
	}
}

func (d *Decoder) Shutdown(ctx context.Context) error {
	close(d.stopCh)
	return nil
}
