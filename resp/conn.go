package resp

import (
	"bufio"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
)

const (
	// SimpleStringOpcode это первый байт простого однострочного ответа ("+")
	SimpleStringOpcode = 0x2b

	// ErrorOpcode это первый байт ответа ошибки ("-")
	ErrorOpcode = 0x2d

	// IntegerOpcode это первый байт целого числа (":")
	IntegerOpcode = 0x3a

	// BulkStringOpcode это первый байт бинарных данных ("$")
	BulkStringOpcode = 0x24

	// ArrayOpcode это первый байт массива ("*")
	ArrayOpcode = 0x2a

	// CR это символ \r
	CR = 0xd

	// LF это символ \n
	LF = 0xa
)

type Conn struct {
	r *bufio.Reader
	w io.Writer
}

func NewConn(c io.ReadWriter) *Conn {
	return &Conn{
		r: bufio.NewReader(c),
		w: c,
	}
}

func (c *Conn) GetReader() *bufio.Reader {
	return c.r
}

func (c *Conn) ExecCmd(cmd Cmd) (*Result, error) {
	if err := c.WriteCmd(cmd); err != nil {
		return nil, err
	}

	return c.ReadCmdResult()
}

func (c *Conn) WriteCmd(cmd Cmd) error {
	argLen := len(cmd)
	cmdBytes := []byte{ArrayOpcode}
	cmdBytes = append(cmdBytes, []byte(strconv.Itoa(argLen))...)
	cmdBytes = append(cmdBytes, CR, LF)
	for _, arg := range cmd {
		cmdBytes = append(cmdBytes, BulkStringOpcode)
		cmdBytes = append(cmdBytes, []byte(strconv.Itoa(len(arg)))...)
		cmdBytes = append(cmdBytes, CR, LF)
		cmdBytes = append(cmdBytes, []byte(arg)...)
		cmdBytes = append(cmdBytes, CR, LF)
	}

	n, err := c.w.Write(cmdBytes)
	if err != nil {
		return nil
	}
	if n != len(cmdBytes) {
		return errors.New("unexpected num of written bytes")
	}
	return nil
}

func (c *Conn) ReadCmdResult() (*Result, error) {
	stringBytes, err := c.r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	return c.DecodeCmdResult(stringBytes[0], stringBytes[1:])
}

func (c *Conn) WaitCmdResult() (*Result, error) {
	var (
		stringBytes []byte
		err         error
	)
	for {
		stringBytes, err = c.r.ReadSlice('\n')
		if err != nil {
			return nil, err
		}
		log.Println(stringBytes)
		if len(stringBytes) == 1 && stringBytes[0] == LF {
			continue
		}

		break
	}

	return c.DecodeCmdResult(stringBytes[0], stringBytes[1:])
}

func (c *Conn) DecodeCmdResult(code byte, data []byte) (*Result, error) {
	var err error
	res := &Result{
		t: code,
	}

	switch code {
	case SimpleStringOpcode, ErrorOpcode:
		res.stringVal = strings.TrimRight(string(data), "\r\n")
	case IntegerOpcode:
		res.intVal, err = strconv.ParseInt(strings.TrimRight(string(data), "\r\n"), 10, 0)
		if err != nil {
			return nil, err
		}
	case BulkStringOpcode:
		res.intVal, err = strconv.ParseInt(strings.TrimRight(string(data), "\r\n"), 10, 0)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}
