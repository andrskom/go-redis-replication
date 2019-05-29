package resp

import (
	"errors"
	"fmt"
)

var ErrAlreadyExists = errors.New("record already exists")
var ErrCheckCreatedUnexpectedResult = errors.New("check created unexpected result")

type Result struct {
	t         byte
	stringVal string
	intVal    int64
}

func (r *Result) String() string {
	return fmt.Sprintf(`
[
	type: %x
	stringVal: %s
	intVal: %d
]
`, r.t, r.stringVal, r.intVal)
}

func (r *Result) IsOk() bool {
	return r.t == SimpleStringOpcode && r.stringVal == "OK"
}

func (r *Result) IsErr() bool {
	return r.t == ErrorOpcode
}

func (r *Result) IsBulkString() bool {
	return r.t == BulkStringOpcode
}

func (r *Result) GetBulkStringLen() int64 {
	return r.intVal
}

func (r *Result) IsInt() bool {
	return r.t == IntegerOpcode
}

func (r *Result) GetInt() int64 {
	return r.intVal
}

func (r *Result) CheckCreated() error {
	if !r.IsInt() {
		return ErrCheckCreatedUnexpectedResult
	}
	switch r.intVal {
	case 0:
		return ErrAlreadyExists
	case 1:
		return nil
	default:
		return ErrCheckCreatedUnexpectedResult
	}
}
