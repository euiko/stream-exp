package stream

import (
	"errors"
	"sync/atomic"
)

type (
	Type int32
)

var typeIndex int32 = 0

var (
	StringType = NewType()
	MapType    = NewType()
	IntType    = NewType()
)

var (
	ErrScanInvalidType = errors.New("scan error, invalid type")
)

func NewType() Type {
	return Type(atomic.AddInt32(&typeIndex, 1))
}
