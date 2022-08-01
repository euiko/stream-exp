package stream

import (
	"time"

	"github.com/mitchellh/mapstructure"
)

type (
	Data interface {
		Is(Type) bool
		Type() Type
		Ts() time.Time
		Scan(interface{}) error
	}

	StringData struct {
		ts    time.Time
		value string
	}

	MapData struct {
		ts    time.Time
		value map[string]interface{}
	}
)

const (
	mapTimeKey = "x-attr-ts"
)

func (m MapData) Is(t Type) bool {
	return t == MapType
}

func (m MapData) Type() Type {
	return MapType
}

func (m MapData) Ts() time.Time {
	v, ok := m.value[mapTimeKey]
	if !ok {
		return time.Time{}
	}

	if v, ok := v.(time.Time); ok {
		return v
	}

	return time.Time{}
}

func (m MapData) Scan(v interface{}) error {
	data := m.value
	data[mapTimeKey] = m.ts

	return mapstructure.Decode(data, v)
}

func (s StringData) Is(t Type) bool {
	return t == StringType
}

func (s StringData) Type() Type {
	return StringType
}

func (s StringData) Ts() time.Time {
	return s.ts
}

func (s StringData) Scan(v interface{}) error {
	ptr, ok := v.(*string)
	if !ok {
		return ErrScanInvalidType
	}

	*ptr = s.value
	return nil
}

func Map(ts time.Time, value map[string]interface{}) MapData {
	return MapData{
		ts:    ts,
		value: value,
	}
}

func String(ts time.Time, value string) StringData {
	return StringData{
		ts:    ts,
		value: value,
	}
}