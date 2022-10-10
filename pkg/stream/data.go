package stream

import (
	"github.com/mitchellh/mapstructure"
)

type (
	Data interface {
		Is(Type) bool
		Type() Type
		Scan(interface{}) error
	}

	KeyedData interface {
		Data
		Key() interface{}
	}

	String string

	Int int

	Map map[string]interface{}

	keyedData struct {
		data Data
		key  interface{}
	}
)

func (m Map) Is(t Type) bool {
	return t == MapType
}

func (m Map) Type() Type {
	return MapType
}

func (m Map) Scan(v interface{}) error {
	return mapstructure.Decode(m, v)
}

func (s String) Is(t Type) bool {
	return t == StringType
}

func (s String) Type() Type {
	return StringType
}

func (s String) Scan(v interface{}) error {
	ptr, ok := v.(*string)
	if !ok {
		return ErrScanInvalidType
	}

	*ptr = string(s)
	return nil
}

func (s Int) Is(t Type) bool {
	return t == IntType
}

func (s Int) Type() Type {
	return IntType
}

func (s Int) Scan(v interface{}) error {
	ptr, ok := v.(*int)
	if !ok {
		return ErrScanInvalidType
	}

	*ptr = int(s)
	return nil
}

func (d *keyedData) Is(t Type) bool {
	return d.data.Is(t)
}

func (d *keyedData) Type() Type {
	return d.data.Type()
}

func (d *keyedData) Scan(target interface{}) error {
	return d.data.Scan(target)
}

func (d *keyedData) Key() interface{} {
	return d.key
}

func NewKeyedData(key interface{}, d Data) *keyedData {
	return &keyedData{
		data: d,
		key:  key,
	}
}
