package stream

import (
	"context"
	"log"
	"strings"
	"testing"
	"time"
)

func TestStringStream(t *testing.T) {
	var (
		ec     ExecutionContext
		source SourceFactory
	)

	ec = New()
	source = SourceFileFactory("stream_test.txt")

	ds := ec.Source(source)

	ds.KeyBy(func(d Data) interface{} {
		var str string
		if err := d.Scan(&str); err != nil {
			return nil
		}

		if len(str) < 1 {
			return nil
		}

		return string(str[0])
	}).Map(func() Mapper {
		var total int
		return MapperFunc(func(ctx context.Context, d Data) (Data, error) {
			total += 1
			keyed := d.(KeyedData)
			return NewKeyedData(keyed.Key(), Int(total)), nil
		})
	}).AddSink(func() Sink {
		return SinkFunc(func(ctx context.Context, d Data) error {
			var total int
			if err := d.Scan(&total); err != nil {
				return err
			}

			keyed := d.(KeyedData)
			log.Println(keyed.Key().(string), " = ", total)
			return nil
		})
	})

	ds.Filter(func(ctx context.Context, d Data) (bool, error) {
		var str string
		if err := d.Scan(&str); err != nil {
			return false, err
		}

		return strings.Contains(str, "process"), nil
	}).Map(func() Mapper {
		return MapperFunc(func(ctx context.Context, d Data) (Data, error) {
			var str string
			if err := d.Scan(&str); err != nil {
				return nil, err
			}

			return String("mapped => " + str), nil
		})
	}).AddSink(func() Sink {
		return SinkFunc(func(ctx context.Context, d Data) error {
			log.Println(d)
			return nil
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ec.Run(ctx)
}
