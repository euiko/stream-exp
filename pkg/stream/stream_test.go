package stream

import (
	"context"
	"strings"
	"testing"
)

func TestStringStream(t *testing.T) {
	var (
		ctx       ExecutionContext
		source    Source
		printSink SinkFactory
	)
	ctx.Source(source).
		Filter(func(ec ExecutionContext, d Data) (bool, error) {
			var str string
			if err := d.Scan(&str); err != nil {
				return false, err
			}

			return strings.Contains(str, "process"), nil
		}).
		Map(func() Mapper {
			return MapperFunc(func(ec ExecutionContext, d Data) (Data, error) {
				var str string
				if err := d.Scan(&str); err != nil {
					return nil, err
				}

				return String(d.Ts(), "mapped => "+str), nil
			})
		}).
		AddSink(printSink)

	ctx.Run(context.Background())
}
