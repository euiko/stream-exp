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

	ec.Source(source).
		Filter(func(ctx context.Context, d Data) (bool, error) {
			var str string
			if err := d.Scan(&str); err != nil {
				return false, err
			}

			return strings.Contains(str, "process"), nil
		}).
		Map(func() Mapper {
			return MapperFunc(func(ctx context.Context, d Data) (Data, error) {
				var str string
				if err := d.Scan(&str); err != nil {
					return nil, err
				}

				return String("mapped => " + str), nil
			})
		}).
		AddSink(func() Sink {
			return SinkFunc(func(ctx context.Context, d Data) error {
				log.Println(d)
				return nil
			})
		})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ec.Run(ctx)
}
