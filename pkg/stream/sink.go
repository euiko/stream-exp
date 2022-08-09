package stream

import "context"

type (
	Sink interface {
		Handle(context.Context, Data) error
	}

	SinkFunc func(context.Context, Data) error

	SinkFactory func() Sink
)

func (f SinkFunc) Handle(ctx context.Context, d Data) error {
	return f(ctx, d)
}

func (f SinkFunc) Process(ctx context.Context, p Process, d Data) error {
	return f.Handle(ctx, d)
}
