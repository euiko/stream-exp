package stream

import "context"

type (
	Sink interface {
		Type() Type
		Handle(ExecutionContext, Data) error
	}

	SinkFactory func(context.Context) Sink
)
