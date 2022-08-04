package stream

import "context"

type (
	ExecutionContext interface {
		Context() context.Context
		Source(Source) DataStream
		Run(context.Context) error
	}
)
