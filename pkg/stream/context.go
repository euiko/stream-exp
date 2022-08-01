package stream

import "context"

type (
	ExecutionContext interface {
		Source(Source) DataStream
		Run(context.Context) error
	}
)
