package stream

import (
	"context"
)

type (
	Process interface {
		Process(context.Context, Data) error
	}

	Processor interface {
		Process(context.Context, Process, Data) error
	}

	ProcessorFunc func(context.Context, Process, Data) error

	ProcessorFactory func() Processor

	Mapper interface {
		Map(context.Context, Data) (Data, error)
	}

	MapperFunc func(context.Context, Data) (Data, error)

	MapperFactory func() Mapper

	FilterFunc func(context.Context, Data) (bool, error)
)

func (f ProcessorFunc) Process(ctx context.Context, p Process, d Data) error {
	return f(ctx, p, d)
}

func (f MapperFunc) Map(ctx context.Context, d Data) (Data, error) {
	return f(ctx, d)
}

func (f MapperFunc) Process(ctx context.Context, p Process, d Data) error {
	mapped, err := f.Map(ctx, d)
	if err != nil {
		return err
	}

	return p.Process(ctx, mapped)
}

func (f KeyFunc) Process(ctx context.Context, p Process, d Data) error {
	return p.Process(ctx, NewKeyedData(f(d), d))
}

func (f FilterFunc) Process(ctx context.Context, p Process, d Data) error {
	filtered, err := f(ctx, d)
	if err != nil {
		return err
	}

	if filtered {
		return nil
	}

	return p.Process(ctx, d)
}
