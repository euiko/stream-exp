package stream

import "context"

type (
	ExecutionContext interface {
		Source(SourceFactory, ...SourceOption) DataStream
		Run(context.Context) error
		Stop() error
	}

	executionContext struct {
		sources []Source
		ds      []*dataStream

		ctx    context.Context
		cancel func()
	}
)

func (ec *executionContext) Source(factory SourceFactory, options ...SourceOption) DataStream {
	source := factory(options...)
	ds := newDataStream(source.Sink()) // TODO: pass datasource options

	ec.sources = append(ec.sources, source)
	ec.ds = append(ec.ds, ds)
	return ds
}

func (ec *executionContext) Run(ctx context.Context) error {
	ec.ctx, ec.cancel = context.WithCancel(ctx)

	// open it first
	for _, s := range ec.sources {
		if err := s.Open(ec.ctx); err != nil {
			return err
		}
	}
	// defer close
	defer func() {
		for _, s := range ec.sources {
			s.Close()
		}
	}()

	// then start
	for _, ds := range ec.ds {
		if err := ds.start(ec.ctx); err != nil {
			return err
		}
	}

	<-ec.ctx.Done()
	return nil
}

func (ec *executionContext) Stop() error {
	if ec.cancel != nil {
		ec.cancel()
	}
	return nil
}

func New() ExecutionContext {
	return &executionContext{
		sources: []Source{},
		ds:      []*dataStream{},
		ctx:     nil,
		cancel:  nil,
	}
}
