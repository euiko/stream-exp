package stream

import "context"

type (
	KeyFunc func(Data) interface{}

	DataStreamOption func(*dataStream)

	DataStream interface {
		Name() string

		KeyBy(KeyFunc, ...DataStreamOption) DataStream
		Filter(FilterFunc, ...DataStreamOption) DataStream
		Process(ProcessorFactory, ...DataStreamOption) DataStream
		Map(MapperFactory, ...DataStreamOption) DataStream
		AddSink(SinkFactory, ...DataStreamOption) DataStream
	}

	dataStream struct {
		name string

		source         Channel
		channelFactory ChannelFactory

		childs []*dataStream
		pipes  []Pipe
	}
)

func DataStreamChannelFactory(name string) DataStreamOption {
	return func(ds *dataStream) {
		ds.channelFactory = getChannelFactory(name)
	}
}

func DataStreamName(name string) DataStreamOption {
	return func(ds *dataStream) {
		ds.name = name
	}
}

func (ds *dataStream) Name() string {
	return ds.name
}
func (ds *dataStream) KeyBy(f KeyFunc, options ...DataStreamOption) DataStream {
	return ds.Process(func() Processor {
		return f
	}, options...)
}

func (ds *dataStream) Filter(f FilterFunc, options ...DataStreamOption) DataStream {
	return ds.Process(func() Processor {
		return f
	}, options...)
}

func (ds *dataStream) Process(f ProcessorFactory, options ...DataStreamOption) DataStream {
	var (
		keyedProcessors = make(map[interface{}]Processor)
		singleProcessor Processor
	)

	source, sink := ds.channelFactory(), ds.channelFactory()
	pipe := newPipe(source, sink)
	nextDs := newDataStream(sink, options...)

	ds.pipes = append(ds.pipes, pipe)
	ds.childs = append(ds.childs, nextDs)

	ds.source.Listen(context.Background(), ListenerFunc(func(ctx context.Context, d Data) error {
		var (
			processor   Processor
			processFunc = pipe
		)
		if keyed, ok := d.(KeyedData); ok {
			key := keyed.Key()
			processor, ok = keyedProcessors[key]
			if !ok {
				processor = f()
				keyedProcessors[key] = processor
			}
		} else {
			if singleProcessor == nil {
				singleProcessor = f()
			}
			processor = singleProcessor
		}

		return processor.Process(ctx, processFunc, d)
	}))
	return nextDs
}

func (ds *dataStream) Map(f MapperFactory, options ...DataStreamOption) DataStream {
	return ds.Process(func() Processor {
		return MapperFunc(f().Map)
	})
}

func (ds *dataStream) AddSink(f SinkFactory, options ...DataStreamOption) DataStream {
	return ds.Process(func() Processor {
		return SinkFunc(f().Handle)
	})
}

func (ds *dataStream) start(ctx context.Context) error {
	for _, p := range ds.pipes {
		if err := p.Start(ctx); err != nil {
			return err
		}
	}

	for _, child := range ds.childs {
		if err := child.start(ctx); err != nil {
			return err
		}
	}

	return nil
}

func newDataStream(source Channel, options ...DataStreamOption) *dataStream {
	ds := dataStream{
		name:           "",
		source:         source,
		channelFactory: DefaultChannelFactory,
		pipes:          []Pipe{},
	}

	for _, o := range options {
		o(&ds)
	}

	return &ds
}
