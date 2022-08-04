package stream

import "context"

type (
	KeyFunc func(Data) interface{}

	DataStreamOption func(*dataStream)

	DataStream interface {
		Type() Type
		Name() string

		KeyBy(KeyFunc, ...DataStreamOption) DataStream
		Filter(FilterFunc, ...DataStreamOption) DataStream
		Process(ProcessorFactory, ...DataStreamOption) DataStream
		Map(MapperFactory, ...DataStreamOption) DataStream
		AddSink(SinkFactory, ...DataStreamOption) DataStream
	}

	dataStream struct {
		dataType Type
		name     string

		executionContext ExecutionContext
		source           Channel
		channelFactory   ChannelFactory

		pipes []Pipe
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

func (ds *dataStream) Type() Type {
	return ds.dataType
}
func (ds *dataStream) Name() string {
	return ds.name
}
func (ds *dataStream) KeyBy(f KeyFunc, options ...DataStreamOption) DataStream {
	return ds.Process(newProcessorFactoryFunc(ds.dataType, func() Processor {
		return f
	}), options...)
}

func (ds *dataStream) Filter(f FilterFunc, options ...DataStreamOption) DataStream {
	return ds.Process(newProcessorFactoryFunc(ds.dataType, func() Processor {
		return f
	}), options...)
}

func (ds *dataStream) Process(f ProcessorFactory, options ...DataStreamOption) DataStream {
	var (
		keyedProcessors = make(map[interface{}]Processor)
		singleProcessor Processor
	)

	source, sink := ds.channelFactory(), ds.channelFactory()
	pipe := newPipe(source, sink)
	ds.pipes = append(ds.pipes, pipe)
	nextDs := newDataStream(sink, f.Type(), options...)

	ds.source.Listen(context.Background(), ListenerFunc(func(ctx context.Context, d Data) error {
		var (
			processor   Processor
			processFunc = pipe
		)
		if keyed, ok := d.(KeyedData); ok {
			key := keyed.Key()
			processor, ok = keyedProcessors[key]
			if !ok {
				processor = f.Create()
				keyedProcessors[key] = processor
			}
		} else {
			if singleProcessor == nil {
				singleProcessor = f.Create()
			}
			processor = singleProcessor
		}

		return processor.Process(ds.executionContext, processFunc, d)
	}))
	return nextDs
}

func (ds *dataStream) Map(f MapperFactory, options ...DataStreamOption) DataStream {
	return nil
}

func (ds *dataStream) AddSink(f SinkFactory, options ...DataStreamOption) DataStream {
	return nil
}

func newDataStream(source Channel, dataType Type, options ...DataStreamOption) *dataStream {
	ds := dataStream{
		dataType:       dataType,
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
