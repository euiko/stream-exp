package stream

type (
	EqualSupport interface {
		Equal(o interface{}) bool
	}

	KeyFunc func(Data) EqualSupport

	DataStream interface {
		Type() Type
		Name() string

		KeyBy(KeyFunc) DataStream
		Filter(FilterFunc) DataStream
		Process(ProcessorFactory) DataStream
		Map(MapperFactory) DataStream

		Sinks() []SinkFactory
		AddSink(SinkFactory) DataStream
	}

	dataStream struct {
		factories []SinkFactory
		sinks     []Sink
	}
)
