package stream

type (
	Process interface {
		Process(ExecutionContext, Data) error
	}

	Processor interface {
		Process(ExecutionContext, Process, Data) error
	}

	ProcessorFunc func(ExecutionContext, Process, Data) error

	ProcessorFactory interface {
		Type() Type
		Create() Processor
	}

	Mapper interface {
		Map(ExecutionContext, Data) (Data, error)
	}

	MapperFunc func(ExecutionContext, Data) (Data, error)

	MapperFactory func() Mapper

	FilterFunc func(ExecutionContext, Data) (bool, error)

	processorFactoryFunc struct {
		dataType Type
		f        func() Processor
	}
)

func (f ProcessorFunc) Process(ec ExecutionContext, p Process, d Data) error {
	return f(ec, p, d)
}

func (f MapperFunc) Map(ec ExecutionContext, d Data) (Data, error) {
	return f(ec, d)
}

func (f processorFactoryFunc) Type() Type {
	return f.dataType
}

func (f processorFactoryFunc) Create() Processor {
	return f.f()
}

func newProcessorFactoryFunc(dataType Type, f func() Processor) ProcessorFactory {
	return processorFactoryFunc{
		dataType: dataType,
		f:        f,
	}
}
