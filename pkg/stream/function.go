package stream

type (
	ProcessFunc func(ExecutionContext, Data) error

	Processor interface {
		Process(ExecutionContext, ProcessFunc, Data) error
	}

	ProcessorFunc func(ExecutionContext, ProcessFunc, Data) error

	ProcessorFactory func() Processor

	Mapper interface {
		Map(ExecutionContext, Data) (Data, error)
	}

	MapperFunc func(ExecutionContext, Data) (Data, error)

	MapperFactory func() Mapper

	FilterFunc func(ExecutionContext, Data) (bool, error)
)

func (f ProcessorFunc) Process(ec ExecutionContext, p ProcessFunc, d Data) error {
	return p(ec, d)
}

func (f MapperFunc) Map(ec ExecutionContext, d Data) (Data, error) {
	return f(ec, d)
}
