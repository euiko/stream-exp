package stream

import "context"

type (
	Listener interface {
		Handle(context.Context, Data) error
	}

	ListenerFunc func(context.Context, Data) error

	Pipe interface {
		Start(context.Context) error
		Stop() error
		Process(context.Context, Data) error
	}

	pipe struct {
		ctx    context.Context
		cancel func()

		source Channel
		sink   Channel
	}
)

func (f ListenerFunc) Handle(ctx context.Context, d Data) error {
	return f(ctx, d)
}

func (p *pipe) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)
	return p.source.Listen(p.ctx, ListenerFunc(p.onDataReceived))
}

func (p *pipe) Stop() error {
	p.cancel()
	return nil
}

func (p *pipe) Process(ctx context.Context, data Data) error {
	return p.source.Send(p.ctx, data)
}

func (p *pipe) onDataReceived(ctx context.Context, data Data) error {
	return p.sink.Send(p.ctx, data)
}

func newPipe(source Channel, sink Channel) Pipe {
	return &pipe{
		ctx:    nil,
		cancel: nil,

		source: source,
		sink:   sink,
	}
}
