package stream

import "context"

type (
	Subscription interface {
		Handle(context.Context, Data) error
	}

	SubscriptionFunc func(context.Context, Data) error

	Channel interface {
		Publish(context.Context, Data) error
		Subscribe(context.Context, Subscription) error
	}

	DirectChannel struct {
		subscriptions []Subscription
	}

	Pipe interface {
		Start(context.Context) error
		Stop() error
		Process(ExecutionContext, Data) error
	}

	pipe struct {
		ctx    context.Context
		cancel func()

		source Channel
		sink   Channel
	}
)

func (f SubscriptionFunc) Handle(ctx context.Context, d Data) error {
	return f(ctx, d)
}

func (p *pipe) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)
	return p.source.Subscribe(p.ctx, SubscriptionFunc(p.onDataReceived))
}

func (p *pipe) Stop() error {
	p.cancel()
	return nil
}

func (p *pipe) Process(ec ExecutionContext, data Data) error {
	return p.source.Publish(p.ctx, data)
}

func (p *pipe) onDataReceived(ctx context.Context, data Data) error {
	return p.sink.Publish(p.ctx, data)
}

func newPipe(source Channel, sink Channel) Pipe {
	return &pipe{
		ctx:    nil,
		cancel: nil,

		source: source,
		sink:   sink,
	}
}
