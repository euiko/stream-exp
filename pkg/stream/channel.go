package stream

import "context"

type (
	Channel interface {
		Send(context.Context, Data) error
		Listen(context.Context, Listener) error
	}

	ChannelFactory func() Channel

	DirectChannel struct {
		listeners []Listener
	}
)

var (
	DefaultChannelFactory = DirectChannelFactory()

	channelRegistry = make(map[string]ChannelFactory)
)

func getChannelFactory(name string) ChannelFactory {
	if f, ok := channelRegistry[name]; ok {
		return f
	}

	return DefaultChannelFactory
}

func (dc *DirectChannel) Send(ctx context.Context, d Data) error {
	for _, l := range dc.listeners {
		if err := l.Handle(ctx, d); err != nil {
			return err
		}
	}

	return nil
}

func (dc *DirectChannel) Listen(ctx context.Context, listener Listener) error {
	dc.listeners = append(dc.listeners, listener)
	return nil
}

func NewDirectChannel() *DirectChannel {
	return &DirectChannel{}
}

func DirectChannelFactory() ChannelFactory {
	return func() Channel {
		return NewDirectChannel()
	}
}

func init() {
	channelRegistry[""] = DirectChannelFactory()
	channelRegistry["direct"] = DirectChannelFactory()
}
