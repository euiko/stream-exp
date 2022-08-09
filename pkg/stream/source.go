package stream

import (
	"bufio"
	"context"
	"os"
)

type (
	SourceSettings struct {
		channelFactory ChannelFactory
	}

	SourceOption func(*SourceSettings)
	Source       interface {
		Type() Type
		Open(context.Context) error
		Close() error
		Sink() Channel
	}

	SourceFactory func(...SourceOption) Source

	SourceFile struct {
		settings SourceSettings
		name     string
		sink     Channel

		file *os.File
	}
)

func (s *SourceFile) Type() Type {
	return StringType
}
func (s *SourceFile) Open(ctx context.Context) error {
	var err error

	s.file, err = os.OpenFile(s.name, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	go func() {
		scanner := bufio.NewScanner(s.file)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			s.sink.Send(ctx, String(scanner.Text()))
		}
	}()

	return nil
}

func (s *SourceFile) Close() error {
	return s.file.Close()
}
func (s *SourceFile) Sink() Channel {
	return s.sink
}

func NewSourceFile(name string, options ...SourceOption) *SourceFile {
	s := SourceFile{
		name:     name,
		settings: newSourceSettings(options...),
	}

	s.sink = s.settings.channelFactory()
	return &s
}

func SourceFileFactory(name string) SourceFactory {
	return func(options ...SourceOption) Source {
		return NewSourceFile(name, options...)
	}
}

func newSourceSettings(options ...SourceOption) SourceSettings {
	settings := SourceSettings{
		channelFactory: DefaultChannelFactory,
	}

	for _, o := range options {
		o(&settings)
	}

	return settings
}
