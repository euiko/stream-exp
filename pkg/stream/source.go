package stream

import (
	"bufio"
	"os"
	"time"
)

type (
	SourceSettings struct {
		channelFactory ChannelFactory
	}

	SourceOption func(*SourceSettings)
	Source       interface {
		Type() Type
		Open(ExecutionContext, ...SourceOption) error
		Close() error
		DataStream() DataStream
	}

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
func (s *SourceFile) Open(ec ExecutionContext, options ...SourceOption) error {
	var err error

	s.settings = newSourceSettings(options...)
	s.sink = s.settings.channelFactory()

	s.file, err = os.OpenFile(s.name, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	go func() {
		scanner := bufio.NewScanner(s.file)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			s.sink.Send(ec.Context(), String(time.Now(), scanner.Text()))
		}
	}()

	return nil
}

func (s *SourceFile) Close() error {
	return s.file.Close()
}
func (s *SourceFile) DataStream(options ...DataStreamOption) DataStream {
	return newDataStream(s.sink, s.Type(), options...)
}

func NewSourceFile(name string) *SourceFile {
	return &SourceFile{
		name: name,
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
