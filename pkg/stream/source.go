package stream

type (
	Source interface {
		Type() Type
		Open(ExecutionContext) error
	}
)
