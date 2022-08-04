package stream

func (f KeyFunc) Process(ec ExecutionContext, p Process, d Data) error {
	return p.Process(ec, newKeyedData(f(d), d))
}

func (f FilterFunc) Process(ec ExecutionContext, p Process, d Data) error {
	filtered, err := f(ec, d)
	if err != nil {
		return err
	}

	if filtered {
		return nil
	}

	return p.Process(ec, d)
}