package drivers

type Stub struct {
	counter int
}

func NewStub() Driver {
	return &Stub{}
}

func (so *Stub) Write(e []byte) (int, error) {
	so.counter++
	return len(e), nil
}

func (so *Stub) Close() error {
	return nil
}

func (so *Stub) Counter() int {
	return so.counter
}
