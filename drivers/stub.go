package drivers

import "bytes"

type Stub struct {
	counter int
	buff    *bytes.Buffer
}

func NewStub() Driver {
	return &Stub{
		buff: bytes.NewBuffer(nil),
	}
}

func (so *Stub) Write(e []byte) (int, error) {
	so.counter++
	return so.buff.Write(e)
}

func (so *Stub) Close() error {
	return nil
}

func (so *Stub) Counter() int {
	return so.counter
}

func (so *Stub) Data() []byte {
	return so.buff.Bytes()
}
