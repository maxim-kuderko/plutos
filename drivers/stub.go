package drivers

import (
	"bytes"
	"go.uber.org/atomic"
)

type Stub struct {
	counter atomic.Int32
	buff    *bytes.Buffer
}

func NewStub() Driver {
	return &Stub{
		buff: bytes.NewBuffer(nil),
	}
}

func (so *Stub) Write(e []byte) (int, error) {
	so.counter.Add(1)
	return so.buff.Write(e)
}

func (so *Stub) Close() error {
	return nil
}

func (so *Stub) Counter() int {
	return int(so.counter.Load())
}

func (so *Stub) Data() []byte {
	return so.buff.Bytes()
}
