package drivers

import (
	"bytes"
	"sync"
)

type Stub struct {
	counter int
	mu      sync.Mutex
	buff    *bytes.Buffer
}

func NewStub() Driver {
	return &Stub{
		buff: bytes.NewBuffer(nil),
	}
}

func (so *Stub) Write(e []byte) (int, error) {
	so.mu.Lock()
	defer so.mu.Unlock()
	so.counter++
	return so.buff.Write(e)
}

func (so *Stub) Close() error {
	return nil
}

func (so *Stub) Counter() int {
	so.mu.Lock()
	defer so.mu.Unlock()
	return so.counter
}

func (so *Stub) Data() []byte {
	so.mu.Lock()
	defer so.mu.Unlock()
	return so.buff.Bytes()
}
