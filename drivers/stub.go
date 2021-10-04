package drivers

import (
	"bytes"
	"strings"
	"sync"
)

type Stub struct {
	mu   sync.Mutex
	buff *bytes.Buffer
}

func NewStub() Driver {
	return &Stub{
		buff: bytes.NewBuffer(nil),
	}
}

func (so *Stub) Write(e []byte) (int, error) {
	so.mu.Lock()
	defer so.mu.Unlock()
	return so.buff.Write(e)
}

func (so *Stub) Close() error {
	return nil
}

func (so *Stub) Counter() int {
	so.mu.Lock()
	defer so.mu.Unlock()
	return len(strings.Split(so.buff.String(), "\n"))
}

func (so *Stub) Data() []byte {
	so.mu.Lock()
	defer so.mu.Unlock()
	return so.buff.Bytes()
}
