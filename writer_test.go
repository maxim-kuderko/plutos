package plutos

import (
	"github.com/maxim-kuderko/plutos/drivers"
	"sync"
	"testing"
)

func TestWriter_SingleWrite(t *testing.T) {
	stub := drivers.NewStub()
	tester := NewWriter(func() drivers.Driver {
		return stub
	})
	e := Event{
		RawData: []byte(`{"test": "me"}`),
		Enrichment: Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	tester.Write(e)
	if stub.(*drivers.Stub).Counter() != 1 {
		t.Fail()
	}
}

func TestWriter_MultiWrite(t *testing.T) {
	stub := drivers.NewStub()
	tester := NewWriter(func() drivers.Driver {
		return stub
	})
	e := Event{
		RawData: []byte(`{"test": "me"}`),
		Enrichment: Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	times := 1000
	for i := 0; i < times; i++ {
		tester.Write(e)
	}
	if stub.(*drivers.Stub).Counter() != times {
		t.Fail()
	}
}

func TestWriter_ConcurrentMultiWrite(t *testing.T) {
	stub := drivers.NewStub()
	tester := NewWriter(func() drivers.Driver {
		return stub
	})
	e := Event{
		RawData: []byte(`{"test": "me"}`),
		Enrichment: Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	times := 1000
	wg := sync.WaitGroup{}
	wg.Add(times)
	for i := 0; i < times; i++ {
		go func() {
			defer wg.Done()
			tester.Write(e)
		}()
	}
	wg.Wait()
	if stub.(*drivers.Stub).Counter() != times {
		t.Fail()
	}
}

func BenchmarkWriter_Write(b *testing.B) {
	b.ReportAllocs()
	tester := NewWriter(drivers.NewStub)
	e := Event{
		RawData: []byte(`{"test": "me"}`),
		Enrichment: Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tester.Write(e)
	}
}
