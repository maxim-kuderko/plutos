package plutos

import (
	"bytes"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/gzip"
	"github.com/maxim-kuderko/plutos/drivers"
	"io/ioutil"
	"strings"
	"sync"
	"testing"
	"time"
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
	jsoniter.ConfigFastest.NewEncoder(tester).Encode(e)
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
		jsoniter.ConfigFastest.NewEncoder(tester).Encode(e)
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
			jsoniter.ConfigFastest.NewEncoder(tester).Encode(e)
		}()
	}
	wg.Wait()
	if stub.(*drivers.Stub).Counter() != times {
		t.Fail()
	}
}

func TestWriter_ConcurrentMultiWriteGzip(t *testing.T) {
	stub := drivers.NewStub()
	enableCompression = `true`
	tester := NewWriter(func() drivers.Driver {
		return stub
	})
	e := Event{
		RawData: []byte(`{"test": "me"}`),
		Enrichment: Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	times := 10
	wg := sync.WaitGroup{}
	wg.Add(times)
	for i := 0; i < times; i++ {
		go func() {
			defer wg.Done()
			jsoniter.ConfigFastest.NewEncoder(tester).Encode(e)
		}()
	}
	wg.Wait()
	tester.Close()
	time.Sleep(time.Second)
	r, err := gzip.NewReader(bytes.NewBuffer(stub.(*drivers.Stub).Data()))
	if err != nil {
		panic(err)
	}
	data, _ := ioutil.ReadAll(r)
	if len(strings.Split(string(data), "\n")) != times+1 {
		fmt.Println(len(strings.Split(string(data), "\n")))
		t.Fail()
	}
}

func TestWriter_ConcurrentMultiWriteFLUSH(t *testing.T) {
	stub := drivers.NewStub()
	enableCompression = `true`
	maxTime = 100
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
			jsoniter.ConfigFastest.NewEncoder(tester).Encode(e)
		}()
	}
	wg.Wait()
	tester.Close()
	time.Sleep(time.Second)

	r, _ := gzip.NewReader(bytes.NewBuffer(stub.(*drivers.Stub).Data()))
	data, _ := ioutil.ReadAll(r)
	if len(strings.Split(string(data), "\n")) != times+1 {
		fmt.Println(len(strings.Split(string(data), "\n")))
		t.Fail()
	}
}

func BenchmarkWriter_Write(b *testing.B) {
	b.ReportAllocs()
	tester := NewWriter(drivers.NewDiscard)
	e := Event{
		RawData: []byte(`{"test": "me"}`),
		Enrichment: Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	data, _ := jsoniter.ConfigFastest.Marshal(e)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tester.Write(data)
	}
}

func BenchmarkWriter_WriteCompress(b *testing.B) {
	b.ReportAllocs()
	tester := NewWriter(drivers.NewDiscard)
	e := Event{
		RawData: []byte(`{"test": "me"}`),
		Enrichment: Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	enableCompression = `true`
	data, _ := jsoniter.ConfigFastest.Marshal(e)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tester.Write(data)
	}
}
