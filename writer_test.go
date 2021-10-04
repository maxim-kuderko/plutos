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
	tester := NewWriter(&Config{
		CompressionType: "",
		BufferTime:      time.Second,
	}, func() drivers.Driver {
		return stub
	})
	e := Event{
		RawData: []byte(`{"test": "me"}`),
		Enrichment: Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	jsoniter.ConfigFastest.NewEncoder(tester).Encode(e)
	tester.Close()
	if stub.(*drivers.Stub).Counter() != 2 { // one for \n
		t.Fail()
	}
}

func TestWriter_MultiWrite(t *testing.T) {
	stub := drivers.NewStub()
	tester := NewWriter(&Config{
		CompressionType: "",
		BufferTime:      time.Second,
	}, func() drivers.Driver {
		return stub
	})
	e := Event{
		RawData: []byte(`{"test": "me"}`),
		Enrichment: Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	times := 1000
	encr := jsoniter.ConfigFastest.NewEncoder(tester)
	for i := 0; i < times; i++ {
		encr.Encode(e)
	}

	tester.Close()
	if c := stub.(*drivers.Stub).Counter(); c != times+1 {
		t.Fail()
	}
}

func TestWriter_ConcurrentMultiWrite(t *testing.T) {
	stub := drivers.NewStub()
	tester := NewWriter(&Config{
		CompressionType: "",
		BufferTime:      time.Second,
	}, func() drivers.Driver {
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
	if stub.(*drivers.Stub).Counter() != times+1 {
		t.Fail()
	}
}

func TestWriter_ConcurrentMultiWriteGzip(t *testing.T) {
	stub := drivers.NewStub()
	tester := NewWriter(&Config{
		CompressionType: "gzip",
		BufferTime:      time.Second,
	}, func() drivers.Driver {
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

	tester := NewWriter(&Config{
		CompressionType: "gzip",
		BufferTime:      time.Second / 2,
	}, func() drivers.Driver {
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
	tester := NewWriter(&Config{
		CompressionType: "",
		BufferTime:      time.Second,
	}, drivers.NewDiscard)
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

func BenchmarkWriter_WriteCompressGZIP(b *testing.B) {
	b.ReportAllocs()
	tester := NewWriter(&Config{
		CompressionType: "gzip",
		BufferTime:      time.Second,
	}, drivers.NewDiscard)
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

func BenchmarkWriter_WriteCompressLZ4(b *testing.B) {
	b.ReportAllocs()
	tester := NewWriter(&Config{
		CompressionType: "lz4",
		BufferTime:      time.Second,
	}, drivers.NewDiscard)
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

func BenchmarkWriter_WriteCompressZSTD(b *testing.B) {
	b.ReportAllocs()
	tester := NewWriter(&Config{
		CompressionType: "zstd",
		BufferTime:      time.Second,
	}, drivers.NewDiscard)
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
