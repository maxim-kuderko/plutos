package services

import (
	"bytes"
	jsoniter "github.com/json-iterator/go"
	"github.com/maxim-kuderko/plutos/entities"
	"io"
	"math/rand"
	"runtime"
	"sync"
)

type Writer struct {
	bufferPools []*sync.Pool
	driver      io.WriteCloser
}

func NewWriter(d io.WriteCloser) *Writer {
	poolSize := runtime.NumCPU()
	pools := make([]*sync.Pool, 0, poolSize)
	for i := 0; i <= poolSize; i++ {
		pools = append(pools, &sync.Pool{New: func() interface{} {
			return bytes.NewBuffer(nil)
		}})
	}
	return &Writer{driver: d, bufferPools: pools}
}

var SEPERATOR = byte('\n')

func (w *Writer) Write(e entities.Event) error {
	r := rand.Int() % len(w.bufferPools)
	buff := w.bufferPools[r].Get().(*bytes.Buffer)
	defer func() {
		buff.Reset()
		w.bufferPools[r].Put(buff)
	}()
	err := jsoniter.ConfigFastest.NewEncoder(buff).Encode(e)
	if err != nil {
		return err
	}
	buff.WriteByte(SEPERATOR)
	_, err = w.driver.Write(buff.Bytes())
	return err

}

func (w *Writer) Close() error {
	return w.driver.Close()
}
