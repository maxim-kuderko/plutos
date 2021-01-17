package services

import (
	"bytes"
	jsoniter "github.com/json-iterator/go"
	"github.com/maxim-kuderko/plutos/entities"
	"io"
	"sync"
)

type Writer struct {
	bufferPools *sync.Pool
	driver      io.WriteCloser
}

func NewWriter(d io.WriteCloser) *Writer {
	return &Writer{driver: d, bufferPools: &sync.Pool{New: func() interface{} {
		return bytes.NewBuffer(nil)
	}}}
}

func (w *Writer) Write(e entities.Event) error {
	return jsoniter.ConfigFastest.NewEncoder(w.driver).Encode(e)

}

func (w *Writer) Close() error {
	return w.driver.Close()
}
