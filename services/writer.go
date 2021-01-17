package services

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/maxim-kuderko/plutos/entities"
	"io"
)

type Writer struct {
	driver io.WriteCloser
}

func NewWriter(d io.WriteCloser) *Writer {
	return &Writer{driver: d}
}

func (w *Writer) Write(e entities.Event) error {
	return jsoniter.ConfigFastest.NewEncoder(w.driver).Encode(e)

}

func (w *Writer) Close() error {
	return w.driver.Close()
}
