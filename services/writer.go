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

var SEPERATOR = byte('\n')

func (w *Writer) Write(e entities.Event) error {
	b, err := jsoniter.ConfigFastest.Marshal(e)
	if err != nil {
		return err
	}
	_, err = w.driver.Write(append(b, SEPERATOR))
	return err

}

func (w *Writer) Close() error {
	return w.driver.Close()
}
