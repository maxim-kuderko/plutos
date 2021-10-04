package plutos

import (
	"github.com/maxim-kuderko/plutos/drivers"
	"sync"
	"time"
)

type Writer struct {
	currentSize int
	newDriver   func() drivers.Driver
	driver      drivers.Driver

	mu sync.Mutex
	wg sync.WaitGroup
}

type Config struct {
	CompressionType string
	BufferTime      time.Duration
}

func NewWriter(cfg *Config, d func() drivers.Driver) *Writer {
	selectedDriver := func() drivers.Driver {
		return drivers.NewCompressor(cfg.CompressionType, d)
	}
	w := &Writer{driver: selectedDriver(), newDriver: selectedDriver}
	go w.periodicFlush(cfg.BufferTime)
	return w
}

const DEFAULT_MAX_TIME = 60 * time.Second

func (w *Writer) periodicFlush(t time.Duration) {
	if t <= 0 {
		t = DEFAULT_MAX_TIME
	}
	ticker := time.NewTicker(t)
	for range ticker.C {
		w.flush()
	}
}

func (w *Writer) flush() {
	newDrv := w.newDriver()
	w.mu.Lock()
	defer w.mu.Unlock()
	tmp := w.driver
	w.driver = newDrv
	w.currentSize = 0
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		tmp.Close()
	}()
}

func (w *Writer) Write(b []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer func() {
		if err == nil {
			w.currentSize++
		}
	}()
	return w.driver.Write(b)

}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer w.wg.Wait()
	if w.currentSize == 0 {
		return nil
	}
	return w.driver.Close()
}
