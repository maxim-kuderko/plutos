package plutos

import (
	"github.com/maxim-kuderko/plutos/drivers"
	"os"
	"strconv"
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

var (
	maxTime, _        = strconv.Atoi(os.Getenv(`MAX_BUFFER_TIME_MILLISECONDS`))
	enableCompression = os.Getenv(`ENABLE_COMPRESSION`)
)

func NewWriter(d func() drivers.Driver) *Writer {
	selectedDriver := d
	if enableCompression == `true` {
		compressed := func() drivers.Driver {
			t, _ := drivers.NewCompressor(d)
			return t
		}
		selectedDriver = compressed
	}
	w := &Writer{driver: selectedDriver(), newDriver: selectedDriver}
	go w.periodicFlush(maxTime)
	return w
}

const DEFAULT_MAX_TIME = 60

func (w *Writer) periodicFlush(t int) {
	if t <= 0 {
		t = DEFAULT_MAX_TIME
	}
	ticker := time.NewTicker(time.Duration(t) * time.Millisecond)
	for range ticker.C {
		w.flush()
	}
}

func (w *Writer) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentSize == 0 {
		return
	}
	newDrv := w.newDriver()
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
