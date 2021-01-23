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
	maxTime, _ = strconv.Atoi(os.Getenv(`MAX_BUFFER_TIME_SECONDS`))
	enableGzip = os.Getenv(`ENABLE_GZIP`)
)

func NewWriter(d func() drivers.Driver) *Writer {
	selectedDriver := d
	if enableGzip == `true` {
		compressed := func() drivers.Driver {
			t, _ := drivers.NewGzipper(d)
			return t
		}
		selectedDriver = compressed
	}
	w := &Writer{driver: selectedDriver(), newDriver: selectedDriver}
	go w.periodicFlush()
	return w
}

func (w *Writer) periodicFlush() {
	ticker := time.NewTicker(time.Duration(maxTime) * time.Second)
	for range ticker.C {
		w.flush()
	}
}

func (w *Writer) flush() {
	newDrv := w.newDriver()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentSize == 0 {
		return
	}
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
