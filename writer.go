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
	maxTime, _ = strconv.Atoi(os.Getenv(`MAX_BUFFER_TIME_MILLISECONDS`))
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

const DEFAULT_MAX_TIME = 60

func (w *Writer) periodicFlush() {
	if maxTime <= 0 {
		maxTime = DEFAULT_MAX_TIME
	}
	ticker := time.NewTicker(time.Duration(maxTime) * time.Millisecond)
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
	/*defer func() {
		if err == nil {
			w.currentSize++
		}
	}()
	return w.driver.Write(b)*/
	return 0, err
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
