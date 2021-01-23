package services

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/maxim-kuderko/plutos/entities"
	"github.com/maxim-kuderko/plutos/services/drivers"
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
		newDrv := w.newDriver()
		w.mu.Lock()
		if w.currentSize == 0 {
			w.mu.Unlock()
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
		w.mu.Unlock()
	}
}

func (w *Writer) Write(e entities.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := jsoniter.ConfigFastest.NewEncoder(w.driver).Encode(e)
	if err != nil {
		return err
	}
	w.currentSize++
	return nil

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
