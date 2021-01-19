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
	lastFlushed time.Time
	newDriver   func() drivers.Driver
	driver      drivers.Driver

	mu sync.Mutex
	wg sync.WaitGroup
}

var (
	maxTime, _ = strconv.Atoi(os.Getenv(`MAX_BUFFER_TIME_SECONDS`))
)

func NewWriter(d func() drivers.Driver) *Writer {
	selectedDriver := d
	if os.Getenv(`ENABLE_GZIP`) == `true` {
		compressed := func() drivers.Driver {
			t, _ := drivers.NewGzipper(d)
			return t
		}
		selectedDriver = compressed
	}
	w := &Writer{driver: selectedDriver(), lastFlushed: time.Now(), newDriver: selectedDriver}
	go w.periodicFlush()
	return w
}

func (w *Writer) periodicFlush() {
	ticker := time.NewTicker(time.Second)
	maxTimeBetweenFlushes := time.Duration(maxTime) * time.Second
	for range ticker.C {
		w.mu.Lock()
		if time.Since(w.lastFlushed) > maxTimeBetweenFlushes && w.currentSize > 0 {
			tmp := w.driver
			w.driver = w.newDriver()
			w.lastFlushed = time.Now()
			w.currentSize = 0
			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				tmp.Close()
			}()
		}
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
