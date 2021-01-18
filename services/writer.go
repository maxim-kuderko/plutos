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
	driver      drivers.Driver

	mu sync.Mutex
}

var (
	maxTime, _ = strconv.Atoi(os.Getenv(`MAX_BUFFER_TIME_SECONDS`))
)

func NewWriter(d drivers.Driver) *Writer {
	w := &Writer{driver: d}
	go w.periodicFlush()
	return w
}

func (w *Writer) periodicFlush() {
	ticker := time.NewTicker(time.Second)
	maxTimeBetweenFlushes := time.Duration(maxTime) * time.Second
	for range ticker.C {
		w.mu.Lock()
		if time.Since(w.lastFlushed) > maxTimeBetweenFlushes && w.currentSize > 0 {
			w.driver.Flush()
			w.lastFlushed = time.Now()
			w.currentSize = 0
		}
		w.mu.Unlock()
	}
}

func (w *Writer) Write(e entities.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := jsoniter.ConfigFastest.NewEncoder(w.driver).Encode(e)
	w.currentSize++
	return err

}

func (w *Writer) Close() error {
	return w.driver.Close()
}
