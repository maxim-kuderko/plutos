package drivers

import (
	"bufio"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/pierrec/lz4/v4"
	"io"
	"sync"
)

type compressor struct {
	origWriter Driver
	w          Driver
	buff       *bufio.Writer
}

var bufioWriterPool = sync.Pool{New: func() interface{} { return bufio.NewWriterSize(io.Discard, 1<<8*5) }}

func NewCompressor(t string, w func() Driver) Driver {
	orig := w()
	ww := compressorFactory(t, orig)
	buff := bufioWriterPool.Get().(*bufio.Writer)
	buff.Reset(ww)
	return &compressor{
		origWriter: orig,
		w:          ww,
		buff:       buff,
	}
}

func (g *compressor) Write(b []byte) (int, error) {
	return g.buff.Write(b)
}

func (g *compressor) Close() error {
	defer func() {
		bufioWriterPool.Put(g.buff)
	}()
	if err := g.buff.Flush(); err != nil {
		return err
	}
	if g.origWriter != g.w {
		if err := g.w.Close(); err != nil {
			return err
		}
	}
	return g.origWriter.Close()
}

func compressorFactory(t string, orig Driver) Driver {
	switch t {
	case `gzip`, `gz`:
		return pgzip.NewWriter(orig)
	case `lz4`:
		return lz4.NewWriter(orig)
	case `snappy`:
		return snappy.NewBufferedWriter(orig)
	case `zstd`:
		w, _ := zstd.NewWriter(orig)
		return w
	case `none`:
		return orig
	default:
		return orig
	}
}
