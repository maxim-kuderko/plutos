package drivers

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"strconv"
	"sync"
)

type gzipper struct {
	origWriter io.WriteCloser
	w          io.WriteCloser
}

func (g *gzipper) Write(b []byte) (int, error) {
	return g.w.Write(b)
}

func (g *gzipper) Close() error {
	if err := g.w.Close(); err != nil {
		return err
	}
	return g.origWriter.Close()
}

var (
	lvl, _   = strconv.Atoi(os.Getenv(`GZIP_LVL`))
	gzipPool = &sync.Pool{New: func() interface{} {
		if lvl == 0 {
			panic(`gzip lvl is not set or 0`)
		}
		gw, _ := gzip.NewWriterLevel(bytes.NewBuffer(nil), lvl)

		return gw
	}}
)

func newGzipper(w io.WriteCloser) (io.WriteCloser, error) {
	gw := gzipPool.Get().(*gzip.Writer)
	defer gzipPool.Put(gw)
	gw.Reset(w)
	return &gzipper{
		origWriter: w,
		w:          gw,
	}, nil
}
