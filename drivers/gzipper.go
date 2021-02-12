package drivers

import (
	"github.com/klauspost/pgzip"
	"io"
	"os"
	"runtime"
	"strconv"
)

var (
	lvl, _ = strconv.Atoi(os.Getenv(`GZIP_LVL`))
)

type Compressor struct {
	origWriter Driver
	w          io.WriteCloser
}

func NewCompressor(w func() Driver) (Driver, error) {
	orig := w()
	gw, _ := pgzip.NewWriterLevel(orig, lvl)
	gw.SetConcurrency(1<<20, runtime.NumCPU()*2)
	return &Compressor{
		origWriter: orig,
		w:          gw,
	}, nil
}

func (g *Compressor) Write(b []byte) (int, error) {
	return g.w.Write(b)
}

func (g *Compressor) Close() error {
	if err := g.w.Close(); err != nil {
		return err
	}

	return g.origWriter.Close()
}
