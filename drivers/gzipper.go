package drivers

import (
	"github.com/golang/snappy"
	"io"
	"os"
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
	gw := snappy.NewBufferedWriter(orig)
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
