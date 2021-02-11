package drivers

import (
	"bufio"
	"github.com/klauspost/pgzip"
	"io"
	"os"
	"strconv"
)

var (
	lvl, _ = strconv.Atoi(os.Getenv(`GZIP_LVL`))
)

type Compressor struct {
	origWriter Driver
	buff       *bufio.Writer
	w          io.WriteCloser
}

func NewCompressor(w func() Driver) (Driver, error) {
	orig := w()
	buff := bufio.NewWriterSize(orig, 10<<20)
	gw, _ := pgzip.NewWriterLevel(buff, lvl)
	return &Compressor{
		origWriter: orig,
		buff:       buff,
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
	if err := g.buff.Flush(); err != nil {
		return err
	}

	return g.origWriter.Close()
}
