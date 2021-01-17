package drivers

import (
	"compress/gzip"
	"io"
	"os"
	"strconv"
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

func newGzipper(w io.WriteCloser) (io.WriteCloser, error) {
	lvl, err := strconv.Atoi(os.Getenv(`GZIP_LVL`))
	if err != nil {
		panic(`GZIP_LVL is empty`)
	}
	gw, err := gzip.NewWriterLevel(w, lvl)
	if err != nil {
		return nil, err
	}
	return &gzipper{
		origWriter: w,
		w:          gw,
	}, nil
}
