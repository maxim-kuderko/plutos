package drivers

import (
	"github.com/valyala/gozstd"
)

type compressor struct {
	origWriter Driver
	w          Driver
}

func NewCompressor(w func() Driver) (Driver, error) {
	orig := w()
	gw := gozstd.NewWriterLevel(orig, 1)
	return &compressor{
		origWriter: orig,
		w:          gw,
	}, nil
}

func (g *compressor) Write(b []byte) (int, error) {
	return g.w.Write(b)
}

func (g *compressor) Close() error {
	if err := g.w.Close(); err != nil {
		return err
	}

	return g.origWriter.Close()
}
