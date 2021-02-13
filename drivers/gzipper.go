package drivers

import (
	"github.com/pierrec/lz4"
)

type compressor struct {
	origWriter Driver
	w          Driver
}

func NewCompressor(w func() Driver) (Driver, error) {
	orig := w()
	gw := lz4.NewWriter(orig)
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
