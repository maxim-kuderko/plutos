package drivers

import (
	gzip "github.com/klauspost/compress"
)

type compressor struct {
	origWriter Driver
	w          Driver
}

func NewCompressor(w func() Driver) (Driver, error) {
	orig := w()
	gw := gzip.NewWriter(orig)
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
