package drivers

import (
	"bufio"
	"github.com/pierrec/lz4"
	"runtime"
)

type compressor struct {
	origWriter Driver
	buff       *bufio.Writer
	w          Driver
}

func NewCompressor(w func() Driver) (Driver, error) {
	orig := w()
	gw := lz4.NewWriter(orig)
	gw.Header = lz4.Header{
		BlockChecksum:    false,
		BlockMaxSize:     4 << 20,
		NoChecksum:       true,
		CompressionLevel: 9,
	}
	gw.WithConcurrency(runtime.NumCPU())
	buff := bufio.NewWriterSize(gw, 4<<20)
	return &compressor{
		origWriter: orig,
		buff:       buff,
		w:          gw,
	}, nil
}

func (g *compressor) Write(b []byte) (int, error) {
	return g.buff.Write(b)
}

func (g *compressor) Close() error {
	if err := g.buff.Flush(); err != nil {
		return err
	}
	if err := g.w.Close(); err != nil {
		return err
	}

	return g.origWriter.Close()
}
