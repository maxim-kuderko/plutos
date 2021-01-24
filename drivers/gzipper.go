package drivers

import (
	"github.com/klauspost/pgzip"
	"os"
	"strconv"
)

var (
	lvl, _ = strconv.Atoi(os.Getenv(`GZIP_LVL`))
)

type gzipper struct {
	origWriter Driver
	w          *pgzip.Writer
}

func NewGzipper(w func() Driver) (Driver, error) {
	orig := w()
	gw, err := pgzip.NewWriterLevel(orig, lvl)
	if err != nil {
		return nil, err
	}
	return &gzipper{
		origWriter: orig,
		w:          gw,
	}, nil
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