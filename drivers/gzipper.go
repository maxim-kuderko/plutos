package drivers

import (
	"github.com/klauspost/pgzip"
	"os"
	"runtime"
	"strconv"
)

var (
	lvl, _ = strconv.Atoi(os.Getenv(`GZIP_LVL`))
)

type gzipper struct {
	origWriter Driver
	w          *pgzip.Writer
}

var numCpus = runtime.GOMAXPROCS(0)

func NewGzipper(w func() Driver) (Driver, error) {
	orig := w()
	gw, err := pgzip.NewWriterLevel(orig, lvl)
	if err != nil {
		return nil, err
	}
	err = gw.SetConcurrency(5<<20, numCpus+6)
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
