package main

import (
	"fmt"
	"github.com/maxim-kuderko/plutos/entities"
	"github.com/maxim-kuderko/plutos/services"
	"github.com/maxim-kuderko/plutos/services/drivers"
	"github.com/qiangxue/fasthttp-routing"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fasthttp"
	"go.uber.org/atomic"
	"io"
	"os"
	"os/signal"
)

var driverRegistry = map[string]func() io.WriteCloser{
	`stdout`: func() io.WriteCloser {
		return &drivers.StdOut{}
	},
	`s3`: drivers.NewS3,
}

func main() {
	log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.ErrorLevel)
	log.Err(fmt.Errorf("test"))
	healthy := atomic.NewBool(true)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	router := routing.New()
	writer := services.NewWriter(fetchDriver()())
	defineRoutes(router, healthy, writer)

	go func() {
		log.Err(fasthttp.ListenAndServe(":8080", router.HandleRequest))
	}()
	<-c
	writer.Close()

}

func fetchDriver() func() io.WriteCloser {
	driver, ok := driverRegistry[os.Getenv(`DRIVER`)]
	if !ok {
		panic(`driver not found`)
	}
	return driver
}

func defineRoutes(router *routing.Router, healthy *atomic.Bool, w *services.Writer) {
	router.Get("/health", func(c *routing.Context) error {
		if !healthy.Load() {
			c.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		}
		return nil
	})

	router.Get("/e", func(c *routing.Context) error {
		e, err := entities.EventFromRoutingCtx(c)
		if err != nil {
			c.Response.SetStatusCode(fasthttp.StatusBadRequest)
		}

		if w.Write(e) != nil {
			c.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		}
		return nil
	})
}
