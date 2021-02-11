package main

import (
	"encoding/json"
	"github.com/kpango/fastime"
	"github.com/maxim-kuderko/plutos"
	"github.com/maxim-kuderko/plutos/drivers"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/savsgio/gotils"
	"github.com/valyala/bytebufferpool"
	"github.com/valyala/fasthttp"
	"go.uber.org/atomic"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.ErrorLevel)
	healthy := atomic.NewBool(true)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	writer := plutos.NewWriter(drivers.FetchDriver())
	handler := defineRoutes(healthy, writer)

	go func() {
		srv := fasthttp.Server{
			Handler:               handler,
			TCPKeepalive:          true,
			NoDefaultServerHeader: true,
			NoDefaultDate:         true,
			NoDefaultContentType:  true,
			ReadBufferSize:        64 * 1024,
			WriteBufferSize:       64 * 1024,
		}
		log.Err(srv.ListenAndServe(os.Getenv(`PORT`)))
	}()
	<-c
	writer.Close()

}

func health(health *atomic.Bool) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		if !health.Load() {
			ctx.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		}
	}
}

func get(w *plutos.Writer) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		e, err := EventFromRoutingCtxGET(ctx)
		defer bytebufferpool.Put(e)
		if err != nil {
			ctx.Response.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}
		if _, err = e.WriteTo(w); err != nil {
			ctx.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		}
		return
	}
}

func defineRoutes(healthy *atomic.Bool, w *plutos.Writer) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		switch gotils.B2S(ctx.Path()) {
		case "/health":
			health(healthy)(ctx)
		case "/e":
			get(w)(ctx)
		default:
			ctx.Error(fasthttp.StatusMessage(fasthttp.StatusNotFound), fasthttp.StatusNotFound)
		}
	}
}

var ft = fastime.New()

func EventFromRoutingCtxGET(ctx *fasthttp.RequestCtx) (*bytebufferpool.ByteBuffer, error) {
	output := bytebufferpool.Get()
	output.WriteString(`{`)
	output.WriteString(`"raw_data": `)
	queryParamsToMapJson(output, ctx.Request.URI().QueryArgs().Peek(`e`), '=', '&')
	output.WriteString(`written_at:"`)
	//output.WriteString(ft.Now().Format(time.RFC3339Nano))
	output.WriteString(`"`)
	output.WriteString(`}`)

	return output, nil
}

var empty = json.RawMessage("{}")

func queryParamsToMapJson(output *bytebufferpool.ByteBuffer, b []byte, kvSep, paramSep byte) {
	output.WriteString(`{`)
	isSTart := true
	for _, c := range b {
		if isSTart {
			output.WriteByte('"')
			isSTart = false
		}
		if c == kvSep {
			output.WriteString(`":`)
			isSTart = true
			continue
		}
		if c == paramSep {
			output.WriteString(`",`)
			isSTart = true
			continue
		}
		output.WriteByte(c)
	}

	output.WriteString(`"}`)
}
