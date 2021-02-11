package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"github.com/kpango/fastime"
	"github.com/maxim-kuderko/plutos"
	"github.com/maxim-kuderko/plutos/drivers"
	"github.com/qiangxue/fasthttp-routing"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/valyala/bytebufferpool"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastrand"
	"go.uber.org/atomic"
	"hash"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.ErrorLevel)
	healthy := atomic.NewBool(true)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	router := routing.New()
	writer := plutos.NewWriter(drivers.FetchDriver())
	defineRoutes(router, healthy, writer)

	go func() {
		srv := fasthttp.Server{
			Handler:                       router.HandleRequest,
			TCPKeepalive:                  true,
			NoDefaultServerHeader:         true,
			NoDefaultDate:                 true,
			NoDefaultContentType:          true,
			DisableHeaderNamesNormalizing: true,
			ReadBufferSize:                64 * 1024,
			WriteBufferSize:               64 * 1024,
		}
		log.Err(srv.ListenAndServe(os.Getenv(`PORT`)))
	}()
	<-c
	writer.Close()

}

func defineRoutes(router *routing.Router, healthy *atomic.Bool, w *plutos.Writer) {
	router.Get("/health", func(c *routing.Context) error {
		/*if !healthy.Load() {
			c.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		}*/
		return nil
	})

	router.Get("/e", func(c *routing.Context) error {
		e, err := EventFromRoutingCtxGET(c)
		defer bytebufferpool.Put(e)
		if err != nil {
			c.Response.SetStatusCode(fasthttp.StatusBadRequest)
			return err
		}
		if _, err = e.WriteTo(w); err != nil {
			c.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		}
		return nil
	})
}

var ft = fastime.New()

var hasherPool = sync.Pool{New: func() interface{} {
	return md5.New()
}}

func EventFromRoutingCtxGET(ctx *routing.Context) (*bytebufferpool.ByteBuffer, error) {
	output := bytebufferpool.Get()
	hasher := hasherPool.Get().(hash.Hash)
	binary.Write(hasher, binary.LittleEndian, fastrand.Uint32())
	defer func() {
		hasher.Reset()
		hasherPool.Put(hasher)
	}()
	output.WriteString(`{`)
	output.WriteString(`"raw_data": `)
	queryParamsToMapJson(output, ctx.Request.URI().QueryArgs().Peek(`e`), '=', '&')
	output.WriteString(`written_at:"`)
	output.WriteString(ft.Now().Format(time.RFC3339Nano))
	output.WriteString(`",`)
	output.WriteString(`request_id:"`)

	output.WriteTo(hasher)

	output.WriteString(hex.EncodeToString(hasher.Sum(nil)))

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
