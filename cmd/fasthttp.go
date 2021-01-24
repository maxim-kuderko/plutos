package main

import (
	"bytes"
	"encoding/json"
	jsoniter "github.com/json-iterator/go"
	"github.com/maxim-kuderko/plutos"
	"github.com/maxim-kuderko/plutos/drivers"
	"github.com/qiangxue/fasthttp-routing"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fasthttp"
	"go.uber.org/atomic"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.ErrorLevel)
	healthy := atomic.NewBool(true)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	router := routing.New()
	writer := plutos.NewWriter(drivers.FetchDriver())
	defineRoutes(router, healthy, writer)

	go func() {
		srv := fasthttp.Server{
			Handler:               router.HandleRequest,
			TCPKeepalive:          true,
			NoDefaultServerHeader: true,
			NoDefaultDate:         true,
			NoDefaultContentType:  true,
		}
		log.Err(srv.ListenAndServe(os.Getenv(`PORT`)))
	}()
	<-c
	writer.Close()

}

func defineRoutes(router *routing.Router, healthy *atomic.Bool, w *plutos.Writer) {
	router.Get("/health", func(c *routing.Context) error {
		if !healthy.Load() {
			c.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		}
		return nil
	})

	router.Get("/e", func(c *routing.Context) error {
		e, err := EventFromRoutingCtxGET(c)
		if err != nil {
			c.Response.SetStatusCode(fasthttp.StatusBadRequest)
		}
		if jsoniter.ConfigFastest.NewEncoder(w).Encode(e) != nil {
			c.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		}
		return nil
	})

	router.Post("/e", func(c *routing.Context) error {
		e, err := EventFromRoutingCtxPOST(c)
		if err != nil {
			c.Response.SetStatusCode(fasthttp.StatusBadRequest)
		}
		if jsoniter.ConfigFastest.NewEncoder(w).Encode(e) != nil {
			c.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		}
		return nil
	})
}

func EventFromRoutingCtxGET(ctx *routing.Context) (plutos.Event, error) {
	return plutos.Event{
		RawData:    queryParamsToMapJson(ctx.Request.URI().QueryString(), '=', '&'),
		Enrichment: getEnrichment(ctx),
		Metadata:   generateMetadata(),
	}, nil
}

func getEnrichment(ctx *routing.Context) plutos.Enrichment {
	return plutos.Enrichment{Headers: headersToMap(ctx.Request.Header.Header(), ':', '\n')}
}

func generateMetadata() plutos.Metadata {
	return plutos.Metadata{WrittenAt: time.Now().Format(time.RFC3339) /* RequestID: uuid.New().String()*/}
}

func EventFromRoutingCtxPOST(ctx *routing.Context) (plutos.Event, error) {
	return plutos.Event{
		RawData:    ctx.Request.Body(),
		Enrichment: getEnrichment(ctx),
		Metadata:   generateMetadata(),
	}, nil
}

var empty = json.RawMessage("{}")

func queryParamsToMapJson(b []byte, kvSep, paramSep byte) json.RawMessage {
	output := bytes.NewBuffer(nil)
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
	if output.Len() == 0 {
		return empty
	}
	output.WriteString(`"}`)
	return output.Bytes()
}

func headersToMap(b []byte, kvSep, paramSep byte) map[string]string {
	var k, v strings.Builder
	output := map[string]string{}

	currentWriter := &k

	for _, c := range b {
		if c == kvSep {
			currentWriter = &v
			continue
		}
		if c == '\r' {
			continue
		}
		if c == paramSep {
			if k.Len() > 0 {
				output[k.String()] = v.String()
				k.Reset()
				v.Reset()
			}
			currentWriter = &k
			continue
		}
		if currentWriter.Len() == 0 && currentWriter == &v && c == ' ' {
			continue
		}
		currentWriter.WriteByte(c)
	}
	if k.Len() > 0 {
		output[k.String()] = v.String()
	}
	return output
}