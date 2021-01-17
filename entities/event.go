package entities

import (
	routing "github.com/qiangxue/fasthttp-routing"
	"strings"
)

type Event struct {
	RawData    map[string]string `json:"raw_data"`
	Enrichment Enrichment        `json:"enrichment"`
}

type Enrichment struct {
	Headers map[string]string
}

func EventFromRoutingCtx(ctx *routing.Context) (Event, error) {
	return Event{
		RawData:    queryParamsToMap(ctx.Request.URI().QueryString(), '=', '&'),
		Enrichment: Enrichment{Headers: headersToMap(ctx.Request.Header.Header(), ':', '\n')},
	}, nil
}

func queryParamsToMap(b []byte, kvSep, paramSep byte) map[string]string {
	var k, v strings.Builder
	output := map[string]string{}

	currentWriter := &k

	for _, c := range b {
		if c == kvSep {
			currentWriter = &v
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
		currentWriter.WriteByte(c)
	}
	if k.Len() > 0 {
		output[k.String()] = v.String()
	}
	return output
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
