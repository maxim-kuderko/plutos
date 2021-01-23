package entities

import (
	"bytes"
	"encoding/json"
	routing "github.com/qiangxue/fasthttp-routing"
	"strings"
	"time"
)

type Event struct {
	RawData    json.RawMessage `json:"raw_data"`
	Enrichment Enrichment      `json:"enrichment"`
	Metadata   Metadata        `json:"metadata"`
}

type Enrichment struct {
	Headers map[string]string
}

type Metadata struct {
	WrittenAt string
}

func EventFromRoutingCtxGET(ctx *routing.Context) (Event, error) {
	//data, err := jsoniter.ConfigFastest.Marshal()
	return Event{
		RawData:    queryParamsToMapJson(ctx.Request.URI().QueryString(), '=', '&'),
		Enrichment: Enrichment{Headers: headersToMap(ctx.Request.Header.Header(), ':', '\n')},
		Metadata:   Metadata{WrittenAt: time.Now().Format(time.RFC3339)},
	}, nil
}

func EventFromRoutingCtxPOST(ctx *routing.Context) (Event, error) {
	return Event{
		RawData:    ctx.Request.Body(),
		Enrichment: Enrichment{Headers: headersToMap(ctx.Request.Header.Header(), ':', '\n')},
		Metadata:   Metadata{WrittenAt: time.Now().Format(time.RFC3339)},
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
