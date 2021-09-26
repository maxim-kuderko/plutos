package main

import (
	"encoding/json"
)

type Event struct {
	RawData    json.RawMessage `json:"raw_data"`
	Enrichment Enrichment      `json:"enrichment"`
	Metadata   Metadata        `json:"metadata"`
}

type Enrichment struct {
	Headers map[string]string `json:"headers"`
}

type Metadata struct {
	WrittenAt string `json:"written_at"`
	RequestID string `json:"request_id"`
}
