package plutos

import (
	"encoding/json"
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
	RequestID string
}
