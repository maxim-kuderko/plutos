package services

import (
	"github.com/maxim-kuderko/plutos/entities"
	"github.com/maxim-kuderko/plutos/services/drivers"
	"testing"
)

func BenchmarkWriter_Write(b *testing.B) {
	b.ReportAllocs()
	tester := NewWriter(&drivers.StdOut{})
	e := entities.Event{
		RawData: map[string]string{`test`: `test`},
		Enrichment: entities.Enrichment{
			Headers: map[string]string{`testH`: `testH`},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tester.Write(e)
	}
}
