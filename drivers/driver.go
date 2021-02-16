package drivers

import (
	"io"
	"os"
)

type Driver io.WriteCloser

var (
	driver = os.Getenv(`DRIVER`)

	driverRegistry = map[string]func() Driver{
		`stdout`: func() Driver {
			return &StdOut{}
		},
		`stub`:    NewStub,
		`discard`: NewDiscard,
		`s3`: func() Driver {
			return NewS3(os.Getenv(`ENABLE_COMPRESSION`) == `true`)
		},
		`sqs`: NewSqs,
	}
)

func FetchDriver() func() Driver {
	driver, ok := driverRegistry[driver]
	if !ok {
		panic(`driver not found`)
	}
	return driver
}
