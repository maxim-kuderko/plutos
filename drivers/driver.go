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
		`s3`:  NewS3,
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
