package drivers

import (
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

type Driver io.WriteCloser

var (
	driver = os.Getenv(`DRIVER`)

	driverRegistry = map[string]func() Driver{
		``: func() Driver {
			logrus.Info("no driver specified, falling back to stdout")
			return &StdOut{}
		},
		`stdout`: func() Driver {
			return &StdOut{}
		},
		`stub`:    NewStub,
		`discard`: NewDiscard,
		`s3`: func() Driver {
			return NewS3(&S3Config{
				region:            os.Getenv(`S3_REGION`),
				dataPrefix:        os.Getenv(`S3_PREFIX`),
				bucket:            os.Getenv(`S3_BUCKET`),
				sqsQueue:          os.Getenv(`SQS_QUEUE`),
				enableCompression: os.Getenv(`ENABLE_COMPRESSION`) == `true`,
			})
		},
	}
)

func FetchDriver() func() Driver {
	driver, ok := driverRegistry[driver]
	if !ok {
		panic(`driver not found`)
	}
	return driver
}
