package drivers

import "io"

type Driver interface {
	io.WriteCloser
	Flush() error
}
