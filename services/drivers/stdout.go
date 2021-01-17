package drivers

import (
	"fmt"
)

type StdOut struct {
}

func (so *StdOut) Write(e []byte) (int, error) {
	fmt.Print(string(e))
	return len(e), nil
}

func (so *StdOut) Close() error {
	fmt.Println(`closed`)
	return nil
}
