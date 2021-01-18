package drivers

import (
	"fmt"
	"os"
)

type StdOut struct {
}

func (so *StdOut) Write(e []byte) (int, error) {
	return fmt.Fprint(os.Stdout, string(e))
}
func (so *StdOut) Flush() error {
	return nil
}

func (so *StdOut) Close() error {
	fmt.Println(`closed`)
	return nil
}
