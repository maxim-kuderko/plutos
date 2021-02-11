package drivers

type Discard struct {
}

func NewDiscard() Driver {
	return &Discard{}
}

func (so *Discard) Write(e []byte) (int, error) {
	return len(e), nil
}

func (so *Discard) Close() error {
	return nil
}
