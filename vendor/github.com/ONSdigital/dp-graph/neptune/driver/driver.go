package driver

import (
	"context"

	gremgo "github.com/ONSdigital/gremgo-neptune"
)

type NeptuneDriver struct {
	Pool NeptunePool // Defined with an interface to support mocking.
}

func New(ctx context.Context, dbAddr string, errs chan error) (*NeptuneDriver, error) {
	pool := gremgo.NewPoolWithDialerCtx(ctx, dbAddr, errs)
	return &NeptuneDriver{
		Pool: pool,
	}, nil
}

func (n *NeptuneDriver) Close(ctx context.Context) error {
	n.Pool.Close()
	return nil
}
