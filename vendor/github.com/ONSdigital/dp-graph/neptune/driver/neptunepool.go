package driver

import (
	"context"

	gremgo "github.com/ONSdigital/gremgo-neptune"
)

//go:generate moq -out ../internal/pool.go -pkg internal . NeptunePool

/*
NeptunePool defines the contract required of the gremgo
connection Pool by the Neptune.Driver.
*/
type NeptunePool interface {
	Close()
	Execute(query string, bindings, rebindings map[string]string) (resp []gremgo.Response, err error)
	Get(query string, bindings, rebindings map[string]string) (resp interface{}, err error)
	GetCount(q string, bindings, rebindings map[string]string) (i int64, err error)
	GetE(q string, bindings, rebindings map[string]string) (resp interface{}, err error)
	OpenCursorCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (cursor *gremgo.Cursor, err error)
	GetStringList(query string, bindings, rebindings map[string]string) (vals []string, err error)
}
