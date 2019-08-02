package driver

import (
	"context"

	"github.com/ONSdigital/graphson"
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
	Get(query string, bindings, rebindings map[string]string) ([]graphson.Vertex, error)
	GetCount(q string, bindings, rebindings map[string]string) (i int64, err error)
	GetE(q string, bindings, rebindings map[string]string) (resp interface{}, err error)
	OpenStreamCursor(ctx context.Context, query string, bindings, rebindings map[string]string) (stream *gremgo.Stream, err error)
	GetStringList(query string, bindings, rebindings map[string]string) (vals []string, err error)
}
