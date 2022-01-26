package models

import (
	"context"
)

// CantabularDataProvider represents all the required methods to access data from Cantabular
type CantabularDataProvider interface {
	Blobs(ctx context.Context) ([]Blob, error)
}
